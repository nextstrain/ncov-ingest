"""
This part of the workflow handles various Slack notifications.
Designed to be used internally by the Nextstrain team with hard-coded paths
to files on AWS S3.

All rules here require two environment variables:
    * SLACK_TOKEN
    * SLACK_CHANNELS

Expects different inputs for GISAID vs GenBank:
    GISAID:
        ndjson = "data/gisaid.ndjson"
        flagged_annotations = "data/gisaid/flagged-annotations"
        additional_info = "data/gisaid/additional_info.tsv"
    GenBank:
        ndjson = "data/gisaid.ndjson"
        flagged_annotations = "data/genbank/flagged-annotations"
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"

Produces the output file as:
    "data/{database}/notify-on-record-change.done"
    "data/{database}/notify.done"
The output files is an empty flag file to force Snakemake to run the notify rules.
"""
rule notify_on_record_change:
    input:
        ndjson = f"data/{database}.ndjson"
    params:
        ndjson_on_s3 = f"{config['s3_src']}/{database}.ndjson.zst"
    output:
        touch(f"data/{database}/notify-on-record-change.done")
    benchmark:
        f"benchmarks/notify_on_record_change_{database}.txt"
    shell:
        """
        ./shared/vendored/scripts/notify-on-record-change {input.ndjson} {params.ndjson_on_s3} {database}
        """


rule notify_gisaid:
    input:
        notify_on_record_change = "data/gisaid/notify-on-record-change.done",
        flagged_annotations = rules.transform_gisaid_data.output.flagged_annotations,
        additional_info = "data/gisaid/additional_info.tsv",
    params:
        s3_bucket = config["s3_src"]
    output:
        touch("data/gisaid/notify.done")
    benchmark:
        "benchmarks/notify_gisaid.txt"
    run:
        shell("./shared/vendored/scripts/notify-slack --upload flagged-annotations < {input.flagged_annotations}")
        shell("./bin/notify-on-additional-info-change {input.additional_info} {params.s3_bucket}/additional_info.tsv.zst")

rule notify_genbank:
    input:
        notify_on_record_change = "data/genbank/notify-on-record-change.done",
        flagged_annotations = rules.transform_genbank_data.output.flagged_annotations,
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"
    params:
        s3_bucket = config["s3_src"]
    output:
        touch("data/genbank/notify.done")
    benchmark:
        "benchmarks/notify_genbank.txt"
    run:
        shell("./shared/vendored/scripts/notify-slack --upload flagged-annotations < {input.flagged_annotations}")
        # transform-genbank writes data/genbank/problem_data.tsv via its --problem-data default;
        # notify-on-problem-data no-ops when the file is empty or absent.
        shell("./bin/notify-on-problem-data data/genbank/problem_data.tsv")
        shell("./bin/notify-on-duplicate-biosample-change {input.duplicate_biosample} {params.s3_bucket}/duplicate_biosample.txt.zst")
