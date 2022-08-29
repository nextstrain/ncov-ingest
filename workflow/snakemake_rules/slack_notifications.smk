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
        flagged_metadata = "data/gisaid/flagged_metadata.txt"
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
        ndjson_on_s3 = f"{config['s3_src']}/{database}.ndjson.xz"
    output:
        touch(f"data/{database}/notify-on-record-change.done")
    shell:
        """
        ./bin/notify-on-record-change {input.ndjson} {params.ndjson_on_s3} {database}
        """


rule notify_gisaid:
    input:
        flagged_annotations = rules.transform_gisaid_data.output.flagged_annotations,
        # metadata = "data/gisaid/metadata.tsv",
        additional_info = "data/gisaid/additional_info.tsv",
        flagged_metadata = "data/gisaid/flagged_metadata.txt"
    params:
        s3_bucket = config["s3_src"]
    output:
        touch("data/gisaid/notify.done")
    run:
        shell("./bin/notify-slack --upload flagged-annotations < {input.flagged_annotations}")
        # notify-on-metadata-change disabled as csv-diff runs out of memory
        # shell("./bin/notify-on-metadata-change {input.metadata} {params.s3_bucket}/metadata.tsv.gz gisaid_epi_isl")
        shell("./bin/notify-on-additional-info-change {input.additional_info} {params.s3_bucket}/additional_info.tsv.gz")
        shell("./bin/notify-on-flagged-metadata-change {input.flagged_metadata}  {params.s3_bucket}/flagged_metadata.txt.gz")

rule notify_genbank:
    input:
        flagged_annotations = rules.transform_genbank_data.output.flagged_annotations,
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"
    params:
        s3_bucket = config["s3_src"]
    output:
        touch("data/genbank/notify.done")
    run:
        shell("./bin/notify-slack --upload flagged-annotations < {input.flagged_annotations}")
        # TODO - which rule produces data/genbank/problem_data.tsv? (was not explicit in `ingest-genbank` bash script)
        shell("./bin/notify-on-problem-data data/genbank/problem_data.tsv")
        shell("./bin/notify-on-duplicate-biosample-change {input.duplicate_biosample} {params.s3_bucket}/duplicate_biosample.txt.gz")

