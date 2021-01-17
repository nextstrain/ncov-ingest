import os

S3_BUCKET = "s3://nextstrain-ncov-private"
ruleorder: unpack_gisaid>fetch_gisaid_from_S3
ruleorder: concat_clades>nextclade_all
localrules: download_inputs, download_gisaid, push_gisaid_to_S3, post_to_slack, fetch_gisaid_from_S3, rerun_clades, upload_gisaid_to_S3


rule download_inputs:
    output:
        additional_info = "data/{data_source}/inputs/additional_info.tsv",
        metadata = "data/{data_source}/inputs/metadata.tsv",
        clades = "data/{data_source}/inputs/nextclade.tsv"
    shell:
        '''
        aws s3 cp {S3_BUCKET}/additional_info.tsv.gz - | gunzip -cfq > {output.additional_info} &\
        aws s3 cp {S3_BUCKET}/metadata.tsv.gz - | gunzip -cfq > {output.metadata} &\
        aws s3 cp {S3_BUCKET}/nextclade.tsv.gz - | gunzip -cfq > {output.clades} &\
        '''

rule unpack_gisaid:
    input:
        json = "data/gisaid/inputs/tmp/gisaid.ndjson.bz2"
    output:
        json = "data/gisaid/inputs/gisaid.ndjson"
    shell:
        '''
        bzip2 -cdk {input.json} > {output.json}
        '''

rule fetch_gisaid_from_S3:
    output:
        json = "data/gisaid/inputs/gisaid.ndjson"
    params:
        src = f"{S3_BUCKET}/gisaid.ndjson.gz"
    shell:
        '''
        aws s3 cp --no-progress {params.src} - | gunzip -cfq > {output.json}
        '''

rule push_gisaid_to_S3:
    input:
        json = "data/gisaid/inputs/gisaid.ndjson"
    shell:
        '''
        ./bin/upload-to-s3 --quiet {input.json} "{S3_BUCKET}/gisaid.ndjson.gz"
        '''

rule download_gisaid:
    output:
        json = "data/gisaid/inputs/tmp/gisaid.ndjson.bz2"
    params:
        GISAID_USERNAME = os.environ.get("GISAID_USERNAME", None),
        GISAID_PASSWORD = os.environ.get("GISAID_PASSWORD", None),
        GISAID_URL = os.environ.get("GISAID_URL", None),
    log:
        "logs/download_gisaid.txt"
    shell:
        '''
        wget \
            --http-user={params.GISAID_USERNAME} \
            --http-password={params.GISAID_PASSWORD} -o {log} \
            -O {output.json}  {params.GISAID_URL}
        '''

rule transform:
    input:
        gisaid_json = rules.unpack_gisaid.output.json,
    output:
        metadata = "data/gisaid/metadata.tsv",
        sequences = "data/gisaid/sequences.fasta",
        additional_info = "data/gisaid/additional_info.tsv"
    shell:
        '''
        ./bin/transform-gisaid {input.gisaid_json} \
        --output-metadata {output.metadata} \
        --output-fasta {output.sequences} \
        --output-additional-info {output.additional_info}
        '''

rule sequences_without_clades:
    input:
        sequences = rules.transform.output.sequences,
        clades = rules.download_inputs.output.clades
    output:
        new_sequences = "data/{data_source}/new_sequences.fasta"
    shell:
        '''
        ./bin/filter-fasta \
        --input_fasta={input.sequences} \
        --input_tsv={input.clades} \
        --output_fasta={output.new_sequences}
        '''

rule run_nextclade:
    input:
        rules.sequences_without_clades.output
    output:
        "data/{data_source}/new_clades.tsv"
    params:
        batch_size = 1000
    threads: 8
    shell:
        '''
        ./bin/run-nextclade {params.batch_size} {threads} {input} {output}
        '''

rule concat_clades:
    input:
        new_clades = rules.run_nextclade.output,
        old_clades = rules.download_inputs.output.clades
    output:
        clades = "data/{data_source}/nextclade.tsv",
    shell:
        '''
        ./bin/join-rows {input.new_clades} {input.old_clades} -o {output.clades}
        '''


rule nextclade_all:
    input:
        rules.transform.output.sequences
    output:
        "data/gisaid/nextclade.tsv"
    params:
        batch_size = 1000
    threads: 16
    shell:
        '''
        ./bin/run-nextclade {params.batch_size} {threads} {input} {output}
        '''

rule join_metadata_and_clades:
    input:
        clades = rules.concat_clades.output.clades,
        metadata = rules.transform.output.metadata,
    output:
        metadata = "data/{data_source}/new_metadata.tsv"
    shell:
        '''
        ./bin/join-metadata-and-clades \
            {input.metadata} \
            {input.clades} \
            -o {output.metadata}
        '''

rule flag_metadata:
    input:
        rules.join_metadata_and_clades.output.metadata
    output:
        "data/{data_source}/flagged_metadata.txt"
    shell:
        '''
        ./bin/flag-metadata {input} > {output}
        '''

rule check_locations:
    input:
        rules.join_metadata_and_clades.output.metadata
    output:
        "data/{data_source}/location_hierarchy.tsv"
    shell:
        '''
        ./bin/check-locations {input} {output} gisaid_epi_isl
        '''

rule clean:
    message: "Removing directories: {params}"
    params:
        "data",
        "tmp"
    shell:
        "rm -rfv {params}"

rule metadata_addition:
    input:
        old = "data/{data_source}/inputs/metadata.tsv",
        new = "data/{data_source}/new_metadata.tsv"
    output:
        additions = "data/{data_source}/metadata_additions.tsv",
        changes = "data/{data_source}/metadata_changes.tsv"
    params:
        key = "gisaid_epi_isl"
    shell:
        '''
        csv-diff \
            {input.old} \
            {input.new} \
            --format tsv \
            --key {params.key} \
            --singular sequence \
            --plural sequences \
            > {output.changes} & \
        ./bin/metadata-additions {input.old} {input.new} {params.key} >  {output.additions}
        '''

rule additional_info_change:
    input:
        old = "data/{data_source}/inputs/additional_info.tsv",
        new = "data/{data_source}/additional_info.tsv"
    output:
        changes = "data/{data_source}/additional_info_changes.tsv"
    params:
        key = "gisaid_epi_isl"
    shell:
        '''
        csv-diff \
            <(awk 'BEGIN {{FS="\t"}}; {{ if ($3 != "" || $4 != "") {{ print }}}}' {input.old}) \
            <(awk 'BEGIN {{FS="\t"}}; {{ if ($3 != "" || $4 != "") {{ print }}}}' {input.new}) \
            --format tsv \
            --key gisaid_epi_isl \
            --singular "additional info" \
            --plural "additional info" > {output.changes}
        '''


##########################################################################
onerror:
    shell("./bin/notify-slack 'ncov-ingest failed'")


##########################################################################
## target rules
##########################################################################
rule post_to_slack:
    input:
        additional_info = "data/gisaid/additional_info_changes.tsv",
        metadata_additions = "data/gisaid/metadata_additions.tsv",
        metadata_changes = "data/gisaid/metadata_changes.tsv"
    shell:
        '''
        ./bin/notify-slack --upload "metadata-changes.txt" < {input.metadata_changes} &\
        ./bin/notify-slack --upload "metadata-additions.txt" < {input.metadata_additions} &\
        ./bin/notify-slack --upload "additional_info_changes.txt" < {input.additional_info}
        '''

rule upload_gisaid_to_S3:
    input:
        metadata = "data/gisaid/metadata.tsv",
        clades = "data/gisaid/nextclade.tsv",
        additional_info = "data/gisaid/additional_info.tsv",
        sequences = "data/gisaid/sequences.fasta",
        flagged_metadata = "data/gisaid/flagged_metadata.txt"
    shell:
        '''
        ./bin/upload-to-s3 --quiet {input.metadata} {S3_BUCKET}/metadata.tsv.gz &\
        ./bin/upload-to-s3 --quiet {input.clades} {S3_BUCKET}/nextclade.tsv.gz &\
        ./bin/upload-to-s3 --quiet {input.additional_info} {S3_BUCKET}/additional_info.tsv.gz &\
        ./bin/upload-to-s3 --quiet {input.flagged_metadata} {S3_BUCKET}/flagged_metadata.txt.gz &\
        ./bin/upload-to-s3 --quiet {input.sequences} {S3_BUCKET}/sequences.fasta.gz
        '''

rule rerun_clades:
    input:
        clades = rules.nextclade_all.output
    shell:
        '''
        ./bin/upload-to-s3 --quiet {input.clades} {S3_BUCKET}/nextclade.tsv.gz
        '''
