import os

S3_BUCKET = "s3://nextstrain-ncov-private"
ruleorder: unpack_gisaid>fetch_from_S3

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

rule fetch_from_S3:
    output:
        json = "data/gisaid/inputs/gisaid.ndjson"
    params:
        src = f"{S3_BUCKET}/gisaid.ndjson.gz"
    shell:
        '''
         aws s3 cp --no-progress {params.src} - | gunzip -cfq > {output.json}
        '''

rule push_to_S3:
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
            -O {output.json}  {params.GISAID_URL}"
        '''

rule transform:
    input:
        gisaid_json = rules.unpack_gisaid.output.json,
    output:
        metadata = "data/output/gisaid/metadata.tsv",
        sequences = "data/output/gisaid/sequences.fasta",
        additional_info = "data/output/gisaid/additional_info.tsv"
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

rule nextclade:
    input:
        rules.sequences_without_clades.output
    output:
        "data/{data_source}/new_clades.tsv"
    params:
        batch_size = 1000
    threads: 4
    shell:
        '''
        touch {output} &\
        ./bin/run-nextclade {params.batch_size} {threads} {input} {output}
        '''

