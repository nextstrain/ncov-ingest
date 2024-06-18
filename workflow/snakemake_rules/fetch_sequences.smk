"""
This part of the workflow handles fetching sequences and metadata from GISAID
or NCBI GenBank/Biosample. Depends on the main Snakefile to define the variable
`database`, which is NOT a wildcard.

If the config contains `s3_dst`,`s3_src`, and `fetch_from_database=False`,
then files will be fetched from the AWS S3 bucket. Or else, the data is fetched
directly from the databases.

Produces different final outputs for GISAID vs GenBank/RKI:
    GISAID:
        ndjson = "data/gisaid.ndjson"
    GenBank:
        ndjson = "data/genbank.ndjson"
        biosample = "data/biosample.ndjson"
        cog_uk_accessions = "data/cog_uk_accessions.tsv"
        cog_uk_metadata = "data/cog_uk_metadata.csv.gz"
        rki_ndjson = "data/rki.ndjson"
"""

rule fetch_main_gisaid_ndjson:
    output:
        ndjson = temp(f"data/gisaid.ndjson")
    benchmark:
        "benchmarks/fetch_main_gisaid_ndjson.txt"
    retries: 5
    shell:
        """
        ./bin/fetch-from-gisaid {output.ndjson}
        """


rule fetch_ncbi_dataset_package:
    output:
        dataset_package = temp("data/ncbi_dataset.zip")
    retries: 5
    benchmark:
        "benchmarks/fetch_ncbi_dataset_package.txt"
    shell:
        """
        datasets download virus genome taxon SARS-CoV-2 \
            --no-progressbar \
            --filename {output.dataset_package}
        """

rule extract_ncbi_dataset_sequences:
    input:
        dataset_package = "data/ncbi_dataset.zip"
    output:
        ncbi_dataset_sequences = temp("data/ncbi_dataset_sequences.fasta")
    benchmark:
        "benchmarks/extract_ncbi_dataset_sequences.txt"
    shell:
        """
        unzip -jp {input.dataset_package} \
            ncbi_dataset/data/genomic.fna > {output.ncbi_dataset_sequences}
        """

def _get_ncbi_dataset_field_mnemonics(wildcard):
    """
    Return list of NCBI Dataset report field mnemonics for fields that we want
    to parse out of the dataset report. The column names in the output TSV
    are different from the mnemonics.

    See NCBI Dataset docs for full list of available fields and their column
    names in the output:
    https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/command-line/dataformat/tsv/dataformat_tsv_virus-genome/#fields
    """
    fields = [
        "accession",
        "sourcedb",
        "sra-accs",
        "isolate-lineage",
        "geo-region",
        "geo-location",
        "isolate-collection-date",
        "release-date",
        "update-date",
        "virus-pangolin",
        "length",
        "host-name",
        "isolate-lineage-source",
        "biosample-acc",
        "submitter-names",
        "submitter-affiliation",
        "submitter-country",
    ]
    return ",".join(fields)

rule format_ncbi_dataset_report:
    input:
        dataset_package = "data/ncbi_dataset.zip"
    output:
        ncbi_dataset_tsv = temp("data/ncbi_dataset_report.tsv")
    params:
        fields_to_include = _get_ncbi_dataset_field_mnemonics
    benchmark:
        "benchmarks/format_ncbi_dataset_report.txt"
    shell:
        """
        dataformat tsv virus-genome \
            --package {input.dataset_package} \
            --fields {params.fields_to_include} \
            > {output.ncbi_dataset_tsv}
        """

rule create_genbank_ndjson:
    input:
        ncbi_dataset_sequences = "data/ncbi_dataset_sequences.fasta",
        ncbi_dataset_tsv = "data/ncbi_dataset_report.tsv",
    output:
        ndjson = temp("data/genbank.ndjson"),
    log: "logs/create_genbank_ndjson.txt"
    benchmark:
        "benchmarks/create_genbank_ndjson.txt"
    shell:
        """
        augur curate passthru \
            --metadata {input.ncbi_dataset_tsv} \
            --fasta {input.ncbi_dataset_sequences} \
            --seq-id-column Accession \
            --seq-field sequence \
            --unmatched-reporting warn \
            --duplicate-reporting warn \
            2> {log} > {output.ndjson}
        """

rule fetch_biosample:
    """Fetching BioSample data (GenBank only)"""
    output:
        biosample = temp("data/biosample.ndjson")
    benchmark:
        "benchmarks/fetch_biosample.txt"
    retries: 5
    shell:
        """
        ./bin/fetch-from-biosample > {output.biosample}
        """


rule fetch_cog_uk_accessions:
    """Fetching COG-UK sample accesions (GenBank only)"""
    output:
        cog_uk_accessions = temp("data/cog_uk_accessions.tsv")
    benchmark:
        "benchmarks/fetch_cog_uk_accessions.txt"
    retries: 5
    shell:
        """
        ./bin/fetch-from-cog-uk-accessions > {output.cog_uk_accessions}
        """


rule fetch_cog_uk_metadata:
    """Fetching COG-UK metadata (GenBank only)"""
    output:
        cog_uk_metadata = temp("data/cog_uk_metadata.csv.gz")
    benchmark:
        "benchmarks/fetch_cog_uk_metadata.txt"
    retries: 5
    shell:
        """
        ./bin/fetch-from-cog-uk-metadata > {output.cog_uk_metadata}
        """


rule uncompress_cog_uk_metadata:
    input:
        "data/cog_uk_metadata.csv.gz"
    output:
        cog_uk_metadata = temp("data/cog_uk_metadata.csv")
    benchmark:
        "benchmarks/uncompress_cog_uk_metadata.txt"
    shell:
        "gunzip -c {input} > {output}"


rule fetch_rki_sequences:
    output:
        rki_sequences=temp("data/rki_sequences.fasta.xz"),
    benchmark:
        "benchmarks/fetch_rki_sequences.txt"
    retries: 5
    shell:
        """
        ./bin/fetch-from-rki-sequences > {output.rki_sequences}
        """


rule fetch_rki_metadata:
    output:
        rki_metadata=temp("data/rki_metadata.tsv.xz"),
    benchmark:
        "benchmarks/fetch_rki_metadata.txt"
    retries: 5
    shell:
        """
        ./bin/fetch-from-rki-metadata > {output.rki_metadata}
        """


rule transform_rki_data_to_ndjson:
    input:
        rki_sequences="data/rki_sequences.fasta.xz",
        rki_metadata="data/rki_metadata.tsv.xz"
    output:
        ndjson="data/rki.ndjson",
    benchmark:
        "benchmarks/transform_rki_data_to_ndjson.txt"
    shell:
        """
        ./bin/transform-rki-data-to-ndjson \
            --input-rki-sequences {input.rki_sequences} \
            --input-rki-metadata {input.rki_metadata} \
            --output-ndjson {output.ndjson}
        """


# Only include rules to fetch from S3 if S3 config params are provided
if config.get("s3_dst") and config.get("s3_src"):

    # Set ruleorder since these fetch rules have the same output
    # Fetch directly from databases when `fetch_from_database=True`
    # or else fetch files from AWS S3 buckets
    if config.get("fetch_from_database", False):
        ruleorder: fetch_main_gisaid_ndjson > fetch_main_ndjson_from_s3
        ruleorder: fetch_biosample > fetch_biosample_from_s3
        ruleorder: transform_rki_data_to_ndjson > fetch_rki_ndjson_from_s3
        ruleorder: fetch_cog_uk_accessions > fetch_cog_uk_accessions_from_s3
        ruleorder: fetch_cog_uk_metadata > compress_cog_uk_metadata
        ruleorder: uncompress_cog_uk_metadata > fetch_cog_uk_metadata_from_s3
        ruleorder: create_genbank_ndjson > fetch_main_ndjson_from_s3
    else:
        ruleorder: fetch_rki_ndjson_from_s3 > transform_rki_data_to_ndjson
        ruleorder: fetch_main_ndjson_from_s3 > fetch_main_gisaid_ndjson
        ruleorder: fetch_biosample_from_s3 > fetch_biosample
        ruleorder: fetch_cog_uk_accessions_from_s3 > fetch_cog_uk_accessions
        ruleorder: fetch_cog_uk_metadata_from_s3 > uncompress_cog_uk_metadata
        ruleorder: compress_cog_uk_metadata > fetch_cog_uk_metadata
        ruleorder: fetch_main_ndjson_from_s3 > create_genbank_ndjson

    rule fetch_main_ndjson_from_s3:
        """Fetching main NDJSON from AWS S3"""
        params:
            file_on_s3_dst=f"{config['s3_dst']}/{database}.ndjson.zst",
            file_on_s3_src=f"{config['s3_src']}/{database}.ndjson.zst",
            lines = config.get("subsample",{}).get("main_ndjson", 0)
        output:
            ndjson = temp(f"data/{database}.ndjson")
        benchmark:
            "benchmarks/fetch_main_ndjson_from_s3.txt"
        shell:
            """
            ./vendored/download-from-s3 {params.file_on_s3_dst} {output.ndjson} {params.lines} ||  \
            ./vendored/download-from-s3 {params.file_on_s3_src} {output.ndjson} {params.lines}
            """

    rule fetch_biosample_from_s3:
        """Fetching BioSample NDJSON from AWS S3"""
        params:
            file_on_s3_dst=f"{config['s3_dst']}/biosample.ndjson.zst",
            file_on_s3_src=f"{config['s3_src']}/biosample.ndjson.zst",
            lines = config.get("subsample",{}).get("biosample", 0)
        output:
            biosample = temp("data/biosample.ndjson")
        benchmark:
            "benchmarks/fetch_biosample_from_s3.txt"
        shell:
            """
            ./vendored/download-from-s3 {params.file_on_s3_dst} {output.biosample} {params.lines} ||  \
            ./vendored/download-from-s3 {params.file_on_s3_src} {output.biosample} {params.lines}
            """

    rule fetch_rki_ndjson_from_s3:
        params:
            file_on_s3_dst=f"{config['s3_dst']}/rki.ndjson.zst",
            file_on_s3_src=f"{config['s3_src']}/rki.ndjson.zst",
            lines = config.get("subsample",{}).get("rki_ndjson", 0)
        output:
            rki_ndjson = temp("data/rki.ndjson")
        benchmark:
            "benchmarks/fetch_rki_ndjson_from_s3.txt"
        shell:
            """
            ./vendored/download-from-s3 {params.file_on_s3_dst} {output.rki_ndjson} {params.lines} ||  \
            ./vendored/download-from-s3 {params.file_on_s3_src} {output.rki_ndjson} {params.lines}
            """
    rule fetch_cog_uk_accessions_from_s3:
        params:
            file_on_s3_dst=f"{config['s3_dst']}/cog_uk_accessions.tsv.zst",
            file_on_s3_src=f"{config['s3_src']}/cog_uk_accessions.tsv.zst",
            lines = config.get("subsample",{}).get("cog_uk_accessions", 0)
        output:
            biosample = "data/cog_uk_accessions.tsv" if config.get("keep_temp",False) else temp("data/cog_uk_accessions.tsv")
        benchmark:
            "benchmarks/fetch_cog_uk_accessions_from_s3.txt"
        shell:
            """
            ./vendored/download-from-s3 {params.file_on_s3_dst} {output.biosample} {params.lines} ||  \
            ./vendored/download-from-s3 {params.file_on_s3_src} {output.biosample} {params.lines}
            """

    rule fetch_cog_uk_metadata_from_s3:
        params:
            file_on_s3_dst=f"{config['s3_dst']}/cog_uk_metadata.csv.zst",
            file_on_s3_src=f"{config['s3_src']}/cog_uk_metadata.csv.zst",
            lines = config.get("subsample",{}).get("cog_uk_metadata", 0)
        output:
            biosample = temp("data/cog_uk_metadata.csv")
        benchmark:
            "benchmarks/fetch_cog_uk_metadata_from_s3.txt"
        shell:
            """
            ./vendored/download-from-s3 {params.file_on_s3_dst} {output.biosample} {params.lines} ||  \
            ./vendored/download-from-s3 {params.file_on_s3_src} {output.biosample} {params.lines}
            """

    rule compress_cog_uk_metadata:
        input:
            "data/cog_uk_metadata.csv"
        output:
            cog_uk_metadata = "data/cog_uk_metadata.csv.gz" if config.get("keep_temp",False) else temp("data/cog_uk_metadata.csv.gz")
        benchmark:
            "benchmarks/compress_cog_uk_metadata.txt"
        shell:
            "gzip -c {input} > {output}"
