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

wildcard_constraints:
    # Constrain GISAID pair names to "gisaid_cache" or YYYY-MM-DD-N
    gisaid_pair = r'gisaid_cache|\d{4}-\d{2}-\d{2}(-\d+)?'


if config.get("s3_src"):

    rule fetch_gisaid_ndjson:
        """
        Fetch previously uploaded gisaid.ndjson if it exists.
        This is a cache of the raw data from previous GISAID ingest(s).
        If it doesn't exist, then just create an empty file.
        """
        output:
            ndjson=temp("data/gisaid/gisaid_cache.ndjson"),
        params:
            s3_file=f"{config['s3_src']}/gisaid.ndjson.zst",
        shell:
            r"""
            if $(./vendored/s3-object-exists {params.s3_file:q}); then
                ./vendored/download-from-s3 {params.s3_file:q} {output.ndjson:q}
            else
                echo "{params.s3_file:q} does not exist, creating empty file."
                touch {output.ndjson:q}
            fi
            """

    checkpoint fetch_unprocessed_files:
        """
        Fetch unprocessed GISAID files.
        These are pairs of metadata.tsv.zst and sequences.fasta.zst files.

        This is a checkpoint because the DAG needs to be re-evaluated to determine
        which `gisaid_pair` need to be processed.
        """
        output:
            directory("data/unprocessed-gisaid-downloads/"),
        params:
            s3_prefix=f"{config['s3_src']}/gisaid-downloads/unprocessed/"
        shell:
            r"""
            aws s3 cp {params.s3_prefix:q} {output:q} \
                --recursive \
                --exclude "*" \
                --include "*-metadata.tsv.zst" \
                --include "*-sequences.fasta.zst"
            """

    rule decompress_unprocessed_files:
        input:
            metadata="data/unprocessed-gisaid-downloads/{gisaid_pair}-metadata.tsv.zst",
            sequences="data/unprocessed-gisaid-downloads/{gisaid_pair}-sequences.fasta.zst",
        output:
            metadata=temp("data/gisaid/{gisaid_pair}-metadata.tsv"),
            sequences=temp("data/gisaid/{gisaid_pair}-sequences.fasta"),
        shell:
            r"""
            zstd --decompress --stdout {input.metadata:q} > {output.metadata:q}
            zstd --decompress --stdout {input.sequences:q} > {output.sequences:q}
            """


rule link_gisaid_metadata_and_fasta:
    input:
        metadata="data/gisaid/{gisaid_pair}-metadata.tsv",
        sequences="data/gisaid/{gisaid_pair}-sequences.fasta",
    output:
        ndjson=temp("data/gisaid/{gisaid_pair}.ndjson"),
    params:
        seq_id_column="strain",
        seq_field="sequence",
    log: "logs/link_gisaid_metadata_and_fasta/{gisaid_pair}.txt"
    shell:
        r"""
        augur curate passthru \
            --metadata {input.metadata:q} \
            --fasta {input.sequences:q} \
            --seq-id-column {params.seq_id_column:q} \
            --seq-field {params.seq_field:q} \
            | ./bin/transform-to-gisaid-cache \
                > {output.ndjson:q} \
                2> {log:q}
        """

def aggregate_gisaid_ndjsons(wildcards):
    """
    Input function for rule concatenate_gisaid_ndjsons to check which
    GISAID pairs to include the output.
    """
    if len(config.get("gisaid_pairs", [])):
        GISAID_PAIRS = config["gisaid_pairs"]
    elif config.get('s3_src') and hasattr(checkpoints, "fetch_unprocessed_files"):
        # Use checkpoint for the Nextstrain automation
        checkpoint_output = checkpoints.fetch_unprocessed_files.get(**wildcards).output[0]
        GISAID_PAIRS, = glob_wildcards(os.path.join(checkpoint_output, "{gisaid_pair}-metadata.tsv.zst"))
        # Reverse sort to list latest downloads first
        GISAID_PAIRS.sort(reverse=True)
        # Add the GISAID cache last to prioritize the latest downloads
        GISAID_PAIRS.append("gisaid_cache")
    else:
        # Create wildcards for pairs of GISAID downloads
        GISAID_PAIRS, = glob_wildcards("data/gisaid/{gisaid_pair}-metadata.tsv")
        # Reverse sort to list latest downloads first
        GISAID_PAIRS.sort(reverse=True)

    assert len(GISAID_PAIRS), "No GISAID metadata and sequences inputs were found"

    return expand("data/gisaid/{gisaid_pair}.ndjson", gisaid_pair=GISAID_PAIRS)


rule concatenate_gisaid_ndjsons:
    input:
        ndjsons=aggregate_gisaid_ndjsons,
    output:
        ndjson=temp("data/gisaid.ndjson"),
    params:
        gisaid_id_field="covv_accession_id",
    log: "logs/concatenate_gisaid_ndjsons.txt"
    shell:
        r"""
        (cat {input.ndjsons:q} \
            | ./bin/dedup-by-gisaid-id \
                --id-field {params.gisaid_id_field:q} \
            > {output.ndjson:q}) 2> {log:q}
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
            --include "genome,biosample" \
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

rule extract_ncbi_dataset_biosample:
    input:
        dataset_package = "data/ncbi_dataset.zip"
    output:
        biosample = temp("data/biosample.ndjson")
    benchmark:
        "benchmarks/extract_ncbi_dataset_biosample.txt"
    shell:
        """
        unzip -jp {input.dataset_package} \
            ncbi_dataset/data/biosample_report.jsonl > {output.biosample}
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
        ruleorder: extract_ncbi_dataset_biosample > fetch_biosample_from_s3
        ruleorder: transform_rki_data_to_ndjson > fetch_rki_ndjson_from_s3
        ruleorder: fetch_cog_uk_accessions > fetch_cog_uk_accessions_from_s3
        ruleorder: fetch_cog_uk_metadata > compress_cog_uk_metadata
        ruleorder: uncompress_cog_uk_metadata > fetch_cog_uk_metadata_from_s3
        ruleorder: create_genbank_ndjson > fetch_main_ndjson_from_s3
        ruleorder: concatenate_gisaid_ndjsons > fetch_main_ndjson_from_s3
    else:
        ruleorder: fetch_rki_ndjson_from_s3 > transform_rki_data_to_ndjson
        ruleorder: fetch_biosample_from_s3 > extract_ncbi_dataset_biosample
        ruleorder: fetch_cog_uk_accessions_from_s3 > fetch_cog_uk_accessions
        ruleorder: fetch_cog_uk_metadata_from_s3 > uncompress_cog_uk_metadata
        ruleorder: compress_cog_uk_metadata > fetch_cog_uk_metadata
        ruleorder: fetch_main_ndjson_from_s3 > create_genbank_ndjson
        ruleorder: fetch_main_ndjson_from_s3 > concatenate_gisaid_ndjsons

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
