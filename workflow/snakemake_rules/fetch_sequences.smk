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

def run_shell_command_n_times(cmd, msg, cleanup_failed_cmd, retry_num=5):
    attempt = 0
    while attempt < retry_num:
        print(f"{msg} attempt number {attempt}")
        try:
            shell(cmd)
            break
        except CalledProcessError:
            print("...FAILED")
            attempt+=1
            shell("{cleanup_failed_cmd} && sleep 10")
    else:
        print(msg + f" has FAILED {retry_num} times. Exiting.")
        raise Exception("function run_shell_command_n_times has failed")

rule fetch_main_ndjson:
    message:
        """Fetching data using the database API"""
    output:
        ndjson = temp(f"data/{database}.ndjson")
    run:
        if database == "gisaid":
            cmd = f"./bin/fetch-from-gisaid {output.ndjson}"
        else:
            cmd = f"./bin/fetch-from-genbank > {output.ndjson}"

        run_shell_command_n_times(
            cmd,
            f"Fetching from {database}",
            f"rm {output.ndjson}"
        )

rule fetch_biosample:
    message:
        """Fetching BioSample data (GenBank only)"""
    output:
        biosample = temp("data/biosample.ndjson")
    run:
        run_shell_command_n_times(
            f"./bin/fetch-from-biosample > {output.biosample}",
            "Fetch BioSample",
            f"rm {output.biosample}"
        )

rule fetch_cog_uk_accessions:
    message:
        """Fetching COG-UK sample accesions (GenBank only)"""
    output:
        cog_uk_accessions = temp("data/cog_uk_accessions.tsv")
    run:
        run_shell_command_n_times(
            f"./bin/fetch-from-cog-uk-accessions > {output.cog_uk_accessions}",
            "Fetch COG-UK sample accessions",
            f"rm {output.cog_uk_accessions}"
        )

rule fetch_cog_uk_metadata:
    message:
        """Fetching COG-UK metadata (GenBank only)"""
    output:
        cog_uk_metadata = temp("data/cog_uk_metadata.csv.gz")
    run:
        run_shell_command_n_times(
            f"./bin/fetch-from-cog-uk-metadata > {output.cog_uk_metadata}",
            "Fetch COG-UK metadata",
            f"rm {output.cog_uk_metadata}"
        )


rule fetch_rki_sequences:
    output:
        rki_sequences=temp("data/rki_sequences.fasta.xz"),
    run:
        run_shell_command_n_times(
            f"./bin/fetch-from-rki-sequences > {output.rki_sequences}",
            "Fetch RKI sequences",
            f"rm {output.rki_sequences}",
        )


rule fetch_rki_metadata:
    output:
        rki_metadata=temp("data/rki_metadata.csv.xz"),
    run:
        run_shell_command_n_times(
            f"./bin/fetch-from-rki-metadata > {output.rki_metadata}",
            "Fetch RKI metadata",
            f"rm {output.rki_metadata}",
        )


rule fetch_rki_lineages:
    output:
        rki_lineages=temp("data/rki_lineages.csv.xz"),
    run:
        run_shell_command_n_times(
            f"./bin/fetch-from-rki-lineages > {output.rki_lineages}",
            "Fetch RKI lineages",
            f"rm {output.rki_lineages}",
        )


rule transform_rki_data_to_ndjson:
    input:
        rki_sequences="data/rki_sequences.fasta.xz",
        rki_metadata="data/rki_metadata.csv.xz",
        rki_lineages="data/rki_lineages.csv.xz",
    output:
        ndjson="data/rki.ndjson",
    shell:
        """
        ./bin/transform-rki-data-to-ndjson \
            --input-rki-sequences {input.rki_sequences} \
            --input-rki-metadata {input.rki_metadata} \
            --input-rki-lineages {input.rki_lineages} \
            --output-ndjson {output.ndjson}
        """


# Only include rules to fetch from S3 if S3 config params are provided
if config.get("s3_dst") and config.get("s3_src"):

    # Set ruleorder since these fetch rules have the same output
    # Fetch directly from databases when `fetch_from_database=True`
    # or else fetch files from AWS S3 buckets
    if config.get("fetch_from_database", False):
        ruleorder: fetch_main_ndjson > fetch_main_ndjson_from_s3
        ruleorder: fetch_biosample > fetch_biosample_from_s3
        ruleorder: transform_rki_data_to_ndjson > fetch_rki_ndjson_from_s3
    else:
        ruleorder: fetch_main_ndjson_from_s3 > fetch_main_ndjson
        ruleorder: fetch_biosample_from_s3 > fetch_biosample
        ruleorder: fetch_rki_ndjson_from_s3 > transform_rki_data_to_ndjson 
        ruleorder: fetch_cog_uk_accessions > fetch_cog_uk_accessions_from_s3
        ruleorder: fetch_cog_uk_metadata > compress_cog_uk_metadata
        ruleorder: uncompress_cog_uk_metadata > fetch_cog_uk_metadata_from_s3
    else:
        ruleorder: fetch_main_ndjson_from_s3 > fetch_main_ndjson
        ruleorder: fetch_biosample_from_s3 > fetch_biosample
        ruleorder: fetch_cog_uk_accessions_from_s3 > fetch_cog_uk_accessions
        ruleorder: fetch_cog_uk_metadata_from_s3 > uncompress_cog_uk_metadata
        ruleorder: compress_cog_uk_metadata > fetch_cog_uk_metadata

    rule fetch_main_ndjson_from_s3:
        message:
            """Fetching main NDJSON from AWS S3"""
        params:
            file_on_s3_dst=f"{config['s3_dst']}/{database}.ndjson.zst",
            file_on_s3_src=f"{config['s3_src']}/{database}.ndjson.zst",
            lines = config.get("subsample",{}).get("main_ndjson", 0)
        output:
            ndjson = temp(f"data/{database}.ndjson")
        shell:
            """
            ./bin/download-from-s3 {params.file_on_s3_dst} {output.ndjson} {params.lines} ||  \
            ./bin/download-from-s3 {params.file_on_s3_src} {output.ndjson} {params.lines}
            """

    rule fetch_biosample_from_s3:
        message:
            """Fetching BioSample NDJSON from AWS S3"""
        params:
            file_on_s3_dst=f"{config['s3_dst']}/biosample.ndjson.zst",
            file_on_s3_src=f"{config['s3_src']}/biosample.ndjson.zst",
            lines = config.get("subsample",{}).get("biosample", 0)
        output:
            biosample = temp("data/biosample.ndjson")
        shell:
            """
            ./bin/download-from-s3 {params.file_on_s3_dst} {output.biosample} {params.lines} ||  \
            ./bin/download-from-s3 {params.file_on_s3_src} {output.biosample} {params.lines}
            """

    rule fetch_rki_ndjson_from_s3:
        params:
            file_on_s3_dst=f"{config['s3_dst']}/rki.ndjson.zst",
            file_on_s3_src=f"{config['s3_src']}/rki.ndjson.zst",
        output:
            rki_ndjson = temp("data/rki.ndjson")
        shell:
            """
            ./bin/download-from-s3 {params.file_on_s3_dst} {output.rki_ndjson} ||  \
            ./bin/download-from-s3 {params.file_on_s3_src} {output.rki_ndjson}
            """
    rule fetch_cog_uk_accessions_from_s3:
        params:
            file_on_s3_dst=f"{config['s3_dst']}/cog_uk_accessions.tsv.zst",
            file_on_s3_src=f"{config['s3_src']}/cog_uk_accessions.tsv.zst",
            lines = config.get("subsample",{}).get("cog_uk_accessions", 0)
        output:
            biosample = "data/cog_uk_accessions.tsv" if config.get("keep_temp",False) else temp("data/cog_uk_accessions.tsv")
        shell:
            """
            ./bin/download-from-s3 {params.file_on_s3_dst} {output.biosample} {params.lines} ||  \
            ./bin/download-from-s3 {params.file_on_s3_src} {output.biosample} {params.lines}
            """

    rule fetch_cog_uk_metadata_from_s3:
        params:
            file_on_s3_dst=f"{config['s3_dst']}/cog_uk_metadata.csv.zst",
            file_on_s3_src=f"{config['s3_src']}/cog_uk_metadata.csv.zst",
            lines = config.get("subsample",{}).get("cog_uk_metadata", 0)
        output:
            biosample = temp("data/cog_uk_metadata.csv")
        shell:
            """
            ./bin/download-from-s3 {params.file_on_s3_dst} {output.biosample} {params.lines} ||  \
            ./bin/download-from-s3 {params.file_on_s3_src} {output.biosample} {params.lines}
            """
    
    rule compress_cog_uk_metadata:
        input:
            "data/cog_uk_metadata.csv"
        output:
            cog_uk_metadata = "data/cog_uk_metadata.csv.gz" if config.get("keep_temp",False) else temp("data/cog_uk_metadata.csv.gz")
        shell:
            "gzip -c {input} > {output}"
