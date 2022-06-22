"""
This part of the workflow handles fetching sequences and metadata from GISAID
or NCBI GenBank/Biosample. Depends on the main Snakefile to define the variable
`database`, which is NOT a wildcard.

If `fetch_from_database=False` in config, then files will be fetched from AWS S3.
Or else, the data is fetched directly from the databases.

Produces different final outputs for GISAID vs GenBank:
    GISAID:
        ndjson = "data/gisaid.ndjson"
    GenBank:
        ndjson = "data/genbank.ndjson"
        biosample = "data/biosample.ndjson"
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

rule download_main_ndjson:
    message:
        """Fetching data using the database API"""
    params:
        file_on_s3_dst= f"{config['s3_dst']}/{database}.ndjson.xz",
        file_on_s3_src= f"{config['s3_src']}/{database}.ndjson.xz"
    output:
        ndjson = temp(f"data/{database}.ndjson")
    run:
        if config.get("fetch_from_database", False):
            if database=="gisaid":
                msg = "Fetching from GISAID API"
                cmd = f"./bin/fetch-from-gisaid {output.ndjson}"
            else:
                msg = "Fetching from GenBank API"
                cmd = f"./bin/fetch-from-genbank > {output.ndjson}"
            cleanup_failed_cmd = f"rm {output.ndjson}"
            run_shell_command_n_times(cmd, msg, cleanup_failed_cmd)
            if send_notifications:
                shell("./bin/notify-on-record-change {output.ndjson} {params.file_on_s3_src} {database}")
        else:
            shell("""
                ./bin/download-from-s3 {params.file_on_s3_dst} {output.ndjson} ||  \
                ./bin/download-from-s3 {params.file_on_s3_src} {output.ndjson}
            """)

rule download_biosample:
    message:
        """Obtaining Biosample data (GenBank only)"""
    params:
        file_on_s3_dst = config["s3_dst"] + '/biosample.ndjson.gz',
        file_on_s3_src = config["s3_src"] + '/biosample.ndjson.gz',
    output:
        biosample = "data/biosample.ndjson"
    run:
        if config.get("fetch_from_database", False):
            run_shell_command_n_times(
                f"./bin/fetch-from-biosample > {output.biosample}",
                "Fetch BioSample",
                f"rm {output.biosample}")
        else:
            shell("""
                ./bin/download-from-s3 {params.file_on_s3_dst} {output.biosample} ||  \
                ./bin/download-from-s3 {params.file_on_s3_src} {output.biosample}
            """)
