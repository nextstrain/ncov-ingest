"""
This part of the workflow handles uploading files to AWS S3.
Depends on the main Snakefile to define the variable `database`, which is NOT a wildcard.

See `raw_files_to_upload` and `compute_files_to_upload` for the list of
expected inputs.

Produces the following outputs:
    "data/{database}/raw.upload.done"
    "data/{database}/upload.done"
These output files are empty flag files to force Snakemake to run the upload rules.

Note: we are doing parallel uploads of zstd compressed files to slowly make the transition to this format.
"""

raw_files_to_upload = {
    f"{database}.ndjson.xz": f"data/{database}.ndjson",
    f"{database}.ndjson.zst": f"data/{database}.ndjson",
}

if database=="genbank":
    raw_files_to_upload["biosample.ndjson.gz"] = f"data/biosample.ndjson"
    raw_files_to_upload["biosample.ndjson.zst"] = f"data/biosample.ndjson"

rule upload_raw_ndjson:
    input:
        **raw_files_to_upload
    output:
        touch(f"data/{database}/raw.upload.done")
    params:
        quiet = "" if send_notifications else "--quiet",
        s3_bucket = config.get("s3_dst",""),
        cloudfront_domain = config.get("cloudfront_domain", "")
    run:
        for remote, local in input.items():
            shell("./bin/upload-to-s3 {params.quiet} {local:q} {params.s3_bucket:q}/{remote:q} {params.cloudfront_domain}")

def compute_files_to_upload(wildcards):
    files_to_upload = {
                        "metadata.tsv.gz":              f"data/{database}/metadata.tsv",
                        "sequences.fasta.xz":           f"data/{database}/sequences.fasta",

                        "metadata.tsv.zst":             f"data/{database}/metadata.tsv",
                        "sequences.fasta.zst":          f"data/{database}/sequences.fasta",

                        # It shouldn't harm to upload these as upload-to-s3 only updates if hashes differ
                        "nextclade.tsv.gz":           f"data/{database}/nextclade.tsv",
                        "aligned.fasta.xz":           f"data/{database}/aligned.fasta",

                        "nextclade.tsv.zst":           f"data/{database}/nextclade.tsv",
                        "aligned.fasta.zst":           f"data/{database}/aligned.fasta",
                    }

    if database=="genbank":
        files_to_upload["biosample.tsv.gz"] =           f"data/{database}/biosample.tsv"
        files_to_upload["duplicate_biosample.txt.gz"] = f"data/{database}/duplicate_biosample.txt"

        files_to_upload["biosample.tsv.zst"] =           f"data/{database}/biosample.tsv"
        files_to_upload["duplicate_biosample.txt.zst"] = f"data/{database}/duplicate_biosample.txt"
    elif database=="gisaid":
        files_to_upload["additional_info.tsv.gz"] =     f"data/{database}/additional_info.tsv"
        files_to_upload["flagged_metadata.txt.gz"] =    f"data/{database}/flagged_metadata.txt"

        files_to_upload["additional_info.tsv.zst"] =     f"data/{database}/additional_info.tsv"
        files_to_upload["flagged_metadata.txt.zst"] =    f"data/{database}/flagged_metadata.txt"

    return files_to_upload

def files_to_upload(wildcards):
    files_to_upload_dict = compute_files_to_upload(wildcards)
    return [f"data/{database}/{remote_file}.upload" for remote_file in files_to_upload_dict.keys()]


rule upload_single:
    input: lambda wildcards: compute_files_to_upload(wildcards)[wildcards.remote_filename]
    output:
        touch("data/{database}/{remote_filename}.upload")
    params:
        quiet = "" if send_notifications else "--quiet",
        s3_bucket = config.get("s3_dst",""),
        cloudfront_domain = config.get("cloudfront_domain", ""),
        local_filename = lambda wildcards: compute_files_to_upload(wildcards)[wildcards.remote_filename]
    shell:
        "./bin/upload-to-s3 {params.quiet} {input:q} {params.s3_bucket:q}/{wildcards.remote_filename:q} {params.cloudfront_domain}"

rule upload:
    """
    Requests one touch file for each uploaded remote file
    Dynamically determines that list of files
    """
    input:
        files_to_upload
    output:
        touch(f"data/{database}/upload.done")
