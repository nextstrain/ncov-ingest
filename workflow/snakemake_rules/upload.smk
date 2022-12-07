"""
This part of the workflow handles uploading files to AWS S3.
Depends on the main Snakefile to define the variable `database`, which is NOT a wildcard.

See `files_to_upload` for the list of
expected inputs.

Produces the following outputs:
    "data/{database}/upload.done"
These output files are empty flag files to force Snakemake to run the upload rules.

Note: we are doing parallel uploads of zstd compressed files to slowly make the transition to this format.
"""

def compute_files_to_upload():
    """
    Compute files to upload
    The keys are the name of the file once uploaded to S3
    The values are the local paths to the file to be uploaded
    """

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
                        "nextclade_21L.tsv.zst":       f"data/{database}/nextclade_21L.tsv",
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
        
    # Include upload of raw NDJSON if we are fetching new sequences from database
    if config.get("fetch_from_database", False):
        files_to_upload.update({
            f"{database}.ndjson.xz": f"data/{database}.ndjson",
            f"{database}.ndjson.zst": f"data/{database}.ndjson",
        })
        if database=="genbank":
            files_to_upload.update({
                "biosample.ndjson.gz": f"data/biosample.ndjson",
                "biosample.ndjson.zst": f"data/biosample.ndjson",

                "rki.ndjson.zst": f"data/rki.ndjson",

                "cog_uk_accessions.tsv.gz": f"data/cog_uk_accessions.tsv",
                "cog_uk_accessions.tsv.zst": f"data/cog_uk_accessions.tsv",

                "cog_uk_metadata.csv.gz": f"data/cog_uk_metadata.csv",
                "cog_uk_metadata.csv.zst": f"data/cog_uk_metadata.csv",
            })
    return files_to_upload

files_to_upload = compute_files_to_upload()


rule upload_single:
    input: lambda w: files_to_upload[w.remote_filename]
    output:
        "data/{database}/{remote_filename}.upload",
    params:
        quiet = "" if send_notifications else "--quiet",
        s3_bucket = config.get("s3_dst",""),
        cloudfront_domain = config.get("cloudfront_domain", ""),
    shell:
        """
        ./bin/upload-to-s3 \
            {params.quiet} \
            {input:q} \
            {params.s3_bucket:q}/{wildcards.remote_filename:q} \
            {params.cloudfront_domain} 2>&1 | tee {output}
        """

rule upload:
    """
    Requests one touch file for each uploaded remote file
    Dynamically determines that list of files
    """
    input: [f"data/{database}/{remote_file}.upload" for remote_file in files_to_upload.keys()]
    output:
        touch(f"data/{database}/upload.done")
