from subprocess import CalledProcessError

#################################################################
####################### general setup ###########################
#################################################################

database=config.get("database_name", "")
if database != "gisaid" and database != "genbank":
    print(f"[Fatal] An unknown database \"{database}\" was specified")
    sys.exit(1)

send_notifications = "SLACK_CHANNELS" in os.environ and "SLACK_TOKEN" in os.environ

#################################################################
################ work out what steps to run #####################
#################################################################

all_targets = [f"data/{database}/upload.done"]

if config.get("trigger_preprocessing", False):
    all_targets.append(f"data/{database}/trigger-preprocessing.done")
if send_notifications:
    all_targets.append(f"data/{database}/notify.done")
if config.get("fetch_from_database", False):
    all_targets.append(f"data/{database}/raw.upload.done")

rule all:
    input: all_targets

#################################################################
###################### rule definitions #########################
#################################################################


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
                cmd = f"./bin/fetch-from-gisaid > {output.ndjson}"
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
        file_on_s3_dst = config["s3_dst"] + '/biosample.ndjson.xz',
        file_on_s3_src = config["s3_src"] + '/biosample.ndjson.xz',
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

raw_files_to_upload = {f"{database}.ndjson.xz": f"data/{database}.ndjson"}

if database=="genbank":
    raw_files_to_upload["biosample.ndjson.gz"] = f"data/biosample.ndjson"

rule upload_raw_ndjson:
    input:
        **raw_files_to_upload
    output:
        touch(f"data/{database}/raw.upload.done")
    params:
        quiet = "" if send_notifications else "--quiet",
        s3_bucket = config["s3_dst"]
    run:
        for remote, local in input.items():
            shell("./bin/upload-to-s3 {params.quiet} {local:q} {params.s3_bucket:q}/{remote:q}")

rule transform_biosample:
    input:
        biosample = "data/biosample.ndjson"
    output:
        biosample = "data/genbank/biosample.tsv"
    shell:
        """
        ./bin/transform-biosample {input.biosample} \
            --output {output.biosample}
        """

rule transform_genbank_data:
    input:
        biosample = "data/genbank/biosample.tsv",
        ndjson = "data/genbank.ndjson"
    output:
        fasta = "data/genbank/sequences.fasta",
        metadata = "data/genbank/metadata_transformed.tsv",
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"
    shell:
        """
        ./bin/transform-genbank {input.ndjson} \
            --biosample {input.biosample} \
            --duplicate-biosample {output.duplicate_biosample} \
            --output-metadata {output.metadata} \
            --output-fasta {output.fasta}
        """

rule transform_gisaid_data:
    input:
        ndjson = "data/gisaid.ndjson"
    output:
        fasta = "data/gisaid/sequences.fasta",
        metadata = "data/gisaid/metadata_transformed.tsv",
        flagged_annotations = temp("data/gisaid/flagged-annotations"),
        additional_info = "data/gisaid/additional_info.tsv"
    shell:
        """
        ./bin/transform-gisaid {input.ndjson} \
            --output-metadata {output.metadata} \
            --output-fasta {output.fasta}  \
            --output-additional-info {output.additional_info} \
            --output-unix-newline > {output.flagged_annotations};
        """

rule download_nextclade:
    params:
        dst_source = config["s3_dst"] + '/nextclade.tsv.gz',
        src_source = config["s3_src"] + '/nextclade.tsv.gz',
    output:
        nextclade = f"data/{database}/nextclade_old.tsv"
    shell:
        """
        ./bin/download-from-s3 {params.dst_source} {output.nextclade} ||  \
        ./bin/download-from-s3 {params.src_source} {output.nextclade}
        """

rule get_sequences_without_nextclade_annotations:
    """Find sequences in FASTA which don't have clades assigned yet"""
    input:
        fasta = f"data/{database}/sequences.fasta",
        nextclade = f"data/{database}/nextclade_old.tsv",
    output:
        fasta = f"data/{database}/nextclade.sequences.fasta"
    shell:
        """
        ./bin/filter-fasta \
            --input_fasta={input.fasta} \
            --input_tsv={input.nextclade} \
            --output_fasta={output.fasta} \
        """

rule run_nextclade:
    message:
        """
        If there are sequences without clades, then run Nextclade to align them, assign clades and calculate some of
        the other useful metrics which will end up in metadata.tsv.
        """
    input:
        sequences = f"data/{database}/nextclade.sequences.fasta",
        nextclade_info = f"data/{database}/nextclade_old.tsv"
    params:
        old_aligned_fasta_s3 = f'{config["s3_dst"]}/nextclade.aligned.fasta.xz',
        old_aligned_fasta = f"data/{database}/nextclade.aligned.old.fasta",
        upd_aligned_fasta = temp(f"data/{database}/nextclade.aligned.upd.fasta"),
        aligned_fasta = f"data/{database}/nextclade.aligned.fasta",
        aligned_fasta_s3 = f'{config["s3_src"]}/nextclade.mutation_summary.tsv.gz',

        old_mutation_summary_s3 = f'{config["s3_src"]}/nextclade.mutation_summary.tsv.gz',
        old_mutation_summary = f"data/{database}/nextclade.mutation_summary.old.tsv",
        upd_mutation_summary = temp(f"data/{database}/nextclade.mutation_summary.upd.tsv"),
        new_mutation_summary = f"data/{database}/nextclade.mutation_summary.tsv",
        new_mutation_summary_s3 = f'{config["s3_dst"]}/nextclade.mutation_summary.tsv.gz',

        nextclade_info = lambda _, output: output.nextclade_info,
        new_info = temp(f"data/{database}/nextclade_new.tsv"),
        nextclade_input_dir = temp(directory(f"data/{database}/nextclade_inputs")),
        nextclade_output_dir = temp(directory(f"data/{database}/nextclade")),
        new_insertions = temp(f"data/{database}/nextclade.insertions.csv"),


        genes = "E,M,N,ORF1a,ORF1b,ORF3a,ORF6,ORF7a,ORF7b,ORF8,ORF9b,S"   # TODO: deduplicate with compute_mutation_summary step
    threads: 16
    output:
        nextclade_info = f"data/{database}/nextclade.tsv",

    # TODO: Split this into proper workflow rules
    # NOTE: The input.sequences are produced or not dynamically in the rule
    # `get_sequences_without_nextclade_annotations`. So if you split this script into rules,they need to be made
    # aware of that.
    # NOTE: It might be possible to workaround the dynamic nature of {input.sequences} by producing an ampty file
    # instead of no file and then make all the rules to produce empty outputs. This wil requre download and upload
    # to always run. Ideally we want to avoid unnecessary downloads and uploads.
    shell:
        """
        if [ ! -s {input.sequences:q} ]; then
            echo "[ INFO] No new sequences for Nextclade to process. Skipping nextclade."
            mv {input.nextclade_info} {output.nextclade_info}
        else
            ./bin/run-nextclade \
                {input.sequences:q} \
                {params.new_info} \
                {params.nextclade_input_dir} \
                {params.nextclade_output_dir} \
                {params.upd_aligned_fasta} \
                {params.new_insertions} \
                {params.genes} \
                {threads}

            # Join new and old clades, so that next run won't need to process sequences that are already processed
            ./bin/join-rows \
                {input.nextclade_info:q} \
                {params.new_info} \
                -o {output.nextclade_info:q}

            # Download old alignment
            ./bin/download-from-s3 {params.old_aligned_fasta_s3} {params.old_aligned_fasta}

            # Join new and old alignment
            cat {params.old_aligned_fasta}  \
                {params.upd_aligned_fasta} \
                >"{params.aligned_fasta}"

            # Commit changed alignment to S3
            # TODO: Should this be moved towards the end of the workflow?
            ./bin/upload-to-s3 --quiet {params.aligned_fasta} {params.aligned_fasta_s3}

            # Compute mutation summary for new sequences
            ./bin/mutation-summary \
                --basename="nextclade" \
                --directory={params.nextclade_output_dir} \
                --alignment={params.upd_aligned_fasta} \
                --insertions={params.new_insertions} \
                --reference={params.nextclade_input_dir}/reference.fasta \
                --genemap={params.nextclade_input_dir}/genemap.gff" \
                --genes {params.genes} \
                --output={params.upd_mutation_summary}

            # Download old mutation summary for the entire database so far
            ./bin/download-from-s3 {params.old_mutation_summary_s3} {params.old_mutation_summary}

            # Join the new updated mutation summary and old mutation summary to get the new result for the whole database
            ./bin/join-rows \
                {params.old_mutation_summary} \
                {params.upd_mutation_summary} \
                -o {params.new_mutation_summary}

            # Comit updated file to S3
            # TODO: Should this be moved towards the end of the workflow?
            ./bin/upload-to-s3 --quiet {params.new_mutation_summary} {params.new_mutation_summary_s3}
        fi
        """

rule generate_metadata:
    input:
        existing_metadata = f"data/{database}/metadata_transformed.tsv",
        new_metadata = f"data/{database}/nextclade.tsv",
    output:
        metadata = f"data/{database}/metadata.tsv"
    # note: the shell scripts which predated this snakemake workflow
    # overwrote the existing_metadata here
    shell:
        """
        ./bin/join-metadata-and-clades \
            {input.existing_metadata} \
            {input.new_metadata} \
            -o {output.metadata}
        """

rule flag_metadata:
    ### only applicable for GISAID
    input:
        metadata = "data/gisaid/metadata.tsv"
    output:
        metadata = "data/gisaid/flagged_metadata.txt"
    shell:
        """
        ./bin/flag-metadata {input.metadata} > {output.metadata}
        """

rule check_locations:
    input:
        metadata = f"data/{database}/metadata.tsv"
    params:
        unique_id = "gisaid_epi_isl" if database=="gisaid" else "genbank_accession"
    output:
        location_hierarchy = f"data/{database}/location_hierarchy.tsv"
    shell:
        """
        ./bin/check-locations {input.metadata} {output.location_hierarchy} {params.unique_id}
        """

rule notify_gisaid:
    input:
        flagged_annotations = rules.transform_gisaid_data.output.flagged_annotations,
        # metadata = "data/gisaid/metadata.tsv",
        additional_info = "data/gisaid/additional_info.tsv",
        flagged_metadata = "data/gisaid/flagged_metadata.txt",
        location_hierarchy = "data/gisaid/location_hierarchy.tsv"
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
        shell("./bin/notify-on-location-hierarchy-addition {input.location_hierarchy} source-data/location_hierarchy.tsv")

rule notify_genbank:
    input:
        metadata = "data/genbank/metadata.tsv",
        location_hierarchy = "data/genbank/location_hierarchy.tsv",
        duplicate_biosample = "data/genbank/duplicate_biosample.txt"
    params:
        s3_bucket = config["s3_src"]
    output:
        touch("data/genbank/notify.done")
    run:
        shell("./bin/notify-on-metadata-change {input.metadata} {params.s3_bucket}/metadata.tsv.gz genbank_accession")
        # TODO - which rule produces data/genbank/problem_data.tsv? (was not explicit in `ingest-genbank` bash script)
        shell("./bin/notify-on-problem-data data/genbank/problem_data.tsv")
        shell("./bin/notify-on-location-hierarchy-addition {input.location_hierarchy} source-data/location_hierarchy.tsv")
        shell("./bin/notify-on-duplicate-biosample-change {input.duplicate_biosample} {params.s3_bucket}/duplicate_biosample.txt.gz")

files_to_upload = {
                    "metadata.tsv.gz":              f"data/{database}/metadata.tsv",
                    "sequences.fasta.xz":           f"data/{database}/sequences.fasta",
                    "nextclade.tsv.gz":             f"data/{database}/nextclade.tsv"}
if database=="genbank":
    files_to_upload["biosample.tsv.gz"] =           f"data/{database}/biosample.tsv"
    files_to_upload["duplicate_biosample.txt.gz"] = f"data/{database}/duplicate_biosample.txt"
elif database=="gisaid":
    files_to_upload["additional_info.tsv.gz"] =     f"data/{database}/additional_info.tsv"
    files_to_upload["flagged_metadata.txt.gz"] =    f"data/{database}/flagged_metadata.txt"

rule upload:
    input:
        **files_to_upload
    output:
        touch(f"data/{database}/upload.done")
    params:
        quiet = "" if send_notifications else "--quiet",
        s3_bucket = config["s3_dst"]
    run:
        for remote, local in input.items():
            shell("./bin/upload-to-s3 {params.quiet} {local:q} {params.s3_bucket:q}/{remote:q}")


rule trigger_preprocessing_pipeline:
    message: "Triggering nextstrain/ncov rebuild action (via repository dispatch)"
    input:
        f"data/{database}/upload.done"
    output:
        touch(f"data/{database}/trigger-preprocessing.done")
    params:
        dispatch_type = f"preprocess-{database}",
        token = os.environ.get("PAT_GITHUB_DISPATCH", "")
    run:
        import requests
        headers = {
                'Content-type': 'application/json',
                'authorization': f"Bearer {params.token}",
                'Accept': 'application/vnd.github.v3+json'}
        data = {"event_type": params.dispatch_type}
        print(f"Triggering ncov preprocessing GitHub action via repository dispatch type: {params.dispatch_type}")
        response = requests.post("https://api.github.com/repos/nextstrain/ncov/dispatches", headers=headers, data=json.dumps(data))
        response.raise_for_status()

################################################################
################################################################

# A helpful list of environment variables in use by various scripts
env_variables = {
    "AWS_DEFAULT_REGION": "Required for S3 access",
    "AWS_ACCESS_KEY_ID": "Required for S3 access",
    "AWS_SECRET_ACCESS_KEY": "Required for S3 access",
    "GITHUB_RUN_ID": "Included in slack notification message (optional)",
    "SLACK_TOKEN": "Required for sending slack notifications",
    "SLACK_CHANNELS": "Required for sending slack notifications",
    "PAT_GITHUB_DISPATCH": "Required for triggering preprocessing GitHub actions",
    "GISAID_API_ENDPOINT": "Required for GISAID API access",
    "GISAID_USERNAME_AND_PASSWORD": "Required for GISAID API access"
}

onstart:
    print(f"Pipeline starting.")
    print(f"Source s3 bucket: {config['s3_src']}, destination: {config['s3_dst']}")
    print("Environment variables present:")
    for var, description in env_variables.items():
        print(f"\t${{{var}}}: " + ("YES" if os.environ.get(var, "") else "NO") + f"({description})")
    if send_notifications:
        message="🥗 GISAID ingest" if database=="gisaid" else "🥣 GenBank ingest"
        shell(f"./bin/notify-on-job-start \"{message}\"")

onsuccess:
    message = "✅ This pipeline has successfully finished 🎉"
    print(message)
    if not config.get("keep_all_files", False):
        print("Removing intermediate files (set config option keep_all_files to skip this)")
        shell("./bin/clean")

onerror:
    message = "❌ This pipeline has FAILED 😞. Please see linked thread for more information."
    print(message)
    if not config.get("keep_all_files", False):
        print("Removing intermediate files (set config option keep_all_files to skip this)")
        shell("./bin/clean")
