from subprocess import CalledProcessError
import os

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

all_targets = [
    f"data/{database}/metadata.tsv",
    f"data/{database}/sequences.fasta",
    f"data/{database}/aligned.fasta",
]

# Include targets for uploading to S3 if `s3_dst` is provided in config
if config.get("s3_dst"):
    all_targets.append(f"data/{database}/upload.done")

    # Only check for trigger config if `s3_dst` is provided because we only
    # want to trigger builds if we've uploaded the output files to S3.
    if config.get("trigger_rebuild", False):
        all_targets.append(f"data/{database}/trigger-rebuild.done")
    if config.get("trigger_counts", False):
        all_targets.append(f"data/{database}/trigger-counts.done")

# Include targets for Slack notifications if Slack env variables are provided
# and the `s3_src` is provided in config since some notify scripts depend
# do diffs with files on S3 from previous runs
if send_notifications and config.get("s3_src"):
    all_targets.extend([
        f"data/{database}/notify-on-record-change.done",
        f"data/{database}/notify.done"
    ])

rule all:
    input: all_targets

#################################################################
###################### rule definitions #########################
#################################################################

include: "workflow/snakemake_rules/fetch_sequences.smk"

include: "workflow/snakemake_rules/curate.smk"

include: "workflow/snakemake_rules/nextclade.smk"

if send_notifications and config.get("s3_src"):
    include: "workflow/snakemake_rules/slack_notifications.smk"

if config.get("s3_dst"):
    include: "workflow/snakemake_rules/upload.smk"
    # Only include rules for trigger if uploading files since the trigger
    # rules depend on the outputs from upload.
    include: "workflow/snakemake_rules/trigger.smk"

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
    "PAT_GITHUB_DISPATCH": "Required for triggering GitHub actions (e.g. to rebuild nextstrain/ncov)",
    "GISAID_API_ENDPOINT": "Required for GISAID API access",
    "GISAID_USERNAME_AND_PASSWORD": "Required for GISAID API access"
}

onstart:
    print(f"Pipeline starting.")
    print(f"Source s3 bucket: {config.get('s3_src', 'N/A')}, destination: {config.get('s3_dst', 'N/A')}")
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
    print("Pipeline failed.")
    if send_notifications:
        shell("./bin/notify-on-job-fail")
    if not config.get("keep_all_files", False):
        print("Removing intermediate files (set config option keep_all_files to skip this)")
        shell("./bin/clean")
