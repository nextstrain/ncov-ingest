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

all_targets = [f"data/{database}/upload.done"]

if config.get("trigger_rebuild", False):
    all_targets.append(f"data/{database}/trigger-rebuild.done")
if config.get("trigger_counts", False):
    all_targets.append(f"data/{database}/trigger-counts.done")
if send_notifications:
    all_targets.append(f"data/{database}/notify.done")
if config.get("fetch_from_database", False):
    all_targets.append(f"data/{database}/raw.upload.done")

rule all:
    input: all_targets

#################################################################
###################### rule definitions #########################
#################################################################

include: "workflow/snakemake_rules/fetch_sequences.smk"

include: "workflow/snakemake_rules/curate.smk"

include: "workflow/snakemake_rules/nextclade.smk"

include: "workflow/snakemake_rules/slack_notifications.smk"

include: "workflow/snakemake_rules/upload.smk"

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
    print("Pipeline failed.")
    if send_notifications:
        shell("./bin/notify-on-job-fail")
    if not config.get("keep_all_files", False):
        print("Removing intermediate files (set config option keep_all_files to skip this)")
        shell("./bin/clean")
