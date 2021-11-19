# nCoV Ingestion Pipeline

Internal tooling for the Nextstrain team to ingest and curate SARS-CoV-2 genome sequences. This is open source, but we are not intending to support this to be used by outside groups.
Relies on data from https://simplemaps.com/data/us-cities.

## Running locally
If you're using Pipenv (see below), then run commands from `./bin/…` inside a `pipenv shell` or wrapped with `pipenv run ./bin/…`.

1. Run `./bin/fetch-from-gisaid > data/gisaid.ndjson`
2. Run `./bin/transform-gisaid data/gisaid.ndjson`
3. Look at `data/gisaid/sequences.fasta` and `data/gisaid/metadata.tsv`

## Running automatically
The ingest pipelines are triggered from the GitHub workflows `.github/workflows/ingest-master-*.yml` and `…/ingest-branch-*.yml` but run on AWS Batch via the `nextstrain build --aws-batch` infrastructure.
They're run on pushes to `master` that modify `source-data/*-annotations.tsv` and on pushes to other branches.
Pushes to branches other than `master` upload files to branch-specific paths in the S3 bucket, don't send notifications, and don't trigger Nextstrain rebuilds, so that they don't interfere with the production data.

AWS credentials are stored in this repository's secrets and are associated with the `nextstrain-ncov-ingest-uploader` IAM user in the Bedford Lab AWS account, which is locked down to reading and publishing only the `gisaid.ndjson`, `metadata.tsv`, and `sequences.fasta` files and their zipped equivalents in the `nextstrain-ncov-private` S3 bucket.

## Manually triggering the automation
A full run is a now done in 3 steps via manual triggers:
1. Fetch new sequences and ingest them by running `./bin/trigger gisaid/fetch-and-ingest --user <your-github-username>`.
2. Add manual annotations, update location hierarchy as needed, and run ingest without fetching new sequences.
    * Pushes of `source-data/*-annotations.tsv` to the master branch will automatically trigger a run of ingest.
    * You can also run ingest manually by running `./bin/trigger gisaid/ingest --user <your-github-username>`.
3. Once all manual fixes are complete, trigger a rebuild of [nextstrain/ncov](https://github.com/nextstrain/ncov) by running `./bin/trigger rebuild --user <your-github-username>`.

See the output of `./bin/trigger gisaid/fetch-and-ingest --user <your-github-username>`, `./bin/trigger gisaid/ingest` or `./bin/trigger rebuild` for more information about authentication with GitHub.

Note: running `./bin/trigger` posts a GitHub `repository_dispatch`.
Regardless of which branch you are on, it will trigger the specified action on the master branch.

Valid dispatch types for `./bin/trigger` are:

  - `ingest` (both GISAID and GenBank)
  - `gisaid/ingest`
  - `genbank/ingest`
  - `gisaid/fetch-and-ingest`
  - `rebuild`

## Updating manual annotations
Manual annotations should be added to `source-data/gisaid_annotations.tsv`.
A common pattern is expected to be:

 1. Run <https://github.com/nextstrain/ncov>.
 2. Discover metadata that needs fixing.
 3. Update `source-data/gisaid_annotations.tsv`.
 4. Push changes to `master` and re-download `gisaid/metadata.tsv`.

## Updating manual location hierarchy
New location hierarchies should be manually added to `source-data/location_hierarchy.tsv`.
A common pattern is expected to be:

 1. Run the ingest.
 2. Discover new location hierarchies via Slack that need review.
 3. Update `source-data/location_hierarchy.tsv`.
 4. Push changes to `master` so the next ingest will have an updated "source of truth" to draw from.

## Configuring alerts for new GISAID data from specific location hierarchy areas
Some Nextstrain team members may be interested in receiving alerts when new GISAID strains are added from specific locations, e.g. Portugal or Louisiana.
To add a custom alert configuration, create a new entry in `new-sequence-alerts-config.json`.
Each resolution (region, division, country, location) accepts a list of strings of areas of interest.
Note that these strings must match the area name exactly.

To set up custom alerts, you'll need to retrieve your Slack member ID.
Note that the `user` field in each alert configuration is for human use only -- it need not match your Slack display name or username.
To view your Slack member ID, open up the Slack menu by clicking your name at the top, and click on 'View profile'.
Then, click on 'More'.
You can then copy your Slack member ID from the menu that appears.
Enter this into the `slack_member_id` field of your alert configuration.

## Refreshing clades: Nextclade full run

Clades assigned with Nextclade are currently cached in `nextclade.tsv` in the S3 bucket and only incremental additions for the new sequences are performed during the daily ingests. This clade cache goes stale with time. It is necessary to perform full update of `nextclade.tsv` file periodically, recomputing clades for all of the GISAID and GenBank sequences all over again, to account for changes in the data and in Nextclade algorithms. 

The most convenient option is to trigger it through the corresponding GitHub Action:

 - [GISAID full Nextclade run](https://github.com/nextstrain/ncov-ingest/actions/workflows/nextclade-full-run-gisaid.yml)
 - [GenBank full Nextclade run](https://github.com/nextstrain/ncov-ingest/actions/workflows/nextclade-full-run-genbank.yml)

They will simply run the `./bin/run-nextclade-full-aws --database=<name of the database>` and will announce the beginning of the job and the AWS Batch Job ID on Nextstrain Slack. 

For that, go to the GitHub Actions UI using one of the links above, click the button "Run workflow", choosing "branch: master" from the list and confirming.

If needed, the runs can be also launched from a local machine, by one of these scripts, depending on whether you want to run the computation locally, in docker, or to schedule an AWS Batch Job (the latter is what GitHub Actions do):

```bash
./bin/run-nextclade-full           # Runs locally (requires significant computational resources)
./bin/run-nextclade-full-aws       # Runs in docker (requires significant computational resources)
./bin/run-nextclade-full-docker    # Schedules an AWS Batch Job and runs there
```

In case of AWS Batch option, the results of the computation, the new `nextclade.tsv` will be uploaded to S3 into a subdirectory in the directory which is the usual location of this file for the database. The subdirectory name will contain a date, so that there is no confusion about versions. The Slack announcement will contain the full path. These files then need to be manually inspected for correctness and scientific soundness and id all good, copied to the usual location where the daily ingest can find them. From that point the clades are considered fresh.

For detailed explanation see PR [#218](https://github.com/nextstrain/ncov-ingest/pull/218).


## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_TOKEN`
* `SLACK_CHANNELS`
