# nCoV Ingestion Pipeline

Internal tooling for the Nextstrain team to ingest and curate SARS-CoV-2 genome sequences. This is open source, but we are not intending to support this to be used by outside groups.
Relies on data from https://simplemaps.com/data/us-cities.

## Running locally
If you're using Pipenv (see below), then run commands from `./bin/…` inside a `pipenv shell` or wrapped with `pipenv run ./bin/…`.

1. Run `./bin/fetch-from-gisaid data/gisaid.ndjson`
2. Run `./bin/transform-gisaid data/gisaid.ndjson`
3. Look at `data/gisaid/sequences.fasta` and `data/gisaid/metadata.tsv`

## Running automatically
The ingest pipelines are triggered from the GitHub workflows `.github/workflows/ingest-master-*.yml` and `…/ingest-branch-*.yml` but run on AWS Batch via the `nextstrain build --aws-batch` infrastructure.
They're run on pushes to `master` that modify `source-data/*-annotations.tsv` and on pushes to other branches.
Pushes to branches other than `master` upload files to branch-specific paths in the S3 bucket, don't send notifications, and don't trigger Nextstrain rebuilds, so that they don't interfere with the production data.

AWS credentials are stored in this repository's secrets and are associated with the `nextstrain-ncov-ingest-uploader` IAM user in the Bedford Lab AWS account, which is locked down to reading and publishing only the `gisaid.ndjson`, `metadata.tsv`, and `sequences.fasta` files and their zipped equivalents in the `nextstrain-ncov-private` S3 bucket.

## Manually triggering the automation
A full run is a now done in 3 steps via manual triggers:
1. Fetch new sequences and ingest them by running `./bin/trigger ncov-ingest gisaid/fetch-and-ingest --user <your-github-username>`.
2. Add manual annotations, update location hierarchy as needed, and run ingest without fetching new sequences.
    * Pushes of `source-data/*-annotations.tsv` to the master branch will automatically trigger a run of ingest.
    * You can also run ingest manually by running `./bin/trigger ncov-ingest gisaid/ingest --user <your-github-username>`.
3. Once all manual fixes are complete, trigger a rebuild of [nextstrain/ncov](https://github.com/nextstrain/ncov) by running `./bin/trigger ncov-ingest rebuild --user <your-github-username>`.

See the output of `./bin/trigger ncov-ingest gisaid/fetch-and-ingest --user <your-github-username>`, `./bin/trigger ncov-ingest gisaid/ingest` or `./bin/trigger ncov-ingest rebuild` for more information about authentication with GitHub.

Note: running `./bin/trigger ncov-ingest` posts a GitHub `repository_dispatch`.
Regardless of which branch you are on, it will trigger the specified action on the master branch.

Valid dispatch types for `./bin/trigger ncov-ingest` are:

  - `ingest` (both GISAID and GenBank)
  - `gisaid/ingest`
  - `genbank/ingest`
  - `fetch-and-ingest` (both GISAID and GenBank)
  - `gisaid/fetch-and-ingest`
  - `genbank/fetch-and-ingest`
  - `rebuild`
  - `gisaid/rebuild`
  - `open/rebuild`
  - `genbank/rebuild`
  - `nextclade-full-run` (both GISAID and GenBank)
  - `gisaid/nextclade-full-run`
  - `genbank/nextclade-full-run`

## Updating manual annotations
Manual annotations should be added to `source-data/gisaid_annotations.tsv`.
A common pattern is expected to be:

 1. Run <https://github.com/nextstrain/ncov>.
 2. Discover metadata that needs fixing.
 3. Update `source-data/gisaid_annotations.tsv`.
 4. Push changes to `master` and re-download `gisaid/metadata.tsv`.


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

## Nextclade full run after Nextclade dataset is updated

Clade assignments and other QC metadata output by Nextclade are currently cached in `nextclade.tsv` in the S3 bucket and only incremental additions for the new sequences are performed during the daily ingests.
Whenever the underlying nextclade dataset (reference tree, QC rules) and/or nextclade software are updated, it is necessary to perform a full update of `nextclade.tsv`, rerunning for all of the GISAID and GenBank sequences all over again, to account for changes in the data and in Nextclade algorithms.

The most convenient option is to trigger it through the corresponding GitHub Action:

* [GISAID full Nextclade run](https://github.com/nextstrain/ncov-ingest/actions/workflows/nextclade-full-run-gisaid.yml)
* [GenBank full Nextclade run](https://github.com/nextstrain/ncov-ingest/actions/workflows/nextclade-full-run-genbank.yml)

They will simply run `./bin/run-nextclade-full-aws --database=<name of the database>` and will announce the beginning of the job and the AWS Batch Job ID on Nextstrain Slack.

For that, go to the GitHub Actions UI using one of the links above, click the button "Run workflow", choosing "branch: master" from the list and confirm.

If needed, the runs can be also launched from a local machine, by one of these scripts, depending on whether you want to run the computation locally, in docker, or to schedule an AWS Batch Job (the latter is what GitHub Actions do):

```bash
./bin/run-nextclade-full           # Runs locally (requires significant computational resources)
./bin/run-nextclade-full-aws       # Runs in docker (requires significant computational resources)
./bin/run-nextclade-full-docker    # Schedules an AWS Batch Job and runs there
```

The resulting nextclade.tsv.gz should be then available in the subdirectory nextclade-full-run in the usual S3 location for the particular database:

```bash
aws s3 ls s3://nextstrain-data/files/ncov/open/nextclade-full-run
aws s3 ls s3://nextstrain-ncov-private/nextclade-full-run
```

Copy the output files to your local machine by using the path returned by `aws s3 ls` or using tab completion from `aws s3 cp s3://nextstrain-data/files/ncov/open/nextclade-full-run`:

```bash
# enter aws s3 cp s3://nextstrain-data/files/ncov/open/nextclade-full-run and use tab completion for exact date
aws s3 cp s3://nextstrain-data/files/ncov/open/nextclade-full-run-2021-11-19--02-34-23--UTC/nextclade.tsv.gz open.tsv.gz
aws s3 cp s3://nextstrain-ncov-private/nextclade-full-run-2021-11-18--23-37-14--UTC/nextclade.tsv.gz private.tsv.gz
```

The nextclade.tsv.gz should be manually inspected for scientific correctness, comparing to the old one, if necessary. Check that clades aren't broken by running for example:

```bash
gzcat open.tsv.gz | tsv-summarize -H --group-by clade --count | sort
gzcat private.tsv.gz | tsv-summarize -H --group-by clade --count | sort
```

If all went well, after agreeing with ingest team, the nextclade.tsv.gz should then be copied to the location where the daily ingest can find it, overwriting the old one. These locations are:

```txt
s3://nextstrain-ncov-private/nextclade.tsv.gz
s3://nextstrain-data/files/ncov/open/nextclade.tsv.gz
```

(just omit the subdirectory nextclade-full-run)

You can use the following commands:

```bash
aws s3 cp private.tsv.gz s3://nextstrain-ncov-private/nextclade.tsv.gz
aws s3 cp open.tsv.gz s3://nextstrain-data/files/ncov/open/nextclade.tsv.gz
```

For detailed explanation see PR [#218](https://github.com/nextstrain/ncov-ingest/pull/218).

## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_TOKEN`
* `SLACK_CHANNELS`
