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

## Refreshing clades

Clades assigned with Nextclade are currently cached in `nextclade.tsv` on S3 bucket and only incremental updates for new sequences are performed during the daily ingests. This clade cache goes stale with time. It is necessary to perform full update of `nextclade.tsv` file periodically, recomputing clades for all of the GISAID sequences all over again, to account for changes in the data. Same goes for when updating Nextclade versions, as they may lead to changes in clade assignment logic. Massive amounts of compute is required and it is not currently feasible to do this computation on current infrastructure, so it should be done elsewhere. As of November 2020, for 200k sequences, it takes approximately 2-3 hours on an on-prem Xeon machine with 16 cores/32 threads.

Use `./bin/gisaid-get-all-clades` to perform this update.
Python >= 3.6+, Node.js >= 12 (14 recommended) and yarn v1 are required.

```
git clone https://github.com/nextstrain/ncov-ingest
cd ncov-ingest
yarn install
pipenv sync
BATCH_SIZE=1000 pipenv run ./bin/gisaid-get-all-clades
```

The resulting `data/gisaid/nextclade.tsv` should be placed on S3 bucked, replacing the one produced by the last daily ingest:

```
./bin/upload-to-s3 data/gisaid/nextclade.tsv s3://nextstrain-ncov-private/nextclade.tsv.gz
```

It will be picked up by the next ingest.

The best time for the update is between daily builds. There is usually no rush, even if the globally recomputed `nextclade.tsv` is lagging behind one or two days, it will be incrementally updated by the next daily ingest.


## Required dependencies
Run `pipenv sync` to setup an isolated Python 3.7 environment using the pinned dependencies.

If you don't have Pipenv, [install it](https://pipenv.pypa.io/en/latest/install/#installing-pipenv) first with `brew install pipenv` or `python3 -m pip install pipenv`.

Node.js >= 12 and yarn v1 are required for the Nextclade part. Make sure you run `yarn install` to install Nextclade. A global installation should also work, however a specific version is required. (see `package.json`). Check [Nextclade CLI readme](https://github.com/nextstrain/nextclade/blob/master/packages/cli/README.md#getting-started) for more details.

## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_TOKEN`
* `SLACK_CHANNELS`
