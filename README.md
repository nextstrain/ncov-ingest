# nCoV Ingestion Pipeline

Internal tooling for the Nextstrain team to ingest and curate SARS-CoV-2 genome sequences. The pipeline is open source, but we are not intending to support it for use by outside groups.
Relies on data from <https://simplemaps.com/data/us-cities>.

Outputs documented here are part of `ncov-ingest`'s public API: https://docs.nextstrain.org/projects/ncov/en/latest/reference/remote_inputs.html

## Running locally

> NOTE: The full set of sequences from GISAID/GenBank will most likely require more compute resources than what is available on your local computer.

To debug all rules on a subset of the data, you can use the `config/debug_sample_genbank.yaml` and `config/debug_sample_gisaid.yaml` config files.
These will download raw data from AWS s3, randomly keeping only a subset of lines of the input files (configurable in the config file).
This way, the pipeline completes in a matter of minutes and acceptable storage requirements for local compute.
However, the output data should not be trusted, as biosample and cog-uk input lines are randomly selected independently of the main ndjson.

To get started, you can run the following:

```sh
snakemake -j all --configfile config/debug_sample_genbank.yaml  -pF --ri --nt
```

> **Warning**
> If you are running the pipeline without a Nextclade cache, it will do a full Nextclade run that aligns _all_ sequences,
> which will take significant time and resources!

Follow these instructions to run the ncov-ingest pipeline _without_ all the bells and whistles used by internal Nextstrain runs that involve AWS S3, Slack notifications, and GitHub Action triggers:

### GISAID

To pull sequences directly from GISAID, you are required to set two environment variables:

- `GISAID_API_ENDPOINT`
- `GISAID_USERNAME_AND_PASSWORD`

Make these environment variables available to the Docker runner environment by creating an `env.d/` directory:

```sh
./bin/write-envdir env.d \
  GISAID_API_ENDPOINT \
  GISAID_USERNAME_AND_PASSWORD
```

Then run the ncov-ingest pipeline with the nextstrain CLI:

```sh
nextstrain build \
  --image nextstrain/ncov-ingest \
  --exec env \
  . \
    envdir env.d snakemake \
      --configfile config/local_gisaid.yaml
```

### GenBank

Sequences can be pulled from GenBank without any environment variables.
Run the ncov-ingest pipeline with the nextstrain CLI:

```sh
nextstrain build \
  --image nextstrain/ncov-ingest \
  . \
  --configfile config/local_genbank.yaml \
```

## Running automatically

The ingest pipelines are triggered from the GitHub workflows `.github/workflows/ingest-master-*.yml` and `…/ingest-branch-*.yml` but run on AWS Batch via the `nextstrain build --aws-batch` infrastructure.
They're run on pushes to `master` that modify `source-data/*-annotations.tsv` and on pushes to other branches.
Pushes to branches other than `master` upload files to branch-specific paths in the S3 bucket, don't send notifications, and don't trigger Nextstrain rebuilds, so that they don't interfere with the production data.

AWS credentials are stored in this repository's secrets and are associated with the `nextstrain-ncov-ingest-uploader` IAM user in the Bedford Lab AWS account, which is locked down to reading and publishing only the `gisaid.ndjson`, `metadata.tsv`, and `sequences.fasta` files and their zipped equivalents in the `nextstrain-ncov-private` S3 bucket.

## Manually triggering the automation

A full run is now done in 3 steps via manual triggers:

1. Fetch new sequences and ingest them by running `./bin/trigger ncov-ingest gisaid/fetch-and-ingest --user <your-github-username>`.
2. Add manual annotations, update location hierarchy as needed, and run ingest without fetching new sequences.
    - Pushes of `source-data/*-annotations.tsv` to the master branch will automatically trigger a run of ingest.
    - You can also run ingest manually by running `./bin/trigger ncov-ingest gisaid/ingest --user <your-github-username>`.
3. Once all manual fixes are complete, trigger a rebuild of [nextstrain/ncov](https://github.com/nextstrain/ncov) by running `./bin/trigger ncov gisaid/rebuild --user <your-github-username>`.

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
To view your Slack member ID, open up the Slack menu by clicking your name at the top, and then click on 'View profile'.
Then, click on 'More'.
You can then copy your Slack member ID from the menu that appears.
Enter this into the `slack_member_id` field of your alert configuration.

## Rerunning Nextclade ignoring cache after Nextclade dataset is updated

Clade assignments and other QC metadata output by Nextclade are currently cached in `nextclade.tsv` in the S3 bucket and only incremental additions for the new sequences are performed during the daily ingests.
Whenever the underlying nextclade dataset (reference tree, QC rules) and/or nextclade software are updated, it is necessary to perform a full update of `nextclade.tsv`, rerunning for all of the GISAID and GenBank sequences all over again, to account for changes in the data and Nextclade algorithms.

In order to tell ingest to not use the cached `nextclade.tsv`/`aligned.fasta` and instead perform a full rerun, you need to add an (empty) touchfile to the s3 bucket:

```bash
aws s3 cp - s3://nextstrain-ncov-private/nextclade.tsv.zst.renew < /dev/null
aws s3 cp - s3://nextstrain-data/files/ncov/open/nextclade.tsv.zst.renew < /dev/null
```

Ingest will automatically remove the touchfiles after it has completed the rerun.

To rerun Nextclade using the `sars-cov-2-21L` dataset - which is only necessary when the calculation of `immune_escape` and `ace2_binding` changes - you need to add an (empty) touchfile to the s3 bucket:

```bash
aws s3 cp - s3://nextstrain-ncov-private/nextclade_21L.tsv.zst.renew < /dev/null
aws s3 cp - s3://nextstrain-data/files/ncov/open/nextclade_21L.tsv.zst.renew < /dev/null
```

## Required environment variables

- `GISAID_API_ENDPOINT`
- `GISAID_USERNAME_AND_PASSWORD`
- `AWS_DEFAULT_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `SLACK_TOKEN`
- `SLACK_CHANNELS`
