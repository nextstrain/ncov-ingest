# nCoV Ingestion Pipeline

Internal tooling for the Nextstrain team to ingest and curate SARS-CoV-2 genome sequences. This is open source, but we are not intending to support this to be used by outside groups.

## Running locally
If you're using Pipenv (see below), then run commands from `./bin/…` inside a `pipenv shell` or wrapped with `pipenv run ./bin/…`.

1. Run `./bin/fetch-from-gisaid > data/gisaid.ndjson`
2. Run `./bin/transform data/gisaid.ndjson`
3. Look at `data/sequences.fasta` and `data/metadata.tsv`

## Running automatically
The fetch and transform pipeline exists as a GitHub workflow at `.github/workflows/fetch-and-transform.yml`.
It is scheduled to run every 15 minutes and on pushes to `master`.

AWS credentials are stored in this repository's secrets and are associated with the `nextstrain-ncov-ingest-uploader` IAM user in the Bedford Lab AWS account, which is locked down to reading and publishing only the `gisaid.ndjson`, `metadata.tsv`, and `sequences.fasta` files in the `nextstrain-ncov-private` S3 bucket.

## Manually triggering the automation
You can manually trigger the full automation by running `./bin/trigger ingest --user <your-github-username>`.
If you want to only trigger a rebuild of [nextstrain/ncov](https://github.com/nextstrain/ncov) without re-ingesting data from GISAID first, run `./bin/trigger rebuild --user <your-github-username>`.
See the output of `./bin/trigger ingest` or `./bin/trigger rebuild` for more information about authentication with GitHub.

## Updating manual annotations
Manual annotations should be added to `source-data/annotations.tsv`.
A common pattern is expected to be:

 1. Run <https://github.com/nextstrain/ncov>.
 2. Discover metadata that needs fixing.
 3. Update `source-data/annotations.tsv`.
 4. Push changes to `master` and re-download `metadata.tsv`.

## Required dependencies
Run `pipenv sync` to setup an isolated Python 3.6 environment using the pinned dependencies.

If you don't have Pipenv, [install it](https://pipenv.pypa.io/en/latest/install/#installing-pipenv) first with `brew install pipenv` or `python3 -m pip install pipenv`.

## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_TOKEN`
