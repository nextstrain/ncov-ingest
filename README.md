# nCoV Ingestion Pipeline

## Running locally
1. Run `./bin/fetch-from-gisaid > data/gisaid.ndjson`
2. Run `./bin/transform data/gisaid.ndjson`
3. Look at `data/sequences.fasta` and `data/metadata.tsv`

## Running automatically
The fetch and transform pipeline exists as a GitHub workflow at `.github/workflows/fetch-and-transform.yml`.
It is scheduled to run every 15 minutes and on pushes to `master`.

AWS credentials are stored in this repository's secrets and are associated with the `nextstrain-ncov-ingest-uploader` IAM user in the Bedford Lab AWS account, which is locked down to reading and publishing only the `gisaid.ndjson`, `metadata.tsv`, and `sequences.fasta` files in the `nextstrain-ncov-private` S3 bucket.

## Updating manual annotations
Manual annotations should be added to `source-data/annotations.tsv`.
A common pattern is expected to be:

 1. Run <https://github.com/nextstrain/ncov>.
 2. Discover metadata that needs fixing.
 3. Update `source-data/annotations.tsv`.
 4. Push changes to `master` and re-download `metadata.tsv`.

## Required dependencies
Install the required dependencies using the exported `environment.yml` file.

## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_INCOMING_WEBHOOK`
