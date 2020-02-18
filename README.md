# nCoV Ingestion Pipeline

## Running locally
1. Run `./bin/fetch-data`
2. Run `./bin/transform-data s3://nextstrain-ncov-private/corona2020_fulldump.json`

## Running automatically
The fetch and transform pipeline exists as a GitHub workflow at `.github/workflows/fetch-and-transform.yml`.
It is scheduled to run every 15 minutes and on pushes to `master`.

## Updating manual annotations
Manual annotations should be added to `source-data/annotations.tsv`. A common pattern should be
running https://github.com/nextstrain/ncov, discovering metadata that needs fixing, updating
`annotations.tsv`, pushing this change to `master` and redownloading `metadata.tsv`.

## Required dependencies
Install the required dependencies using the exported `environment.yml` file.

## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_INCOMING_WEBHOOK`
