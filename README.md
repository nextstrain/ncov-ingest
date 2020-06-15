# nCoV Ingestion Pipeline

Internal tooling for the Nextstrain team to ingest and curate SARS-CoV-2 genome sequences. This is open source, but we are not intending to support this to be used by outside groups.

## Running locally
If you're using Pipenv (see below), then run commands from `./bin/…` inside a `pipenv shell` or wrapped with `pipenv run ./bin/…`.

1. Run `./bin/fetch-from-gisaid > data/gisaid.ndjson`
2. Run `./bin/transform-gisaid data/gisaid.ndjson`
3. Look at `data/gisaid/sequences.fasta` and `data/gisaid/metadata.tsv`

## Running automatically
The ingest pipeline exists as the GitHub workflows `.github/workflows/ingest-master.yml` and `…/ingest-branch.yml`.
It is scheduled to run four times a day, on pushes to `master` that modify `source-data/annotations.tsv`, and on pushes to other branches.
Pushes to branches other than `master` upload files to branch-specific paths in the S3 bucket, don't send notifications, and don't trigger Nextstrain rebuilds, so that they don't interfere with the production data.

The current schedule as of 10 June 2020 is:

| UTC         | Seattle     | Basel       |
| ----------- | ----------- | ----------- |
| 06:00:00+00 | 23:00:00-07 | 08:00:00+02 |
| 12:00:00+00 | 05:00:00-07 | 14:00:00+02 |
| 15:00:00+00 | 08:00:00-07 | 17:00:00+02 |
| 21:00:00+00 | 14:00:00-07 | 23:00:00+02 |

(time zone math courtesy of [this PostgreSQL query](https://gist.github.com/tsibley/2237b487ad022b3fdf62937fa94cf216))

AWS credentials are stored in this repository's secrets and are associated with the `nextstrain-ncov-ingest-uploader` IAM user in the Bedford Lab AWS account, which is locked down to reading and publishing only the `gisaid.ndjson`, `metadata.tsv`, and `sequences.fasta` files and their zipped equivalents in the `nextstrain-ncov-private` S3 bucket.

## Manually triggering the automation
You can manually trigger the full automation by running `./bin/trigger ingest --user <your-github-username>`.
If you want to only trigger a rebuild of [nextstrain/ncov](https://github.com/nextstrain/ncov) without re-ingesting data from GISAID first, run `./bin/trigger rebuild --user <your-github-username>`.
See the output of `./bin/trigger ingest` or `./bin/trigger rebuild` for more information about authentication with GitHub.

## Updating manual annotations
Manual annotations should be added to `source-data/gisaid_annotations.tsv`.
A common pattern is expected to be:

 1. Run <https://github.com/nextstrain/ncov>.
 2. Discover metadata that needs fixing.
 3. Update `source-data/gisaid_annotations.tsv`.
 4. Push changes to `master` and re-download `gisaid/metadata.tsv`.

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
