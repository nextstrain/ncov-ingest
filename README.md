# nCoV Ingestion Pipeline

## Required dependencies
Install the required dependencies using the exported `environment.yml` file.

## Required environment variables
* `GISAID_API_ENDPOINT`
* `GISAID_USERNAME_AND_PASSWORD`
* `AWS_DEFAULT_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `SLACK_INCOMING_WEBHOOK`

## Running manually
1. run `./bin/fetch-data`
2. run `python ./bin/transform-data.py {ncov json data}`

## Running automatically
The fetch and transform pipeline exists as a GitHub workflow at `.github/workflows/fetch-and-transform.yml`.
It is scheduled tdo run every 15 minutes.
