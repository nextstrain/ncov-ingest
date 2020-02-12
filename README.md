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

Steps:
1. run `./bin/fetch-data`
2. run `python ./bin/transform-data.py {ncov json data}`
