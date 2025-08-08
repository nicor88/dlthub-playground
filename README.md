# dlthub-playground
A playground repository for dltHub (data load tool)

## Requirements
* uv - follow [these instructions](https://docs.astral.sh/uv/getting-started/installation/) for installation

## Getting started
1. Be sure to have uv installed
2. Run `make init`
3. Be sure to put all relevant environment variable inside `.env` file
    * for `TOKEN` generate a PAT token in Databricks interface. Going to Settings (Top Right) -> Developer -> Access Token


## Databricks destination
* Create a destination target schema e.g. `CREATE SCHEMA IF NOT EXISTS your_catalog.dlt_playground_nico;`
* Create a volume e.g. `CREATE VOLUME IF NOT EXISTS your_catalog.dlt_playground_nico.dlt_volume;`

## Sources

### Jira
Run the following command to initialize the Jira source `dlt init jira duckdb`.
After that, a `jira` folder will be created. Then we copy in the `sources` directory.

## Handling secrets
This repository uses environment variables to handle secrets.
You can set them in the `.env` file, or export them in your terminal session, or alternatively we use `dotenv` to load them from the `.env` file in a python context.

Destinations secrets are set prefixing them by `DESTINATION__DATABRICKS__CREDENTIALS__`.
While for sources secrets we prefix them by the source e.g. in for Jira API token we set `JIRA_API_TOKEN` in the `.env` file, because we use
as argument `jira_api_token` in the source functions.
