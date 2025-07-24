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
Run the following command to initialize the Jira source `dlt init --source jira`.
After that, a `jira` folder will be created in the `sources` directory.