from typing import Iterable, List

import dlt
from dlt.common.typing import TDataItem
from dlt.destinations import databricks
from dotenv import dotenv_values

from sources.jira import get_paginated_data

config = dotenv_values(".env")

jira_queries = [
    "project = 'DATAENG' AND (created >= -15d OR updated >= -15d)"
]

bricks = databricks(
    credentials={
        "catalog": config.get('DATABRICKS_CATALOG_NAME'),
        "server_hostname": config.get('DATABRICKS_HOST'),
        "http_path": config.get('DATABRICKS_HTTP_PATH'),
        "access_token": config.get("DATABRICKS_TOKEN")},
    staging_volume_name=config.get('DATABRICKS_VOLUME_NAME')
)

@dlt.resource(name='jira_issues', write_disposition="replace", max_table_nesting=0)
def jira_issues(jql_queries: List[str], subdomain: str, email: str, api_token) -> Iterable[TDataItem]:
    api_path = "rest/api/3/search"

    for jql in jql_queries:
        params = {
            "fields": "*all",
            "expand": "fields,changelog,operations,transitions,names",
            "validateQuery": "strict",
            "jql": jql,
        }

        yield from get_paginated_data(
            api_path=api_path,
            params=params,
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=100,
            data_path="issues",
        )

pipeline = dlt.pipeline(
    pipeline_name="jira_ingestion",
    dataset_name=config.get('DATABRICKS_SCHEMA_NAME'),
    destination=bricks,
)

pipeline.run(
    jira_issues(
        jql_queries=jira_queries,
        subdomain=config.get('JIRA_SUBDOMAIN'),
        email=config.get('JIRA_EMAIL'),
        api_token=config.get("JIRA_API_TOKEN"),
    )
)
