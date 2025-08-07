import os
from typing import Iterable, List

import dlt
from dlt.common.typing import TDataItem
from dlt.destinations import databricks
from dotenv import load_dotenv

from sources.jira import get_paginated_data

load_dotenv()

DATABRICKS_SCHEMA_NAME = os.environ["DATABRICKS_SCHEMA_NAME"]
DATABRICKS_VOLUME_NAME = os.environ["DATABRICKS_VOLUME_NAME"]

jira_queries = ["project = 'DATAENG' AND (created >= -15d OR updated >= -15d)"]

bricks = databricks(staging_volume_name=DATABRICKS_VOLUME_NAME)


@dlt.resource(name="jira_issues", write_disposition="replace", max_table_nesting=0)
def jira_issues(
    jql_queries: List[str],
    jira_subdomain: str = dlt.secrets.value,
    jira_email: str = dlt.secrets.value,
    jira_api_token: str = dlt.secrets.value,
) -> Iterable[TDataItem]:
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
            subdomain=jira_subdomain,
            email=jira_email,
            api_token=jira_api_token,
            page_size=100,
            data_path="issues",
        )


pipeline = dlt.pipeline(
    pipeline_name="jira_ingestion",
    dataset_name=DATABRICKS_SCHEMA_NAME,
    destination=bricks,
)

pipeline.run(
    jira_issues(
        jql_queries=jira_queries,
    )
)
