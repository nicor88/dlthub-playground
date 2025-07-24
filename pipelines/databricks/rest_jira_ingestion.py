import dlt
from dlt.destinations import databricks
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import HttpBasicAuthConfig
from dotenv import dotenv_values

config = dotenv_values(".env")

bricks = databricks(
    credentials={
        "catalog": config.get('DATABRICKS_CATALOG_NAME'),
        "server_hostname": config.get('DATABRICKS_HOST'),
        "http_path": config.get('DATABRICKS_HTTP_PATH'),
        "access_token": config.get("DATABRICKS_TOKEN")},
    staging_volume_name=config.get('DATABRICKS_VOLUME_NAME')
)

rest_source = rest_api_source(
    {
        "client": {
            "base_url": f"https://{config.get('JIRA_SUBDOMAIN')}.atlassian.net/",
            "headers": {"Accept": "application/json"},
            "auth": HttpBasicAuthConfig(
                type="http_basic",
                username=config.get('JIRA_EMAIL'),
                password=config.get("JIRA_API_TOKEN")
            ),
            "paginator": OffsetPaginator(
                limit=50,
                offset=0,
                limit_param="maxResults",
                offset_param="startAt"
            ),

        },
        "resources": [
            {
                "name": "jira_issues_rest",
                "write_disposition": "merge",
                "primary_key": "id",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "rest/api/3/search",
                    "params": {
                        "fields": "*all",
                        "expand": "fields,changelog,operations,transitions,names",
                        "validateQuery": "strict",
                        "jql": "project = 'DATAENG' AND (created >= -20d OR updated >= -20d)",
                    },
                    "data_selector": "issues"
                },

            }
        ]
    }

)

pipeline = dlt.pipeline(
    pipeline_name="rest_jira_ingestion",
    dataset_name=config.get('DATABRICKS_SCHEMA_NAME'),
    destination=bricks,
)

load_info = pipeline.run(rest_source)
print(load_info)
print(pipeline.dataset().jira_issues_rest.df())
