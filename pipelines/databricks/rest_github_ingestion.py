import os

import dlt
from dlt.destinations import databricks
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import BearerTokenAuth
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
            "base_url": f"https://api.github.com/",
            "headers": {
                "User-Agent": "dltHub-Ingestion/1.0",
                "Accept": "application/vnd.github.v3+json"
            },
            "auth": BearerTokenAuth(token=config.get('GITHUB_TOKEN')),
        },
        "resources": [
            {
                "name": "github_repositories",
                "write_disposition": "merge",
                "primary_key": "name",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": f"orgs/{config.get('GITHUB_ORG')}/repos",
                },
            },
            {
                "name": "github_repositories_pull_requests",
                "write_disposition": "merge",
                "primary_key": "id",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": os.path.join("repos", config.get('GITHUB_ORG'), '{repository_name}', 'pulls'),
                    "params": {
                        "repository_name": {
                            # Use type "resolve" to define child endpoint which should be resolved
                            "type": "resolve",
                            # Parent endpoint resource name
                            "resource": "github_repositories",
                            # The specific field from the parent endpoint that needs to be used for iteration
                            "field": "name",
                        }
                    },
                },
                "include_from_parent": ["name"],
            }
        ]
    }
)

pipeline = dlt.pipeline(
    pipeline_name="github_repositories",
    dataset_name=config.get('DATABRICKS_SCHEMA_NAME'),
    destination=bricks,
)

load_info = pipeline.run(rest_source)
print(load_info)
