import dlt
from dlt.destinations import databricks
from dlt.sources.rest_api import rest_api_source
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
pokemon_source = rest_api_source(
    {
        "client": {"base_url": "https://pokeapi.co/api/v2/"},
        "resources": [
            {
                "name": "pokemon",
                "write_disposition": "merge",
                "primary_key": "name",
                "endpoint": {
                    "path": "pokemon",
                    "params": {"limit": 1000}
                },
            },
        ]
    }
)

pipeline = dlt.pipeline(
    pipeline_name="rest_pokemon_ingestion",
    dataset_name=config.get('DATABRICKS_SCHEMA_NAME'),
    destination=bricks,
)

load_info = pipeline.run(pokemon_source)
print(load_info)
print(pipeline.dataset().pokemon.df())
