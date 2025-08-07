import os

import dlt
from dlt.destinations import databricks
from dlt.sources.rest_api import rest_api_source
from dotenv import load_dotenv

load_dotenv()
DATABRICKS_SCHEMA_NAME = os.environ["DATABRICKS_SCHEMA_NAME"]
DATABRICKS_VOLUME_NAME = os.environ["DATABRICKS_VOLUME_NAME"]

bricks = databricks(staging_volume_name=DATABRICKS_VOLUME_NAME)
pokemon_source = rest_api_source(
    {
        "client": {"base_url": "https://pokeapi.co/api/v2/"},
        "resources": [
            {
                "name": "pokemon",
                "write_disposition": "merge",
                "primary_key": "name",
                "endpoint": {"path": "pokemon", "params": {"limit": 1000}},
            },
        ],
    }
)

pipeline = dlt.pipeline(
    pipeline_name="rest_pokemon_ingestion",
    dataset_name=DATABRICKS_SCHEMA_NAME,
    destination=bricks,
)

load_info = pipeline.run(pokemon_source)
print(load_info)
print(pipeline.dataset().pokemon.df())
