import os
import random

import dlt
from dlt.destinations import databricks
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

DATABRICKS_SCHEMA_NAME = os.environ["DATABRICKS_SCHEMA_NAME"]
DATABRICKS_VOLUME_NAME = os.environ["DATABRICKS_VOLUME_NAME"]

fake = Faker()

bricks = databricks(staging_volume_name=DATABRICKS_VOLUME_NAME)


def generate_event():
    return {
        "event_id": fake.uuid4(),
        "title": fake.catch_phrase(),
        "description": fake.text(max_nb_chars=100),
        "start_time": fake.date_time_this_year().isoformat(),
        "end_time": fake.date_time_this_year().isoformat(),
        "location": {
            "venue": fake.company(),
            "address": fake.address(),
            "coordinates": {"lat": fake.latitude(), "lng": fake.longitude()},
        },
        "organizer": {
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
        },
        "participants": [
            {
                "name": fake.name(),
                "email": fake.email(),
                "rsvp": random.choice(["Yes", "No", "Maybe"]),
            }
            for _ in range(random.randint(2, 5))
        ],
        "tags": fake.words(nb=random.randint(2, 5)),
        "is_online": random.choice([True, False]),
    }


@dlt.resource(name="events", write_disposition="append", max_table_nesting=2)
def generate_events():
    for _ in range(10):
        yield generate_event()


pipeline = dlt.pipeline(
    pipeline_name="python_source_ingestion",
    dataset_name=DATABRICKS_SCHEMA_NAME,
    destination=bricks,
)

load_info = pipeline.run(generate_events)
print(load_info)
