# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pathlib import Path

import pendulum
import tomllib
from prefect import flow, task
from pymongo import ASCENDING as ASC, DESCENDING as DESC

from clients import mongo_client as client
from helpers.alert_handlers import failure_handler

# Load settings
with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

from .bha_loader import load_bha_data
from .formdata_loader import load_formdata
from .racecourse_loader import load_racecourses
from .rapid_horseracing_loader import load_rapid_horseracing_entries
from .theracingapi_loader import load_theracingapi_data

db = client.handykapp


@task
def drop_database():
    client.drop_database("handykapp")


@task
def spec_database():
    db.formdata.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
    )
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)],
        unique=True,
        partialFilterExpression={
            "$and": [{"country": {"$type": ["string"]}}, {"year": {"$type": ["int"]}}]
        },
    )
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC), ("sex", ASC)], unique=True
    )
    db.horses.create_index("name")
    db.horses.create_index("sire")
    db.horses.create_index("dam")
    db.people.create_index("last")
    db.people.create_index([("last", ASC), ("first", ASC)])
    db.people.create_index(
        [("last", ASC), ("first", ASC), ("middle", ASC)], unique=True
    )
    db.people.create_index(
        "references.racing_research",
        unique=True,
        partialFilterExpression={"references.racing_research": {"$exists": True}},
    )
    db.racecourses.create_index(
        [("name", ASC), ("country", ASC), ("obstacle", ASC), ("surface", ASC)],
        unique=True,
    )
    db.racecourses.create_index("name")
    db.races.create_index([("racecourse", ASC), ("datetime", ASC)], unique=True)
    db.races.create_index("runners.horse")
    db.races.create_index([("datetime", DESC)])
    db.loads.create_index("source", unique=True)
    db.people.create_index({"references": 1}, wildcard=True)


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def nuclear_reload():
    drop_database()
    spec_database()
    load_racecourses()
    switch_date = pendulum.parse(settings["app"]["switch_date"]).date()
    load_rapid_horseracing_entries(until_date=switch_date)
    load_theracingapi_data()
    load_bha_data()
    load_formdata()


if __name__ == "__main__":
    nuclear_reload()  # type: ignore
