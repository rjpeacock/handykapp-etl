# To allow running as a script
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from pendulum import Date
from prefect import flow, get_run_logger

from clients import SpacesClient
from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from models import TheRacingApiRacecard
from processors.record_processor import record_processor
from transformers.theracingapi_transformer import transform_races

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def increment_theracingapi_data():
    logger = get_run_logger()
    logger.info("Querying database for most recent race")
    races = list(db.races.find().sort("datetime", -1))
    logger.info(f"{len(races)} races found")
    if races:
        most_recent = races[-1]["datetime"]
        logger.info(f"Most recent race on db is: {pendulum.parse(most_recent)}")
        load_theracingapi_data(from_date=pendulum.parse(most_recent))
    else:
        logger.info("No races currently in db")
        load_theracingapi_data()


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def load_theracingapi_data(*, from_date: Date | None = None):
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    r = record_processor()
    next(r)
    record_count = 0
    for file in SpacesClient.get_files(f"{SOURCE}racecards"):
        if from_date:
            file_date = pendulum.parse(file.split(".")[0][-8:]).date()  # type: ignore[union-attr]
            if file_date < from_date:
                continue

        logger.info(f"Reading {file}")
        contents = SpacesClient.read_file(file)
        for dec in contents["racecards"]:
            data = {k: v for k, v in dec.items() if k != "off_dt"}
            try:
                record = TheRacingApiRacecard(**data)
                r.send((record, transform_races, file, "theracingapi"))
                record_count += 1
                # Add small sleep every 50 races to prevent CPU saturation
                if record_count % 50 == 0:
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Unable to process Racing API racecard {file}: {e}")

    r.close()


if __name__ == "__main__":
    increment_theracingapi_data()
