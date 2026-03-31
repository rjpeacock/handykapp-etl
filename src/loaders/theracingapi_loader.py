# To allow running as a script
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from pendulum import Date, DateTime
from prefect import flow, get_run_logger

from clients import SpacesClient
from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from helpers.loads_tracker import get_last_load, update_load
from models import TheRacingApiRacecard
from processors.record_processor import record_processor
from transformers.theracingapi_transformer import transform_races

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp
SOURCE_NAME = "theracingapi"


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def increment_theracingapi_data():
    logger = get_run_logger()
    logger.info("Querying database for most recent race")
    races = list(db.races.find().sort("datetime", -1))
    logger.info(f"{len(races)} races found")
    if races:
        most_recent = races[-1]["datetime"]
        logger.info(f"Most recent race on db is: {pendulum.parse(most_recent)}")  # type: ignore[attr-defined]
        parsed: DateTime = pendulum.parse(most_recent)  # type: ignore[assignment]
        load_theracingapi_data(from_date=parsed.date())
    else:
        logger.info("No races currently in db")
        load_theracingapi_data()


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def load_theracingapi_data(*, from_date: Date | None = None):
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    last_load = get_last_load(db, SOURCE_NAME)
    if last_load is None:
        logger.info("No previous load found, loading all data")
    elif last_load.status == "skipped":
        logger.info("Previous load was skipped, loading all data")
    else:
        if last_load.last_processed:
            from_date = pendulum.from_format(last_load.last_processed, "YYYYMMDD").date()
            logger.info(f"Resuming from date: {from_date}")
        else:
            logger.info("No last processed date found, loading all data")

    r = record_processor()
    next(r)
    record_count = 0
    last_file_date = last_load.last_processed if last_load else None
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
                last_file_date = file.split(".")[0][-8:]
                # Add small sleep every 50 races to prevent CPU saturation
                if record_count % 50 == 0:
                    time.sleep(0.1)
            except Exception as e:
                logger.error(f"Unable to process Racing API racecard {file}: {e}")

    r.close()

    if record_count > 0:
        update_load(db, SOURCE_NAME, last_file_date, record_count, "success")
        logger.info(f"Loaded {record_count} records, last file: {last_file_date}")
    elif last_load is None or last_load.status != "skipped":
        update_load(db, SOURCE_NAME, None, 0, "skipped")
        logger.info("No new records to load")


if __name__ == "__main__":
    increment_theracingapi_data()
