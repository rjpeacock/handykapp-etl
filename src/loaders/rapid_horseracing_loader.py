# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from prefect import flow, get_run_logger

from clients import SpacesClient
from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from models import RapidRecord
from processors.record_processor import record_processor
from transformers.rapid_horseracing_transformer import (
    transform_results,
    transform_results_as_entries,
)

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]

db = client.handykapp


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def load_rapid_horseracing_entries(
    *, until_date: pendulum.Date = pendulum.now().date()
):
    logger = get_run_logger()
    logger.info("Starting rapid_horseracing entries loader")

    r = record_processor()
    next(r)

    source_location = f"{SOURCE}results"
    files = SpacesClient.get_files(source_location)
    logger.info(f"Processing files from {source_location}")

    for file in files:
        if "results_to_do_list.json" not in file:
            data = SpacesClient.read_file(file)
            try:
                record = RapidRecord(**data)
                if pendulum.parse(record.date).date() >= until_date:  # type: ignore[union-attr]
                    continue
                r.send((record, transform_results_as_entries, file, "rapid"))
            except Exception:
                logger.error(f"Unable to create a record from {file}")

    r.close()


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def load_rapid_horseracing_data():
    logger = get_run_logger()
    logger.info("Starting rapid_horseracing loader")

    r = record_processor()
    next(r)

    files = SpacesClient.get_files(f"{SOURCE}results")

    for file in files:
        if file != "results_to_do_list.json":
            data = SpacesClient.read_file(file)
            record = RapidRecord(**data)
            r.send((record, transform_results, file, "rapid"))

    r.close()


if __name__ == "__main__":
    load_rapid_horseracing_data()
