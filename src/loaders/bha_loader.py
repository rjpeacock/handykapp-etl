# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import datetime

import pendulum
import petl
import tomllib
from peak_utility.number import Numbertext
from prefect import flow, get_run_logger, task

from clients import SpacesClient
from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from helpers.loads_tracker import get_last_load, update_load
from models.bha_ratings_record import BHARatingsRecord
from processors.ratings_processor import ratings_processor
from transformers.bha_transformer import transform_ratings

db = client.handykapp
SOURCE_NAME = "bha"

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)
SOURCE = settings["bha"]["spaces_dir"]  # Directory where BHA CSV files are stored


@task(tags=["BHA"], task_run_name="get_{date}_{csv_type}_csv")
def get_csv(csv_type="ratings", date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(SpacesClient.get_files(SOURCE))
        if csv_type in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None


@task(tags=["BHA"])
def read_csv(csv):
    source = petl.MemorySource(SpacesClient.stream_file(csv))
    return petl.fromcsv(source)


def convert_header_to_field_name(header: str) -> str:
    result = header.strip().lower().replace(" ", "_")
    for digit in "123456789":
        if digit in result:
            result = result.replace(digit, str(Numbertext(digit)))
    return result


def csv_row_to_dict(header_row, data_row):
    return dict(zip(header_row, data_row))


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def load_bha_data():
    logger = get_run_logger()
    logger.info("Starting BHA loader")

    last_load = get_last_load(db, SOURCE_NAME)
    last_processed = last_load.last_processed if last_load else None

    csv = get_csv()
    logger.info(f"Got CSV file: {csv}")

    if csv is None:
        logger.info("No BHA CSV file found")
        update_load(db, SOURCE_NAME, last_processed, 0, "skipped")
        return

    if last_processed == csv:
        logger.info(f"No new BHA file since {last_processed}, skipping")
        update_load(db, SOURCE_NAME, last_processed, 0, "skipped")
        return

    if last_processed:
        logger.info(f"Resuming from last processed file: {last_processed}")

    r = ratings_processor()
    next(r)

    data = read_csv(csv)
    date_str = csv.split("_")[-1].split(".")[0]  # Remove file extension
    pendulum_date = pendulum.from_format(date_str, "YYYYMMDD")
    date = datetime.date(pendulum_date.year, pendulum_date.month, pendulum_date.day)

    rows = list(data)
    logger.info(f"Total rows in CSV: {len(rows)}")

    header = [convert_header_to_field_name(col) for col in rows[0]]
    record_count = 0

    for data_row in rows[1:]:
        row_dict = csv_row_to_dict(header, data_row)
        try:
            record = BHARatingsRecord(**row_dict, date=date)
        except (ValueError, TypeError) as e:
            logger.error(f"Unable to create BHA record from row: {e}")
            continue

        try:
            transformed_ratings = transform_ratings(record)
            r.send(transformed_ratings)
            record_count += 1
        except Exception as e:
            logger.error(f"Unable to process BHA ratings for {record.name}: {e}")

    r.close()

    if record_count > 0:
        update_load(db, SOURCE_NAME, csv, record_count, "success")
        logger.info(f"Loaded {record_count} records from {csv}")
    else:
        update_load(db, SOURCE_NAME, csv, 0, "success")
        logger.info(f"Processed {csv} with 0 new records")


if __name__ == "__main__":
    load_bha_data()
