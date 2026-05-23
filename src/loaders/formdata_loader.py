# To allow running as a script
import sys
from pathlib import Path

from horsetalk import RacingCode

sys.path.append(str(Path(__file__).resolve().parent.parent))


import pendulum
import tomllib
from prefect import flow, get_run_logger

from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from helpers.loads_tracker import get_last_load, update_load
from processors.formdata_processors import file_processor
from transformers.formdata_transformer import get_formdata_date, get_formdatas

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]

db = client.handykapp
SOURCE_NAME = "formdata"


@flow(
    on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)]
)
def load_formdata():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    last_load = get_last_load(db, SOURCE_NAME)
    last_processed = last_load.last_processed if last_load else None
    files = get_formdatas(code=RacingCode.FLAT, after_year=20, for_refresh=True)

    if last_processed:
        last_date = pendulum.from_format(last_processed, "YYYY-MM-DD").date()
        files = [f for f in files if get_formdata_date(f) > last_date]

        if not files:
            logger.info("No new formdata files since last load")
            update_load(db, SOURCE_NAME, last_processed, 0, "skipped")
            return

        logger.info(f"Loading {len(files)} formdata files since {last_processed}")
    else:
        logger.info(f"Full formdata reload with {len(files)} files")

    f = file_processor()
    next(f)
    for file in files:
        f.send(file)
    f.close()

    last_file_date = get_formdata_date(files[-1]).format("YYYY-MM-DD")
    update_load(db, SOURCE_NAME, last_file_date, len(files), "success")
    logger.info("Loaded formdata collection")


if __name__ == "__main__":
    load_formdata()  # type: ignore
