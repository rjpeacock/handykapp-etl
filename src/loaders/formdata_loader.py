# To allow running as a script
import sys
from pathlib import Path

from horsetalk import RacingCode

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from prefect import flow, get_run_logger

from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from processors.formdata_processors import file_processor
from transformers.formdata_transformer import get_formdatas

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]

db = client.handykapp


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def load_formdata():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    db.formdata.drop()
    logger.info("Dropped formdata collection")

    f = file_processor()
    next(f)

    files = get_formdatas(code=RacingCode.FLAT, after_year=22, for_refresh=True)
    for file in files:
        f.send(file)

    f.close()
    logger.info("Loaded formdata collection")


if __name__ == "__main__":
    load_formdata()  # type: ignore
