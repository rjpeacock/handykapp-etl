# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger

from extractors.rapid_horseracing_extractor import (
    replace_missing_theracingapi_racecards,  # type: ignore  # TEMP: replaces rapid_horseracing_extractor
)
from extractors.theracingapi_extractor import (
    theracingapi_racecards_extractor,  # type: ignore
)


@flow
def load_nothing_and_log():
    logger = get_run_logger()
    logger.info("I've run, but done nothing.")


@flow
def source_data_runner():
    load_nothing_and_log()  # type: ignore
    theracingapi_racecards_extractor()  # type: ignore
    replace_missing_theracingapi_racecards()  # type: ignore


if __name__ == "__main__":
    source_data_runner()  # type: ignore
