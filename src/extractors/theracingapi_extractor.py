# To allow running as a script
import re
import sys
from pathlib import Path

import pendulum

sys.path.append(str(Path(__file__).resolve().parent.parent))

import tomllib
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

from clients import SpacesClient
from helpers import fetch_content
from helpers.alert_handlers import failure_handler

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["source_dir"]
DESTINATION = settings["theracingapi"]["spaces_dir"]


def get_headers():
    return {
        "x-rapidapi-host": "the-racing-api1.p.rapidapi.com",
        "x-rapidapi-key": Secret.load("rapid-api-key").get(),
    }


@task(tags=["TheRacingApi"])
def extract_countries():
    source = f"{SOURCE}courses/regions"
    headers = get_headers()

    return fetch_content(source, headers=headers)


@task(tags=["TheRacingApi"])
def extract_racecards(day="tomorrow", region_codes=["gb", "ire"]):
    source = f"{SOURCE}racecards/free"
    headers = get_headers()
    params = {"day": day, "region_codes": region_codes}

    content = fetch_content(source, params=params, headers=headers)
    date_str = pendulum.now().add(days=1).format("YYYYMMDD")
    filename = f"{DESTINATION}racecards/theracingapi_racecards_{date_str}.json"
    SpacesClient.write_file(content, filename)


@task(tags=["TheRacingApi"])
def identify_missing_dates():
    logger = get_run_logger()
    files = list(SpacesClient.get_files(f"{DESTINATION}racecards"))

    present = set()
    for f in files:
        if m := re.search(r"_(\d{8})\.json$", f):
            present.add(m.group(1))

    if not present:
        logger.info("No racecard files found in Spaces")
        return

    start = pendulum.parse(min(present)).date()
    end = pendulum.parse(max(present)).date()

    missing = []
    current_date = start
    while current_date <= end:
        date_str = current_date.format("YYYYMMDD")
        if date_str not in present:
            missing.append(date_str)
        current_date = current_date.add(days=1)

    if missing:
        SpacesClient.write_file(
            "\n".join(missing) + "\n",
            f"{DESTINATION}missing_racecard_dates.txt",
        )
        logger.info(f"Found {len(missing)} missing racecard dates")
    else:
        SpacesClient.delete_file(f"{DESTINATION}missing_racecard_dates.txt")
        logger.info("No missing dates in theracingapi")


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def theracingapi_racecards_extractor():
    extract_racecards()


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def theracingapi_countries_extractor():
    extract_countries()


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def check_missing_racecard_dates():
    identify_missing_dates()


if __name__ == "__main__":
    theracingapi_racecards_extractor()  # type: ignore
    theracingapi_countries_extractor()  # type: ignore
