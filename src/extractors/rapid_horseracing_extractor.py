# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
from time import sleep

import pendulum
import tomllib
from prefect import flow, task
from prefect.blocks.system import Secret

from clients import SpacesClient
from helpers import fetch_content
from helpers.alert_handlers import failure_handler

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

# NB: In RapidAPI, results are called race details

SOURCE = settings["rapid_horseracing"]["source_dir"]
DESTINATION = settings["rapid_horseracing"]["spaces_dir"]
RACECARDS_DESTINATION = f"{DESTINATION}racecards/"
RESULTS_DESTINATION = f"{DESTINATION}results/"
LIMITS = settings["rapid_horseracing"]["limits"]


def get_file_date(filename):
    return filename.split(".")[0][-8:]


def get_headers(url):
    return {
        "x-rapidapi-host": url.split("//")[1].split("/")[0],
        "x-rapidapi-key": Secret.load("rapid-api-key").get(),
    }


@task(tags=["Rapid"])
def get_unfetched_race_ids(last_checked):
    racecard_files = list(SpacesClient.get_files(RACECARDS_DESTINATION, last_checked))
    return [
        race["id_race"]
        for file in racecard_files
        for race in SpacesClient.read_file(file)
    ]


@task(tags=["Rapid"], task_run_name="extract_result_{race_id}")
def extract_result(race_id):
    source = f"{SOURCE}race/{race_id}"
    headers = get_headers(source)

    content = fetch_content(source, headers=headers)
    filename = f"{RESULTS_DESTINATION}rapid_api_result_{race_id}.json"
    SpacesClient.write_file(content, filename)


@task(tags=["Rapid"], task_run_name="extract_racecards_{date}")
def extract_racecards(date):  # date - YYYY-MM-DD
    source = f"{SOURCE}racecards"
    params = {"date": date}
    headers = get_headers(source)

    content = fetch_content(source, params, headers)
    date_str = date.replace("-", "")
    filename = f"{RACECARDS_DESTINATION}rapid_api_racecards_{date_str}.json"
    SpacesClient.write_file(content, filename)


@task(tags=["Rapid"])
def get_next_racecard_date():
    start_date = pendulum.parse("2020-01-01")
    end_date = pendulum.now()
    test_date = start_date

    files = list(SpacesClient.get_files(RACECARDS_DESTINATION))
    file_date_strs = [get_file_date(filename) for filename in files]
    file_dates = [pendulum.from_format(x, "YYYYMMDD") for x in file_date_strs]

    while test_date <= end_date:
        if test_date not in file_dates:
            return test_date.format("YYYY-MM-DD")
        test_date += pendulum.duration(days=1)

    return None


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def update_results_to_do_list():
    filename = f"{DESTINATION}results_to_do_list.json"
    current_status = SpacesClient.read_file(filename)
    last_checked = (
        pendulum.parse(current_status["last_checked"])
        if current_status["last_checked"]
        else None
    )

    result_files = list(SpacesClient.get_files(RESULTS_DESTINATION))
    new_race_ids = get_unfetched_race_ids(last_checked)
    done_race_ids = [filename.split(".")[0].split("_")[-1] for filename in result_files]
    to_do_race_ids = current_status["results_to_do"] + new_race_ids

    content = json.dumps(
        {
            "last_checked": str(pendulum.now()),
            "results_to_do": [
                race_id for race_id in to_do_race_ids if race_id not in done_race_ids
            ],
            "results_done": done_race_ids,
        }
    )
    SpacesClient.write_file(content, filename)


@flow(on_failure=[lambda flow, state: failure_handler("Flow", flow.name, state)])
def rapid_horseracing_extractor():
    # Add another day"s racing to the racecards folder
    date = get_next_racecard_date()
    extract_racecards(date)

    # Update the list of results to fetch
    update_results_to_do_list()

    # Fetch a number of results within the limits presented
    races_batch = SpacesClient.read_file(f"{DESTINATION}results_to_do_list.json")[
        "results_to_do"
    ][: LIMITS["day"] - 2]
    for race_id in races_batch:
        extract_result(race_id)
        sleep(90)


if __name__ == "__main__":
    rapid_horseracing_extractor()  # type: ignore
