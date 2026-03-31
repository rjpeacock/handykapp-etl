# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from prefect import flow, task

from clients import SpacesClient
from helpers import fetch_content, get_last_occurrence_of
from helpers.alert_handlers import failure_handler

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["bha"]["source_dir"]
FILES = settings["bha"]["files"]
DESTINATION = settings["bha"]["spaces_dir"]
UPDATE_DAY = pendulum.TUESDAY
LAST_UPDATE_STR = str(get_last_occurrence_of(UPDATE_DAY)).replace("-", "")


@task(tags=["BHA"], task_run_name="fetch_bha_{file}")
def fetch(file):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
    }
    return fetch_content(f"{SOURCE}{FILES[file]}", headers=headers)


@task(tags=["BHA"], task_run_name="save_bha_{file}")
def save(file, content):
    filename = f"{DESTINATION}bha_{file}_{LAST_UPDATE_STR}.csv"
    SpacesClient.write_file(content, filename)


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def bha_extractor():
    for file in FILES:
        content = fetch(file)
        save(file, content)


if __name__ == "__main__":
    bha_extractor()  # type: ignore
