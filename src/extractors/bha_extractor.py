# To allow running as a script
import re
import sys
from html import unescape
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

BHA_PAGE_URL = "https://www.britishhorseracing.com/regulation/official-ratings/ratings-database/"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
}


@task(tags=["BHA"])
def get_signed_urls():
    content = fetch_content(BHA_PAGE_URL, headers=_HEADERS)
    text = unescape(content.decode() if isinstance(content, bytes) else content)

    pattern = re.escape(SOURCE) + r'[^"\'<>]+'
    matches = re.findall(pattern, text)

    result = {}
    for url in matches:
        if "performance-figures" in url:
            result["perf_figs"] = url
        elif "?diff" in url or "&diff=" in url:
            result["rating_changes"] = url
        else:
            result["ratings"] = url
    return result


@task(tags=["BHA"], task_run_name="fetch_bha_{file}")
def fetch(file, signed_urls):
    return fetch_content(signed_urls[file], headers=_HEADERS)


@task(tags=["BHA"], task_run_name="save_bha_{file}")
def save(file, content):
    filename = f"{DESTINATION}bha_{file}_{LAST_UPDATE_STR}.csv"
    SpacesClient.write_file(content, filename)


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def bha_extractor():
    signed_urls = get_signed_urls()
    for file in FILES:
        content = fetch(file, signed_urls)
        save(file, content)


if __name__ == "__main__":
    bha_extractor()  # type: ignore
