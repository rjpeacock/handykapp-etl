# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from prefect import flow

from helpers.alert_handlers import failure_handler

from .bha_loader import load_bha_data
from .rapid_horseracing_loader import load_rapid_horseracing_entries
from .theracingapi_loader import load_theracingapi_data

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def incremental_load():
    switch_date = pendulum.parse(settings["app"]["switch_date"]).date()
    load_rapid_horseracing_entries(until_date=switch_date)
    load_theracingapi_data()
    load_bha_data()


if __name__ == "__main__":
    incremental_load()  # type: ignore
