# To allow running as a script
import csv
import pathlib
import sys
from pathlib import Path

import tomllib

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
from prefect import flow, get_run_logger

from helpers.alert_handlers import failure_handler
from helpers.helpers import fetch_content
from processors.betfair_processor import betfair_pnl_processor, betfair_price_processor
from transformers.betfair_transformer import (
    betfair_pnl_transformer,
    betfair_price_transformer,
)

with pathlib.Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

BASE_URL = settings["betfair_prices"]["source_url"]
DEFAULT_START = pendulum.from_format(
    settings["betfair_prices"]["start_date"], "YYYY-MM-DD"
).date()
FILENAME_PREFIX = "dwbfprices"


def generate_url(country: str, market_type: str, date: pendulum.Date) -> str:
    suffix = date.format("DDMMYYYY")
    return f"{BASE_URL}/{FILENAME_PREFIX}{country}{market_type}{suffix}.csv"


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def load_betfair_horserace_pnl():
    logger = get_run_logger()
    logger.info("Starting betfair loader")

    bf = betfair_pnl_processor()
    next(bf)

    for bf_pnl_line in betfair_pnl_transformer():
        bf.send(bf_pnl_line)

    bf.close()


@flow(on_failure=[lambda flow, flow_run, state: failure_handler("Flow", flow.name, state)])
def load_betfair_horserace_prices(
    countries: list[str] = ["uk", "ire"],
    market_types: list[str] = ["win", "place"],
    start_date: pendulum.Date = DEFAULT_START,
    end_date: pendulum.Date | None = None,
):
    logger = get_run_logger()
    logger.info("Starting betfair price loader")
    end = end_date or pendulum.today().date()

    bf = betfair_price_processor()
    next(bf)

    for day in range((end - start_date).days + 1):
        d = start_date.add(days=day)
        for country in countries:
            for mtype in market_types:
                url = generate_url(country, mtype, d)
                try:
                    content = fetch_content(url)
                except Exception:
                    continue
                reader = csv.DictReader(content.decode("utf-8").splitlines())
                records = [betfair_price_transformer(row) for row in reader]
                for record in records:
                    bf.send(record)

    bf.close()


if __name__ == "__main__":
    load_betfair_horserace_pnl()
