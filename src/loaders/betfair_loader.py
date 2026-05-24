# To allow running as a script
import csv
import pathlib
import sys
from pathlib import Path

import tomllib

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
from prefect import flow, get_run_logger

from clients import mongo_client as client
from helpers.alert_handlers import failure_handler
from helpers.helpers import fetch_content
from helpers.loads_tracker import get_last_load, update_load
from processors.betfair_processor import betfair_pnl_processor, betfair_price_processor
from transformers.betfair_transformer import (
    betfair_pnl_transformer,
    betfair_price_transformer,
)

db = client.handykapp
SOURCE_NAME = "betfair_prices"

with pathlib.Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

BASE_URL = settings["betfair_prices"]["source_url"]
DEFAULT_START = pendulum.from_format(
    settings["betfair_prices"]["start_date"], "YYYY-MM-DD"
).date()
FILENAME_PREFIX = "dwbfprices"


JUMP_INDICATORS = {"HRD", "CHS", "XC", "NHF"}


def is_flat_race(event_name: str) -> bool:
    return not any(i in event_name.upper() for i in JUMP_INDICATORS)


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
def load_betfair_prices(
    countries: list[str] = ["uk", "ire"],
    market_types: list[str] = ["win", "place"],
    start_date: pendulum.Date = DEFAULT_START,
    end_date: pendulum.Date | None = None,
):
    logger = get_run_logger()
    logger.info("Starting betfair price loader")

    last_load = get_last_load(db, SOURCE_NAME)
    if last_load and last_load.last_processed:
        start_date = pendulum.from_format(
            last_load.last_processed, "YYYY-MM-DD"
        ).date().add(days=1)

    end = end_date or pendulum.today().date()

    if start_date > end:
        last_processed = last_load.last_processed if last_load else None
        logger.info(f"Betfair prices up to date (last processed {last_processed})")
        update_load(db, SOURCE_NAME, last_processed, 0, "skipped")
        return

    bf = betfair_price_processor()
    next(bf)

    flat_cache: dict[tuple[str, str], bool] = {}

    for day in range((end - start_date).days + 1):
        d = start_date.add(days=day)
        for country in countries:
            for market_type in market_types:
                url = generate_url(country, market_type, d)
                try:
                    content = fetch_content(url)
                except Exception:
                    continue
                reader = csv.DictReader(content.decode("utf-8").splitlines())
                records = []
                for row in reader:
                    row = {k.upper(): v for k, v in row.items()}
                    if market_type == "win":
                        flat = is_flat_race(row["EVENT_NAME"])
                        flat_cache[row["MENU_HINT"], row["EVENT_DT"]] = flat
                        if flat:
                            records.append(betfair_price_transformer(row))
                    elif flat_cache.get((row["MENU_HINT"], row["EVENT_DT"])):
                        records.append(betfair_price_transformer(row))
                for record in records:
                    record.country = country
                    record.market_type = market_type
                    bf.send(record)
        bf.send(None)
        update_load(db, SOURCE_NAME, d.format("YYYY-MM-DD"), 0, "running")

    bf.close()
    update_load(db, SOURCE_NAME, end.format("YYYY-MM-DD"), 0, "success")


if __name__ == "__main__":
    load_betfair_horserace_pnl()
