import pathlib
import re

import pendulum
import petl
import tomllib
from prefect import flow, task
from pybet import Odds

from clients import SpacesClient
from helpers import log_validation_problem
from models.betfair_price_record import BetfairPriceRecord
from models.mongo_betfair_horserace_bet_history import MongoBetfairHorseraceBetHistory
from models.mongo_betfair_horserace_pnl import MongoBetfairHorseracePnl

with pathlib.Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["betfair"]["spaces_dir"]

MARKET = "\ufeffMarket"
START_TIME = "Start time"
SETTLED_DATE = "Settled date"
PROFIT_LOSS = "Profit/Loss (£)"
STAKE = "Stake (£)"
SELECTION = " Selection"
BID_TYPE = " Bid type"
AVG_ODDS = " Avg. odds matched"


@task(tags=["Betfair"])
def get_csv(date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(SpacesClient.get_files(SOURCE))
        if "PandL" in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None


@task(tags=["Betfair"])
def read_csv(csv):
    source = petl.MemorySource(SpacesClient.stream_file(csv))
    return petl.fromcsv(source, encoding="utf-8")


def get_places_from_place_detail(place_detail: str) -> int | None:
    if "TBP" in place_detail:
        found_places = re.search(r"\d+", place_detail)
        return int(found_places.group()) if found_places else None
    return None


@task(tags=["Betfair"])
def transform_betfair_bet_history(
    data: petl.Table,
) -> list[MongoBetfairHorseraceBetHistory]:
    transformed_data = (
        petl.rename(
            data,
            {
                MARKET: "market",
                SELECTION: "horse",
                BID_TYPE: "bet_type",
                AVG_ODDS: "odds",
                STAKE: "stake",
                PROFIT_LOSS: "profit_loss",
            },
        )
        .convert("odds", lambda x: Odds(x))
        .convert("stake", float)
        .convert("profit_loss", lambda x: float(x.replace("(", "-").replace(")", "")))
        .addfield("sport", lambda rec: rec["market"].split("/")[0].strip())
        .addfield("market_detail", lambda rec: rec["market"].split("/")[1].strip())
        .addfield(
            "racecourse_and_datetime",
            lambda rec: rec["market_detail"].split(" : ")[0].strip(),
        )
        .addfield(
            "place_detail", lambda rec: rec["market_detail"].split(" : ")[1].strip()
        )
        .addfield(
            "racecourse",
            lambda rec: re.split(r" (?=\d)", rec["racecourse_and_datetime"])[0].strip(),
        )
        .select(lambda rec: rec["sport"] == "Horse Racing")
        .addfield(
            "is_place_market",
            lambda rec: "TBP" in rec["place_detail"] or "Placed" in rec["place_detail"],
        )
        .addfield(
            "places",
            lambda rec: get_places_from_place_detail(rec["place_detail"])
            if rec["is_place_market"]
            else 1,
        )
        .addfield(
            "race_description",
            lambda rec: rec["place_detail"] if not rec["is_place_market"] else None,
        )
        .cutout(
            "market",
            "market_detail",
            "racecourse_and_datetime",
            "place_detail",
            "is_place_market",
        )
        .dicts()
    )

    filled_out_data = []
    for rec in transformed_data:
        if not rec["race_description"]:
            matching_description = next(
                (
                    other_rec["race_description"]
                    for other_rec in transformed_data
                    if other_rec["race_datetime"] == rec["race_datetime"]
                    and other_rec["race_description"]
                ),
                None,
            )
            rec["race_description"] = matching_description
        filled_out_data.append(rec)

    return [MongoBetfairHorseraceBetHistory(**rec) for rec in filled_out_data]


@task(tags=["Betfair"])
def validate_betfair_bet_history(data: petl.Table) -> bool:
    header = (MARKET, SELECTION, BID_TYPE, AVG_ODDS, STAKE, PROFIT_LOSS)

    validate_betfair_date = lambda x: re.match(
        r"\d{2}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}", x
    )

    constraints = [
        {"name": "market_str", "field": MARKET, "assertion": str},
        {"name": "horse_str", "field": SELECTION, "assertion": str},
        {
            "name": "bid_type_str",
            "field": BID_TYPE,
            "assertion": lambda x: x in ("BACK", "LAY"),
        },
        {"name": "odds_float", "field": AVG_ODDS, "assertion": float},
        {"name": "stake_float", "field": STAKE, "assertion": float},
        {
            "name": "start_time_valid",
            "field": START_TIME,
            "assertion": validate_betfair_date,
        },
        {
            "name": "pnl_float",
            "field": PROFIT_LOSS,
            "assertion": lambda x: x == "0.00" or float(x),
        },
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["Betfair"])
def transform_betfair_pnl_data(data: petl.Table) -> list[MongoBetfairHorseracePnl]:
    transformed_data = (
        petl.rename(
            data,
            {
                MARKET: "market",
                START_TIME: "race_datetime",
                SETTLED_DATE: "settled_date",
                PROFIT_LOSS: "profit_loss",
            },
        )
        .convert("profit_loss", float)
        .addfield("sport", lambda rec: rec["market"].split("/")[0].strip())
        .addfield("market_detail", lambda rec: rec["market"].split("/")[1].strip())
        .addfield(
            "racecourse_and_datetime",
            lambda rec: rec["market_detail"].split(" : ")[0].strip(),
        )
        .addfield(
            "place_detail", lambda rec: rec["market_detail"].split(" : ")[1].strip()
        )
        .addfield(
            "racecourse",
            lambda rec: re.split(r" (?=\d)", rec["racecourse_and_datetime"])[0].strip(),
        )
        .select(lambda rec: rec["sport"] == "Horse Racing")
        .addfield(
            "is_place_market",
            lambda rec: "TBP" in rec["place_detail"] or "Placed" in rec["place_detail"],
        )
        .addfield(
            "places",
            lambda rec: get_places_from_place_detail(rec["place_detail"])
            if rec["is_place_market"]
            else 1,
        )
        .addfield(
            "race_description",
            lambda rec: rec["place_detail"] if not rec["is_place_market"] else None,
        )
        .convert(
            "race_datetime",
            lambda x: pendulum.from_format(x, "DD-MMM-YY HH:mm", tz="UTC"),
        )
        .cutout(
            "market",
            "market_detail",
            "racecourse_and_datetime",
            "place_detail",
            "is_place_market",
            "settled_date",
            "sport",
        )
        .dicts()
    )

    filled_out_data = []
    for rec in transformed_data:
        if not rec["race_description"]:
            matching_description = next(
                (
                    other_rec["race_description"]
                    for other_rec in transformed_data
                    if other_rec["race_datetime"] == rec["race_datetime"]
                    and other_rec["race_description"]
                ),
                None,
            )
            rec["race_description"] = matching_description
        filled_out_data.append(rec)

    return [MongoBetfairHorseracePnl(**rec) for rec in filled_out_data]


@task(tags=["Betfair"])
def validate_betfair_pnl_data(data: petl.Table) -> bool:
    header = (MARKET, START_TIME, SETTLED_DATE, PROFIT_LOSS)

    validate_betfair_date = lambda x: re.match(
        r"\d{2}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}", x
    )

    constraints = [
        {"name": "market_str", "field": MARKET, "assertion": str},
        {
            "name": "start_time_valid",
            "field": START_TIME,
            "assertion": validate_betfair_date,
        },
        {
            "name": "settled_date_valid",
            "field": SETTLED_DATE,
            "assertion": validate_betfair_date,
        },
        {
            "name": "pnl_float",
            "field": PROFIT_LOSS,
            "assertion": lambda x: x == "0.00" or float(x),
        },
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


DECIMAL_FIELDS = {
    "BSP", "PPWAP", "MORNINGWAP", "PPMAX", "PPMIN",
    "IPMAX", "IPMIN", "MORNINGTRADEDVOL", "PPTRADEDVOL", "IPTRADEDVOL",
}


def betfair_price_transformer(row: dict) -> BetfairPriceRecord:
    for key in DECIMAL_FIELDS:
        if row.get(key) == "":
            row[key] = None
    return BetfairPriceRecord.model_validate(row)


@flow
def betfair_pnl_transformer():
    csv = get_csv()
    data = read_csv(csv)
    problems = validate_betfair_pnl_data(data)
    for problem in problems.dicts():
        log_validation_problem(problem)
    return transform_betfair_pnl_data(data)


if __name__ == "__main__":
    data = betfair_pnl_transformer()  # type: ignore
    print(data)
