# To allow running as a script
import logging
import operator
import re
import sys
from pathlib import Path

from helpers import horse_name_to_pre_mongo_horse

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
from horsetalk import (
    Country,
    Going,
    Horse,
    HorseAge,
    Horselength,
    RaceWeight,
    Surface,
)
from peak_utility.names.corrections import eirify, scotify

from models import PreMongoEntry, PreMongoRace, PreMongoRunner, RapidRecord, RapidRunner
from transformers.parsers import (
    parse_code,
    parse_obstacle,
)


def infer_surface(going_description: str) -> str:
    try:
        going = (
            next(iter(Going.multiparse(going_description).values()))
            if "COURSE" in going_description.upper()
            else Going(going_description)
        )
        return going.surface.name.title().replace("_", " ")
    except Exception:
        logging.warning(f"Failed to parse going description: {going_description}")
        return "Turf"  # Default to turf if parsing fails


def standardise_name(name: str) -> str:
    if re.match(r"^non[-_\s]?runner", name, re.IGNORECASE):
        return ""

    aggressive = False

    if ", " in name:
        name, country = name.rsplit(", ", 1)
        if country.lower() == "ireland":
            aggressive = True
        # TODO: Persist country to the person document

    if name.upper().startswith("IRELAND"):
        aggressive = True
        name = name.upper().replace("IRELAND", "").title()

    return eirify(scotify(name), aggressive=aggressive)


def transform_horse(
    runner: RapidRunner,
    race_date: pendulum.DateTime = pendulum.now("UTC"),
    finishing_time: str | None = None,
) -> PreMongoRunner:
    data = petl.fromdicts([runner.model_dump()])
    transformed_horse = (
        petl.rename(
            data,
            {
                "id_horse": "rapid_id",
                "weight": "lbs_carried",
                "last_ran_days_ago": "days_since_prev_run",
                "number": "saddlecloth",
                "OR": "official_rating",
                "distance_beaten": "beaten_distance",
                "position": "finishing_position",
            },
        )
        .convert(
            {
                "age": int,
                "days_since_prev_run": int,
                "official_rating": int,
                "non_runner": lambda x: bool(int(x)),
                "lbs_carried": lambda x: RaceWeight(x).lb,
                "sp": lambda x: x or None,
                "sire": lambda x: horse_name_to_pre_mongo_horse(
                    x, sex="M", default_country="GB"
                ),
                "dam": lambda x: horse_name_to_pre_mongo_horse(
                    x, sex="F", default_country="GB"
                ),
                "beaten_distance": lambda x: float(Horselength(x)) if x else None,
                "jockey": lambda x: standardise_name(x),
                "trainer": lambda x: standardise_name(x),
            }
        )
        .addfield(
            "country", lambda rec: (Horse(rec["horse"]).country or Country.GB).name
        )
        .addfield("name", lambda rec: Horse(rec["horse"]).name.upper())
        .addfield(
            "year",
            lambda rec: (
                HorseAge(
                    rec["age"],
                    context_date=race_date,
                    hemisphere=Country[rec["country"]].hemisphere,  # type: ignore[attr-defined]
                )._official_dob.year
            ),
        )
        .addfield(
            "finishing_time",
            lambda rec: finishing_time if rec["finishing_position"] == 1 else None,
        )
        .addfield("official_position", operator.itemgetter("finishing_position"))
        .cutout("horse", "age")
        .dicts()[0]
    )
    return PreMongoRunner(**transformed_horse)


def transform_results(record: RapidRecord) -> list[PreMongoRace]:
    data = petl.fromdicts([record.model_dump()])
    transformed_races = (
        petl.rename(
            data,
            {
                "id_race": "rapid_id",
                "date": "datetime",
                "age": "age_restriction",
                "canceled": "cancelled",
                "distance": "distance_description",
                "going": "going_description",
            },
        )
        .convert(
            {
                "datetime": lambda x: pendulum.from_format(
                    x, "YYYY-MM-DD HH:mm:ss"
                ).isoformat(),
                "finished": lambda x: bool(int(x)),
                "cancelled": lambda x: bool(int(x)),
                "race_class": lambda x: x or None,
            }
        )
        .addfield(
            "is_handicap",
            lambda rec: (
                "HANDICAP" in rec["title"].upper() or "H'CAP" in rec["title"].upper()
            ),
            index=4,
        )
        .addfield("obstacle", lambda rec: parse_obstacle(rec["title"]))
        .addfield(
            "surface",
            lambda rec: (
                infer_surface(rec["going_description"])
                if rec["going_description"]
                else None
            ),
        )
        .addfield("code", lambda rec: parse_code(rec["obstacle"], rec["title"]))
        .addfield(
            "runners",
            lambda rec: [
                transform_horse(
                    RapidRunner(**h),
                    pendulum.parse(rec["datetime"]),  # type: ignore[arg-type]
                    finishing_time=rec["finish_time"],
                )
                for h in rec["horses"]
            ],
        )
        .cutout("horses", "finish_time")
        .dicts()
    )
    return [PreMongoRace(**race) for race in transformed_races]


def transform_to_entries(record: RapidRecord) -> list[PreMongoRace]:
    base = transform_results(record)

    entry_fields = set(PreMongoEntry.model_fields.keys())

    return [
        PreMongoRace(
            **{
                **race.model_dump(exclude={"runners"}),
                "runners": [
                    PreMongoRunner(
                        **{
                            k: v
                            for k, v in runner.model_dump().items()
                            if k in entry_fields
                        }
                    )
                    for runner in race.runners
                ],
            }
        )
        for race in base
    ]


if __name__ == "__main__":
    print("Cannot run rapid_horseracing_transformer.py as a script.")
