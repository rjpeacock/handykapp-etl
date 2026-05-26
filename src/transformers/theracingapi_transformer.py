# To allow running as a script
import sys
from pathlib import Path

from helpers import horse_name_to_pre_mongo_horse

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
from horsetalk import (  # type: ignore
    CoatColour,
    Country,
    Gender,
    Headgear,
    HorseAge,
    RaceClass,
    RaceDistance,
    RaceGrade,
    Sex,
)

from models import (
    PreMongoRace,
    PreMongoRunner,
    TheRacingApiRacecard,
    TheRacingApiRunner,
)


def build_datetime(date_str: str, time_str: str) -> str:
    hour, minute = time_str.split(":")
    hour = str(h + 12 if (h := int(hour)) < 11 else h)
    return pendulum.from_format(
        f"{date_str} {hour}:{minute}", "YYYY-MM-DD HH:mm"
    ).isoformat()


def transform_horse(
    runner: TheRacingApiRunner, race_date: pendulum.DateTime = pendulum.now("UTC")
) -> PreMongoRunner:
    data = petl.fromdicts([runner.model_dump()])

    transformed_horse = (
        petl.rename(
            data,
            {
                "horse": "name",
                "region": "country",
                "number": "saddlecloth",
                "lbs": "lbs_carried",
                "ofr": "official_rating",
            },
        )
        .addfield(
            "year",
            lambda rec: HorseAge(
                rec["age"],
                context_date=race_date,
                hemisphere=Country[rec["country"]].hemisphere,  # type: ignore[attr-defined]
            )._official_dob.year,
            index=3,
        )
        .addfield(
            "allowance",
            lambda rec: int(rec["jockey"].split("(")[1].split(")")[0])
            if "(" in rec["jockey"]
            else 0,
        )
        .addfield(
            "gelded_from",
            lambda rec: (race_date - pendulum.duration(days=1)).date
            if (x := Gender[rec["sex"]]) == Sex.MALE and x.has_testes  # type: ignore[misc]
            else None,
        )
        .addfield(
            "non_runner",
            lambda rec: rec["saddlecloth"] == "NR",
        )
        .convert(
            {
                "name": lambda x: x.upper(),
                "sex": lambda x: Gender[x].sex.name[0],  # type: ignore
                "age": int,
                "colour": lambda x: CoatColour[x].name.title(),  # type: ignore
                "sire": lambda x: horse_name_to_pre_mongo_horse(x, sex="M"),
                "damsire": lambda x: horse_name_to_pre_mongo_horse(x, sex="M"),
                "saddlecloth": lambda x: int(x) if x != "NR" else None,
                "draw": lambda x: int(x) if x != "" else None,
                "lbs_carried": lambda x: int(x) if x != "" else None,
                "headgear": lambda x: Headgear[x].name.title() if x else None,  # type: ignore
                "official_rating": int,
                "jockey": lambda x: x.split("(")[0].strip(),
                "trainer": lambda x: x.split(", ")[0].strip(),
            }
        )
        .convert(
            "dam",
            lambda x, rec: horse_name_to_pre_mongo_horse(
                x, sex="F", sire=rec["damsire"]
            ),
            pass_row=True,
        )
        .cutout("sex_code", "last_run", "form", "age")
        .dicts()[0]
    )
    return PreMongoRunner(**transformed_horse)


def transform_races(record: TheRacingApiRacecard) -> list[PreMongoRace]:
    data = petl.fromdicts([record.model_dump()])
    transformed_races = (
        petl.rename(
            data,
            {
                "race_name": "title",
                "age_band": "age_restriction",
                "rating_band": "rating_restriction",
                "going": "going_description",
                "pattern": "race_grade",
                "distance_f": "distance_description",
            },
        )
        .addfield(
            "is_handicap",
            lambda rec: "HANDICAP" in rec["title"].upper()
            or "H'CAP" in rec["title"].upper(),
            # getattr(
            #     RaceTitle.parse(rec["title"])["race_designation"], "name", None
            # )
            # == "HANDICAP",
            index=4,
        )
        .addfield(
            "datetime",
            lambda rec: build_datetime(rec["date"], rec["off_time"]),
            index=1,
        )
        .addfield(
            "obstacle",
            # TODO: Improve in horsetalk
            lambda rec: {
                "Hurdle": "Hurdle",
                "Chase": "Steeplechase",
            }.get(rec["race_type"]),
            index=5,
        )
        .addfield(
            "code",
            lambda rec: "National Hunt"
            if rec["race_type"] in ("NH Flat", "Hurdle", "Chase")
            else "Flat",
            index=6,
        )
        .convert(
            {
                "course": lambda x: x.replace(" (AW)", ""),
                "prize": lambda x: x.replace(",", ""),
                "race_grade": lambda x: str(RaceGrade(x)) if x else None,
                "race_class": lambda x: int(RaceClass(x)),
                "age_restriction": lambda x: x or None,
                "rating_restriction": lambda x: x or None,
                "distance_description": lambda x: str(
                    RaceDistance(f"{int(float(x) // 1)}f {int((float(x) % 1) * 220)}y")
                ),
            }
        )
        .addfield(
            "runners",
            lambda rec: [
                transform_horse(
                    TheRacingApiRunner(**h),
                    pendulum.parse(rec["datetime"]),  # type: ignore[arg-type]
                )
                for h in rec["runners"]
            ],
        )
        .cutout("field_size", "region", "race_type", "date", "off_time")
        .dicts()
    )
    return [PreMongoRace(**race) for race in transformed_races]


if __name__ == "__main__":
    print("Cannot run theracingapi_transformer.py as a script.")
