from datetime import datetime
from functools import cache, wraps
from typing import Literal

import pendulum
from peak_utility.listish import compact
from pymongo import MongoClient

from models import PreMongoHorse, PreMongoRaceCourseDetails

mongo_client = MongoClient("mongodb://localhost:27017/", timeoutMS=120000)  # type: ignore


db = mongo_client.handykapp


def cache_if_found(maxsize=None):
    def decorator(func):
        cache = {}

        @wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            if maxsize and len(cache) > maxsize:
                for _ in range(1000):
                    cache.pop(next(iter(cache)))
            return result

        return wrapper

    return decorator


def create_apostrophe_regex(name: str) -> str:
    name_regex = name.replace("'", "'?")
    if "'" not in name:
        name_regex = "".join(
            char + "'?" if char.isalpha() else char for char in name_regex
        )
    return name_regex


def update_horse_name_if_needed(horse: PreMongoHorse, result: dict) -> None:
    if "'" in horse.name and "'" not in result["name"]:
        db.horses.update_one({"_id": result["_id"]}, {"$set": {"name": horse.name}})
        result["name"] = horse.name


@cache_if_found(maxsize=50000)
def get_horse(horse: PreMongoHorse) -> dict | None:
    search = db.horses.find_one
    base = {"name": horse.name, "country": horse.country, "year": horse.year}

    result = search(base)
    if result:
        return result

    result = search(compact(base) | {"name": horse.name, "sex": horse.sex})
    if result:
        return result

    if "'" in horse.name:
        result = search(
            base
            | {
                "name": {
                    "$regex": f"^{create_apostrophe_regex(horse.name)}$",
                    "$options": "i",
                }
            }
        )

        if result:
            update_horse_name_if_needed(horse, result)
            return result

    return None


type NewmarketRacecourse = Literal["Newmarket July", "Newmarket Rowley"]


def apply_newmarket_workaround(date: pendulum.DateTime) -> NewmarketRacecourse:
    return "Newmarket July" if date.month in (6, 7, 8) else "Newmarket Rowley"


@cache
def rr_code_to_course_dict() -> dict:
    source = db.racecourses.find(
        projection={"_id": 1, "surface": 1, "references.racing_research": 1}
    )
    return {
        (
            racecourse["references"]["racing_research"],
            racecourse["surface"] if racecourse["surface"] == "Turf" else "AW",
        ): racecourse["_id"]
        for racecourse in source
        if racecourse["surface"]
        != "Sand"  # Can remove this once Going.SAND available in horsetalk
    }


@cache
def get_all_racecourses():
    return list(
        db.racecourses.find(
            {},
            {
                "name": 1,
                "formal_name": 1,
                "surface": 1,
                "code": 1,
                "obstacle": 1,
                "references": 1,
            },
        )
    )


@cache
def get_racecourse_id(
    race: PreMongoRaceCourseDetails, datetime: datetime, source: str
) -> str | None:
    if source == "racing_research":
        return rr_code_to_course_dict().get((race.course, race.surface))

    racecourses = get_all_racecourses()
    surface_options = (
        ["Tapeta", "Polytrack"]
        if race.surface == "AW" or race.surface == "All Weather"
        else [race.surface]
        if race.surface
        else ["Tapeta", "Polytrack", "Sand", "Turf"]
    )
    course_name = race.course.lower().replace("(", "").replace(")", "").strip()
    if course_name == "newmarket":
        course_name = apply_newmarket_workaround(
            pendulum.parse(str(datetime))  # type: ignore[arg-type]
        ).lower()

    for racecourse in racecourses:
        if (
            (
                racecourse["name"].lower() == course_name
                or racecourse["formal_name"].lower() == course_name
            )
            and racecourse.get("surface") in surface_options
            and racecourse.get("code") == race.code
            and (
                racecourse.get("obstacle") == race.obstacle
                or (racecourse.get("obstacle") is None and race.obstacle is None)
            )
        ):
            return racecourse["_id"]

    return None
