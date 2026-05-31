from collections.abc import Generator

from horsetalk import RaceDistance
from peak_utility.listish import compact
from prefect import get_run_logger

from clients import mongo_client as client
from clients.mongo_client import rr_code_to_course_dict
from models import FormdataRun, PreMongoPerson
from processors.person_processor import person_processor
from transformers.formdata_transformer import transform_run

db = client.handykapp


def _apply_result_to_race(found_race, horse, run, pp, logger):
    race_id = found_race["_id"]
    result = transform_run(run)
    going_assessment = result.pop("going_assessment")
    surface = result.pop("surface")
    jockey = result.pop("jockey")
    headgear = result.pop("headgear")
    update = {"$set": {**{f"runners.$.{k}": v for k, v in compact(result).items()}}}
    if "going_assessment" not in found_race:
        update["$set"]["going_assessment"] = going_assessment
        update["$set"]["surface"] = surface
    db.races.update_one(
        {"_id": race_id, "runners.horse": horse["_id"]},
        update,
    )
    if headgear:
        db.races.update_one(
            {
                "_id": race_id,
                "runners.horse": horse["_id"],
                "runners.headgear": {"$exists": False},
            },
            {"$set": {"runners.$.headgear": headgear}},
        )
    pp.send(
        (
            PreMongoPerson(
                name=jockey,
                role="jockey",
                race_id=race_id,
                runner_id=horse["_id"],
            ),
            "rr",
        )
    )
    logger.debug(
        f"Added result for {horse['_id']} in race at {run.course} on {run.date}"
    )


def _find_candidate_race(racecourse_id, run):
    target_distance = RaceDistance(f"{run.distance}f")

    possible = list(
        db.races.find(
            {
                "racecourse": racecourse_id,
                "$expr": {
                    "$eq": [
                        {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$datetime",
                            }
                        },
                        run.date,
                    ]
                },
            }
        )
    )

    matching = []
    for race in possible:
        try:
            if RaceDistance(race.get("distance_description", "")) == target_distance:
                matching.append(race)
        except Exception:
            continue

    return matching


def result_line_processor() -> Generator[None, tuple[dict, FormdataRun], None]:
    logger = get_run_logger()
    logger.info("Starting result line processor")

    pp = person_processor()
    next(pp)

    try:
        while True:
            horse, run = yield

            surface = "AW" if run.going.lower() == run.going else "Turf"
            racecourse_id = rr_code_to_course_dict().get((run.course, surface))

            if not racecourse_id:
                logger.warning(f"No racecourse found for {run.course} ({surface})")
                continue

            found_race = db.races.find_one(
                {
                    "racecourse": racecourse_id,
                    "$expr": {
                        "$eq": [
                            {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$datetime",
                                }
                            },
                            run.date,
                        ]
                    },
                    "runners.horse": horse["_id"],
                }
            )

            if not found_race:
                candidates = _find_candidate_race(racecourse_id, run)
                if len(candidates) != 1:
                    logger.debug(
                        f"No race found for {horse['_id']} at "
                        f"{run.course} on {run.date}"
                    )
                    continue
                race = candidates[0]
                if not any(
                    r.get("horse") == horse["_id"]
                    for r in race.get("runners", [])
                ):
                    db.races.update_one(
                        {"_id": race["_id"]},
                        {"$addToSet": {"runners": {"horse": horse["_id"]}}},
                    )
                found_race = db.races.find_one(
                    {"_id": race["_id"], "runners.horse": horse["_id"]},
                )

            _apply_result_to_race(found_race, horse, run, pp, logger)
    except GeneratorExit:
        pp.close()
        logger.info("Finished processing results.")
