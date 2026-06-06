import re
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
    result.pop("surface")
    jockey = result.pop("jockey")
    headgear = result.pop("headgear")
    update = {"$set": {**{f"runners.$.{k}": v for k, v in compact(result).items()}}}
    if "going_assessment" not in found_race:
        update["$set"]["going_assessment"] = going_assessment
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


def _parse_fd_age(race_type: str) -> str | None:
    if m := re.match(r"^(\d+)", race_type):
        return m.group(1)
    return None


def _parse_db_age(age_restriction: str | None) -> str | None:
    if age_restriction and (m := re.match(r"^(\d+)", age_restriction)):
        return m.group(1)
    return None


def find_candidate_race(racecourse_id, run):
    fd_dist = RaceDistance(f"{run.distance}f").furlongs
    fd_prize_k = int(run.win_prize)
    fd_is_hcap = "H" in run.race_type
    fd_age = _parse_fd_age(run.race_type)

    candidates = []

    for race in db.races.find(
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
    ):
        dist_str = race.get("distance_description", "")
        if not dist_str:
            continue
        try:
            skel_f = RaceDistance(dist_str).furlongs
        except Exception:
            continue
        if abs(skel_f - fd_dist) > 0.5:
            continue

        skel_raw = re.sub(r"[^\d]", "", race.get("prize", "") or "")
        skel_prize_k = int(skel_raw) // 1000 if skel_raw else None
        if fd_prize_k is not None and skel_prize_k is not None and fd_prize_k != skel_prize_k:
            continue

        if fd_is_hcap != race.get("is_handicap", False):
            continue

        if fd_age:
            db_age = _parse_db_age(race.get("age_restriction"))
            if db_age and db_age != fd_age:
                continue

        candidates.append(race)

    if len(candidates) == 1:
        return candidates[0]

    if len(candidates) > 1:
        logger = get_run_logger()
        logger.warning(
            f"{len(candidates)} candidates for {run.course} {run.date}: "
            f"{[str(c['_id']) for c in candidates]}"
        )

    return None


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
                race = find_candidate_race(racecourse_id, run)
                if not race:
                    logger.debug(
                        f"No race found for {horse['_id']} at "
                        f"{run.course} on {run.date}"
                    )
                    continue
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
