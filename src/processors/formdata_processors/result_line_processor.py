from collections.abc import Generator

from horsetalk import Going
from prefect import get_run_logger

from clients import mongo_client as client
from clients.mongo_client import rr_code_to_course_dict
from models import FormdataRun

db = client.handykapp

FORMDATA_AW_GOINGS = {
    "s": "Slow",
    "d": "Standard to Slow",
    "g": "Standard",
    "m": "Standard to Fast",
    "f": "Fast",
}


def result_line_processor() -> Generator[None, tuple[dict, FormdataRun], None]:
    logger = get_run_logger()
    logger.info("Starting result line processor")

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

            if found_race:
                race_id = found_race["_id"]

                going_value = (
                    run.going
                    if surface == "Turf"
                    else FORMDATA_AW_GOINGS.get(run.going, "Standard")
                )
                going_assessment = str(Going(going_value))
                db.races.update_one(
                    {"_id": race_id, "runners.horse": horse["_id"]},
                    {
                        "$set": {
                            "going_assessment": going_assessment,
                            "runners.$.finishing_position": run.position,
                            "runners.$.beaten_distance": run.beaten_distance,
                            "runners.$.ratings.rr_time": run.time_rating,
                            "runners.$.ratings.rr_form": run.form_rating,
                        }
                    },
                )
                logger.debug(
                    f"Added result for {horse['_id']} in race at {run.course} on {run.date}"
                )
            else:
                logger.warning(
                    f"No race found for {horse['_id']} at {run.course} on {run.date}"
                )
    except GeneratorExit:
        logger.info("Finished processing results.")
