from collections.abc import Generator
from typing import Any

from peak_utility.listish import compact
from prefect import get_run_logger
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from clients.mongo_client import get_horse, mongo_client
from models import PreMongoPerson, PreMongoRunner, PyObjectId, Role
from processors.horse_processor import (
    make_horse_insert_dictionary,
    make_horse_update_dictionary,
)
from processors.person_processor import person_processor
from processors.utils import log_memory_usage

db = mongo_client.handykapp


def make_runner_dict(horse: PreMongoRunner, horse_id: PyObjectId) -> dict:
    return compact(
        {
            "horse": horse_id,
            "owner": horse.owner,
            "allowance": horse.allowance,
            "saddlecloth": horse.saddlecloth,
            "draw": horse.draw,
            "headgear": horse.headgear,
            "lbs_carried": horse.lbs_carried,
            "official_rating": horse.official_rating,
            "finishing_position": horse.finishing_position,
            "official_position": horse.official_position,
            "beaten_distance": horse.beaten_distance,
            "sp": horse.sp,
            "non_runner": horse.non_runner,
        }
    )


def collect_people(
    horse: PreMongoRunner, race_id: PyObjectId, horse_id: PyObjectId, source: str
) -> list:
    roles: tuple[Role, ...] = ("trainer", "jockey")
    return [
        (
            PreMongoPerson(
                name=person_name,
                role=role,
                race_id=race_id,
                runner_id=horse_id,
            ),
            source,
        )
        for role in roles
        if (person_name := getattr(horse, role, None))
    ]


def flush_races(race_updates: dict, logger: Any):
    for rid, runners in race_updates.items():
        race = db.races.find_one({"_id": rid}, {"runners.horse": 1})
        existing_horses = {
            r["horse"] for r in race.get("runners", [])
        } if race else set()
        new_runners = [
            r for r in runners if r.get("horse") not in existing_horses
        ]
        if new_runners:
            db.races.update_one(
                {"_id": rid},
                {"$push": {"runners": {"$each": new_runners}}},
            )
    logger.debug(f"Updated {len(race_updates)} races with runners")


def flush_people(
    pending_people: list,
    person_gen: Generator[None, tuple[PreMongoPerson, str], None],
    logger: Any,
) -> Generator[None, tuple[PreMongoPerson, str], None]:
    for person_data in pending_people:
        try:
            person_gen.send(person_data)
        except StopIteration:  # noqa: PERF203
            logger.error("person_processor generator died, reinitializing")
            person_gen = person_processor()
            next(person_gen)
            person_gen.send(person_data)
        except Exception:
            logger.exception(f"Error processing person: {person_data}")

    return person_gen


def runner_processor() -> Generator[None, tuple[PreMongoRunner, PyObjectId, str], None]:
    logger = get_run_logger()
    logger.info("Starting runner processor")
    added_count = 0
    updated_count = 0
    skipped_count = 0

    p = person_processor()
    next(p)

    horse_updates = []
    horse_update_threshold = 500

    race_updates: dict[PyObjectId, list[dict]] = {}
    race_update_threshold = 20

    pending_people = []

    try:
        while True:
            horse, race_id, source = yield

            try:
                db_horse = get_horse(horse)

                if db_horse:
                    horse_updates.append(
                        UpdateOne(
                            {"_id": db_horse["_id"]},
                            {"$set": make_horse_update_dictionary(horse, db_horse)},
                        )
                    )
                    horse_id = db_horse["_id"]
                    logger.debug(f"{horse.name} updated")
                    updated_count += 1
                else:
                    try:
                        horse_id = db.horses.insert_one(
                            make_horse_insert_dictionary(horse)
                        ).inserted_id
                        logger.debug(f"{horse.name} added to db")
                        added_count += 1
                    except DuplicateKeyError:
                        logger.warning(f"Duplicate horse: {horse}) in race {race_id}")
                        skipped_count += 1
                        continue
                    except ValueError as e:
                        logger.warning(e)
                        skipped_count += 1
                        continue

                # Process horse updates when threshold reached
                if horse_updates and len(horse_updates) >= horse_update_threshold:
                    db.horses.bulk_write(horse_updates)
                    logger.debug(f"Processed {len(horse_updates)} bulk horse operations")
                    log_memory_usage()
                    horse_updates = []

                if not race_id:
                    continue

                if race_id not in race_updates:
                    race_updates[race_id] = []

                race_updates[race_id].append(make_runner_dict(horse, horse_id))
                pending_people.extend(collect_people(horse, race_id, horse_id, source))

                if len(race_updates) >= race_update_threshold:
                    flush_races(race_updates, logger)
                    p = flush_people(pending_people, p, logger)
                    race_updates = {}
                    pending_people = []
            except Exception:
                logger.exception(
                    f"Runner processor error: horse={horse.name}, "
                    f"race_id={race_id}, source={source}"
                )
                skipped_count += 1
                continue

    except GeneratorExit:
        if horse_updates:
            db.horses.bulk_write(horse_updates)
            logger.debug(f"Processed {len(horse_updates)} remaining bulk operations")

        if race_updates:
            flush_races(race_updates, logger)
        if pending_people:
            p = flush_people(pending_people, p, logger)

        log_memory_usage()
        logger.info(
            f"Finished processing runners. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
        p.close()
