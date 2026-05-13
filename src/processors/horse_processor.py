from collections.abc import Generator

from peak_utility.listish import compact
from prefect import get_run_logger
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError
from tenacity import retry, stop_after_attempt, wait_exponential

from clients.mongo_client import get_horse, mongo_client
from helpers import get_operations, make_operations_update
from models import MongoHorse, PreMongoHorse

db = mongo_client.handykapp


def make_horse_update_dictionary(horse: PreMongoHorse, db_horse: MongoHorse):
    return compact(
        {
            "colour": horse.colour,
            "sire": sire["_id"]
            if (horse.sire and (sire := get_horse(horse.sire)))
            else None,
            "dam": dam["_id"]
            if (horse.dam and (dam := get_horse(horse.dam)))
            else None,
            "operations": make_operations_update(horse, db_horse),
            "ratings": horse.ratings,
        }
    )


def make_horse_insert_dictionary(horse: PreMongoHorse):
    return compact(
        {
            "name": horse.name,
            "sex": horse.sex,
            "year": horse.year,
            "country": horse.country,
            "colour": horse.colour,
            "sire": sire["_id"]
            if (horse.sire and (sire := get_horse(horse.sire)))
            else None,
            "dam": dam["_id"]
            if (horse.dam and (dam := get_horse(horse.dam)))
            else None,
            "operations": get_operations(horse),
            "ratings": horse.ratings,
        }
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    reraise=True,
)
def _flush_bulk_operations(operations):
    db.horses.bulk_write(operations)


def horse_processor() -> Generator[None, PreMongoHorse, None]:
    logger = get_run_logger()
    logger.info("Starting runner processor")
    added_count = 0
    updated_count = 0
    skipped_count = 0

    bulk_operations = []
    bulk_threshold = 100

    try:
        while True:
            horse = yield
            try:
                db_horse = get_horse(horse)

                if db_horse:
                    bulk_operations.append(
                        UpdateOne(
                            {"_id": db_horse["_id"]},
                            {"$set": make_horse_update_dictionary(horse, db_horse)},
                        )
                    )
                    logger.debug(f"{horse.name} updated")
                    updated_count += 1
                else:
                    try:
                        db.horses.insert_one(make_horse_insert_dictionary(horse))
                        logger.debug(f"{horse.name} added to db")
                        added_count += 1
                    except DuplicateKeyError:
                        logger.warning(f"Duplicate horse: {horse})")
                        skipped_count += 1
                    except ValueError as e:
                        logger.warning(e)
                        skipped_count += 1

                # Process bulk operations when threshold reached
                if bulk_operations and len(bulk_operations) >= bulk_threshold:
                    _flush_bulk_operations(bulk_operations)
                    logger.debug(f"Processed {len(bulk_operations)} bulk horse operations")
                    bulk_operations = []
            except Exception:
                logger.exception(f"Horse processor error: horse={horse.name}")
                skipped_count += 1

    except GeneratorExit:
        # Process any remaining bulk operations
        if bulk_operations:
            _flush_bulk_operations(bulk_operations)
            logger.debug(f"Processed {len(bulk_operations)} remaining bulk operations")

        logger.info(
            f"Finished processing horses. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
