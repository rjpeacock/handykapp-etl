import time
from collections.abc import Generator

from nameparser import HumanName  # type: ignore
from prefect import get_run_logger
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models import PreMongoPerson, PyObjectId

db = client.handykapp


def preload_person_cache(names, source):
    """Preload cache with people already in database"""
    cache = {}
    if not names:
        return cache

    # Batch query people by names
    persons = db.people.find({"references." + source: {"$in": list(names)}})
    for person in persons:
        source_name = person.get("references", {}).get(source)
        if source_name:
            cache[source_name, source] = person["_id"]
    return cache


def person_processor() -> Generator[None, tuple[PreMongoPerson, str], None]:
    logger = get_run_logger()
    logger.info("Starting person processor")
    person_cache: dict[tuple[str, str], PyObjectId] = {}
    pending_people = set()
    batch_size = 50
    updated_count = 0
    added_count = 0
    skipped_count = 0
    person_updates = []
    person_update_threshold = 100

    try:
        while True:
            person, source = yield
            try:
                name = person.name
                race_id = person.race_id
                runner_id = person.runner_id
                role = person.role
                ratings = person.ratings or None

                # Add to pending batch
                pending_people.add(name)

                # When batch gets large enough, preload cache
                if len(pending_people) >= batch_size:
                    logger.debug(f"Preloading cache with {len(pending_people)} people")
                    person_cache.update(preload_person_cache(pending_people, source))
                    pending_people = set()

                cache_key = (name, source)
                if cache_key in person_cache:
                    found_id = person_cache[cache_key]
                    logger.debug(f"Cache hit for {name}")
                    if ratings:
                        db.people.update_one(
                            {"_id": found_id},
                            {"$set": {"ratings": ratings}},
                        )
                else:
                    found_person = None
                    name_parts = HumanName(name)

                    # Only fetch fields we need for matching - much faster
                    possibilities = db.people.find(
                        {"last": name_parts.last}, {"_id": 1, "first": 1, "title": 1}
                    )
                    for possibility in possibilities:
                        if name_parts.first == possibility["first"] or (
                            name_parts.first
                            and possibility["first"]
                            and name_parts.first[0] == possibility["first"][0]
                            and name_parts.title == possibility["title"]
                        ):
                            found_person = possibility
                            break

                    if found_person:
                        found_id = found_person["_id"]
                        update_data = {f"references.{source}": name} | (
                            {"ratings": ratings} if ratings else {}
                        )
                        db.people.update_one(
                            {"_id": found_id},
                            {"$set": update_data},
                        )
                        logger.debug(f"{person} updated")
                        updated_count += 1
                    else:
                        try:
                            inserted_person = db.people.insert_one(
                                name_parts.as_dict()
                                | {"references": {source: name}}
                                | ({"ratings": ratings} if ratings else {})
                            )
                            found_id = inserted_person.inserted_id
                            logger.debug(f"{person} added to db")
                            added_count += 1
                        except DuplicateKeyError:
                            logger.warning(f"Duplicate person: {name}")
                            skipped_count += 1
                            continue

                # Add person to horse in race
                if race_id:
                    person_updates.append(
                        UpdateOne(
                            {"_id": race_id, "runners.horse": runner_id},
                            {"$set": {f"runners.$.{role}": found_id}},
                        )
                    )
                    if len(person_updates) >= person_update_threshold:
                        db.races.bulk_write(person_updates, ordered=False)
                        time.sleep(0.05)
                        person_updates = []
            except Exception:
                logger.exception(
                    f"Person processor error: name={person.name}, "
                    f"role={person.role}, source={source}"
                )
                skipped_count += 1
                continue

    except GeneratorExit:
        if person_updates:
            db.races.bulk_write(person_updates, ordered=False)
        logger.info(
            f"Finished processing people. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
