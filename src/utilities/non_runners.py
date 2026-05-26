from prefect import get_run_logger, task

from clients.mongo_client import db


@task
def mark_non_runners(
    set_position: bool = False,
    limit: int | None = None,
    dry_run: bool = False,
) -> tuple[int, list]:
    races = db.races.find(
        {"runners": {"$elemMatch": {"finishing_position": {"$ne": None}}}},
        {"runners": 1},
    ).limit(limit or 0)

    race_ids = []
    total_runners = 0

    for race in races:
        affected = [
            r for r in race.get("runners", [])
            if r.get("finishing_position") is None
            and not r.get("non_runner")
        ]
        if affected:
            race_ids.append(race["_id"])
            total_runners += len(affected)

    if not race_ids:
        return 0, []

    logger = get_run_logger()

    if dry_run:
        logger.info(f"Found {total_runners} non-runner(s) across {len(race_ids)} race(s).")
        return total_runners, race_ids

    modified_count = 0
    for race_id in race_ids:
        race = db.races.find_one({"_id": race_id}, {"runners": 1})
        for i, runner in enumerate(race.get("runners", [])):
            if runner.get("finishing_position") is None and not runner.get("non_runner"):
                update = {"$set": {f"runners.{i}.non_runner": True}}
                if set_position:
                    update["$set"][f"runners.{i}.finishing_position"] = "N"
                db.races.update_one({"_id": race_id}, update)
                modified_count += 1

    logger.info(f"Marked {modified_count} non-runner(s) across {len(race_ids)} race(s).")
    return modified_count, race_ids
