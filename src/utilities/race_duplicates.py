from typing import Any

from pymongo.collection import Collection


def find_duplicate_horses(races: Collection) -> list[dict]:
    pipeline = [
        {"$unwind": {"path": "$runners", "preserveNullAndEmptyArrays": False}},
        {
            "$group": {
                "_id": {"race": "$_id", "horse": "$runners.horse"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {
            "$group": {
                "_id": "$_id.race",
                "duplicates": {"$push": {"horse": "$_id.horse", "count": "$count"}},
            }
        },
    ]
    return list(races.aggregate(pipeline, allowDiskUse=True))


def resolve_duplicates(races: Collection, race_id: Any) -> int:
    race = races.find_one({"_id": race_id})
    if not race:
        return 0

    runners = race.get("runners", [])
    if not runners:
        return 0

    rated = sorted(runners, key=lambda r: bool(r.get("ratings")), reverse=True)
    horse_ids = {r["horse"] for r in runners}
    kept = [next(r for r in rated if r["horse"] == h) for h in horse_ids]
    removed_count = len(runners) - len(kept)

    if removed_count:
        races.update_one({"_id": race_id}, {"$set": {"runners": kept}})

    return removed_count
