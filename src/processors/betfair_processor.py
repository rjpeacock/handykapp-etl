from collections.abc import Generator
from datetime import datetime
from typing import Any

from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models.betfair_price_record import BetfairPriceRecord

db = client.handykapp


def find_race(rec: BetfairPriceRecord) -> dict | None:
    return db.races.find_one(
        {f"bf_{rec.market_type}_market_id": rec.event_id}
    ) or db.races.find_one({"datetime": rec.race_datetime})


def process_race_group(
    race: dict,
    records: list[BetfairPriceRecord],
) -> int:
    race_id = race["_id"]
    runners = race.get("runners", [])

    horse_ids = [r["horse"] for r in runners]
    horses = {h["_id"]: h for h in db.horses.find({"_id": {"$in": horse_ids}})}

    update_fields: dict[str, Any] = {}
    updated_horses: set[Any] = set()

    for rec in records:
        runner_idx = next(
            (
                i
                for i, r in enumerate(runners)
                if horses.get(r["horse"])
                and horses[r["horse"]]["name"] == rec.horse_name
            ),
            None,
        )
        if runner_idx is None:
            continue

        prefix = f"runners.{runner_idx}.prices"
        update_fields[f"{prefix}.bsp_{rec.market_type}"] = rec.bsp
        update_fields[f"{prefix}.bwap_{rec.market_type}"] = rec.pre_play_wap

        horse_id = runners[runner_idx]["horse"]
        if horse_id not in updated_horses:
            db.horses.update_one(
                {"_id": horse_id},
                {"$set": {"bf_id": rec.selection_id}},
            )
            updated_horses.add(horse_id)

    if not update_fields:
        return 0

    if win_rec := next((r for r in records if r.market_type == "win"), None):
        update_fields["bf_win_market_id"] = win_rec.event_id
    if place_rec := next((r for r in records if r.market_type == "place"), None):
        update_fields["bf_place_market_id"] = place_rec.event_id

    db.races.update_one({"_id": race_id}, {"$set": update_fields})
    return len(records)


def flush_betfair_prices(
    buffer: list[BetfairPriceRecord], logger: Any
) -> tuple[int, int]:
    if not buffer:
        return 0, 0

    groups: dict[tuple[datetime, str], list[BetfairPriceRecord]] = {}
    for rec in buffer:
        key = (rec.race_datetime, rec.country)
        groups.setdefault(key, []).append(rec)

    added = 0
    skipped = 0

    for (race_datetime, country), records in groups.items():
        try:
            race = find_race(records[0])
            if not race:
                logger.warning(
                    f"No race found for {records[0].race_datetime} ({country})"
                )
                skipped += len(records)
                continue

            processed = process_race_group(race, records)
            if processed:
                added += processed
            else:
                skipped += len(records)

        except Exception:
            logger.exception(f"Error processing race at {race_datetime} ({country})")
            skipped += len(records)

    return added, skipped


def betfair_price_processor() -> Generator[None, BetfairPriceRecord | None, None]:
    logger = get_run_logger()
    logger.info("Starting betfair price processor")
    buffer: list[BetfairPriceRecord] = []
    added_count = 0
    skipped_count = 0

    try:
        while True:
            record = yield
            if record is None:
                a, s = flush_betfair_prices(buffer, logger)
                added_count += a
                skipped_count += s
                buffer.clear()
            else:
                buffer.append(record)
    except GeneratorExit:
        if buffer:
            a, s = flush_betfair_prices(buffer, logger)
            added_count += a
            skipped_count += s
        logger.info(
            f"Finished processing Betfair prices. "
            f"Added {added_count}, skipped {skipped_count}"
        )


def betfair_pnl_processor():
    logger = get_run_logger()
    logger.info("Starting betfair processor")
    updated_count = 0
    added_count = 0
    skipped_count = 0

    try:
        while True:
            pnl_line = yield

            try:
                inserted_pnl_line = db.betfair.insert_one(pnl_line.model_dump())
                found_id = inserted_pnl_line.inserted_id
                logger.debug(
                    f"{pnl_line.racecourse} {pnl_line.race_datetime} P&L added to db. ID: {found_id}"
                )
                added_count += 1
            except DuplicateKeyError:
                logger.warning(
                    f"Duplicate P&L line: {pnl_line.racecourse} {pnl_line.race_datetime}"
                )
                skipped_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing Betfair P&L. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
