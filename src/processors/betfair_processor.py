from collections.abc import Generator

from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models.betfair_price_record import BetfairPriceRecord

db = client.handykapp


def betfair_price_processor() -> Generator[None, BetfairPriceRecord, None]:
    logger = get_run_logger()
    logger.info("Starting betfair price processor")
    added_count = 0
    skipped_count = 0

    try:
        while True:
            _price_line = yield
            # TODO: insert into MongoDB
            added_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing Betfair prices. Added {added_count}, skipped {skipped_count}"
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
                inserted_pnl_line = db.betfair.insert_one(
                    pnl_line.model_dump()
                )
                found_id = inserted_pnl_line.inserted_id
                logger.debug(f"{pnl_line.racecourse} {pnl_line.race_datetime} P&L added to db. ID: {found_id}")
                added_count += 1
            except DuplicateKeyError:
                logger.warning(f"Duplicate P&L line: {pnl_line.racecourse} {pnl_line.race_datetime}")
                skipped_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing Betfair P&L. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
