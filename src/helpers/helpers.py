from typing import Literal

import pendulum
from horsetalk import Horse
from peak_utility.listish import compact
from prefect import get_run_logger
from pydantic_extra_types.pendulum_dt import Date
from requests import ConnectionError as RequestsConnectionError
from requests import Timeout, get
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from models import MongoHorse, MongoOperation, PreMongoHorse


class FetchError(Exception):
    def __init__(self, message: str, url: str, attempt: int):
        super().__init__(message)
        self.url = url
        self.attempt = attempt


def _log_retry(retry_state: RetryCallState) -> None:
    logger = get_run_logger()
    attempt = retry_state.attempt_number + 1
    logger.warning(f"Retry {attempt}/5 for URL: {retry_state.args[0] if retry_state.args else 'unknown'}")


def _wrap_fetch_error(retry_state: RetryCallState) -> None:
    if retry_state.outcome and retry_state.outcome.failed():
        exc = retry_state.outcome.exception()
        if exc and not isinstance(exc, FetchError):
            attempt = retry_state.attempt_number
            raise FetchError(str(exc), retry_state.args[0] if retry_state.args else "", attempt) from exc


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    retry=retry_if_exception_type((RequestsConnectionError, Timeout, OSError)),
    reraise=True,
    before_sleep=_log_retry,
    after=_wrap_fetch_error,
)
def fetch_content(url, params=None, headers=None):
    response = get(url, params=params, headers=headers, timeout=30)
    if response.status_code >= 500:
        raise OSError(f"Server error: {response.status_code}")
    response.raise_for_status()
    return response.content


def get_last_occurrence_of(weekday):
    return pendulum.now().add(days=1).previous(weekday).date()


def log_validation_problem(problem):
    msg = f"{problem['error']} in row {problem['row']} for {problem['field']}: {problem['value']}"
    logger = get_run_logger()
    logger.warning(msg)


def horse_name_to_pre_mongo_horse(
    name: str,
    *,
    sex: Literal["M", "F"] | None = None,
    sire: PreMongoHorse | None = None,
    default_country: str | None = None,
) -> PreMongoHorse | None:
    if not name:
        return None

    horse = Horse(name)

    if not horse:
        return None

    name = horse.name
    country = horse.country.name if horse.country else default_country

    params = compact(
        {
            "name": name.upper(),
            "country": country,
            "sex": sex,
            "sire": sire.model_dump() if sire else None,
        }
    )
    return PreMongoHorse(**params)


def create_gelding_operation(date: Date) -> MongoOperation:
    return MongoOperation(operation_type="gelding", date=date)


def get_operations(horse: PreMongoHorse) -> list[MongoOperation] | None:
    if not horse.gelded_from:
        return None

    return [create_gelding_operation(horse.gelded_from)]


def make_operations_update(
    horse: PreMongoHorse, db_horse: MongoHorse
) -> list[MongoOperation] | None:
    if not hasattr(horse, "gelded_from") or not horse.gelded_from:
        return None

    operations = db_horse.operations

    if not operations:
        return get_operations(horse)

    gelding_op = next((op for op in operations if op.operation_type == "gelding"), None)
    non_gelding_ops = [op for op in operations if op.operation_type != "gelding"]

    if not gelding_op:
        return [*operations, create_gelding_operation(horse.gelded_from)]

    current_date = gelding_op.date

    if current_date is None or horse.gelded_from < current_date:
        return [*non_gelding_ops, create_gelding_operation(horse.gelded_from)]

    return operations
