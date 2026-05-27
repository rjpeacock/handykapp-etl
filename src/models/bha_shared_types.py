import re
from typing import Annotated

from horsetalk import Gender  # type: ignore
from pydantic import AfterValidator, BeforeValidator


def validate_horse(v: str) -> str:
    if not v:
        raise ValueError("Horse name cannot be empty")

    has_country = bool(re.search(r"\([A-Z]{2,3}\)", v))
    if not has_country or len(v) > 30:
        raise ValueError(f"Invalid horse name: {v}")
    return v


def validate_year(v: int) -> int:
    if not (1600 <= v <= 2100):
        raise ValueError(f"Invalid year: {v}")
    return v


def empty_string_to_none(v):
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def validate_perf_fig(v: str) -> str:
    if v != "-":
        valid_keys = ["A", "H", "S", "T"]
        key, val = v.split(":")

        if key not in valid_keys:
            raise ValueError(f"Invalid performance figure key: {key}")
        if not (val.isdigit() or val == "x"):
            raise ValueError(f"Invalid performance figure value: {val}")

    return v


def validate_rating(v: int | None) -> int | None:
    if v is None:
        return v
    if not (0 <= v <= 240):
        raise ValueError(f"Invalid rating: {v}")
    return v


HorseName = Annotated[str, AfterValidator(validate_horse)]
BirthYear = Annotated[int, AfterValidator(validate_year)]
Rating = Annotated[
    int | None,
    BeforeValidator(empty_string_to_none),
    AfterValidator(validate_rating),
]
def validate_sex(v: str) -> str:
    if v.upper() not in {g.name for g in Gender}:
        raise ValueError(f"Invalid sex: {v}")
    return v.upper()


Sex = Annotated[str, AfterValidator(validate_sex)]
PerfFig = Annotated[str, AfterValidator(validate_perf_fig)]
