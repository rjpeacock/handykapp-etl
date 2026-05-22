from decimal import Decimal
from typing import Literal

from pydantic import BaseModel
from pydantic_extra_types.pendulum_dt import Date, Duration

from .py_object_id import PyObjectId


class Prices(BaseModel):
    sp: str | None = None
    bsp_win: Decimal | None = None
    bwap_win: Decimal | None = None
    bsp_place: Decimal | None = None
    bwap_place: Decimal | None = None


class MongoRunner(BaseModel):
    horse: PyObjectId
    sex: Literal["M", "F"] | None = None
    gelded_from: Date | None = None
    colour: str | None = None
    owner: str | None = None
    lbs_carried: int | None = None
    allowance: int | None = None
    saddlecloth: int | None = None
    draw: int | None = None
    headgear: str | None = None
    official_rating: int | None = None
    trainer: PyObjectId | None = None
    jockey: PyObjectId | None = None
    sire: PyObjectId | None = None
    dam: PyObjectId | None = None
    damsire: PyObjectId | None = None
    finishing_position: str | None = None
    official_position: str | None = None
    beaten_distance: float | None = None
    time: Duration | None = None
    prices: Prices | None = None
    ratings: dict[str, int] | None = None
