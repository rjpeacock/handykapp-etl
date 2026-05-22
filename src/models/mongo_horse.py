from typing import Literal

from pydantic import BaseModel, Field
from pydantic_extra_types.pendulum_dt import Date

from .py_object_id import PyObjectId


class MongoOperation(BaseModel):
    operation_type: str
    date: Date | None = None


class MongoOfficialRatings(BaseModel):
    flat: int | None = None
    aw: int | None = None
    chase: int | None = None
    hurdle: int | None = None


class MongoHorse(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    name: str = Field(..., min_length=2, max_length=21)
    country: str | None = Field(default=None, min_length=2, max_length=3)
    year: int | None = None
    sex: Literal["M", "F"] | None = None
    breed: str | None = None
    colour: str | None = None
    sire: PyObjectId | None = None
    dam: PyObjectId | None = None
    trainer: PyObjectId | None = None
    operations: list[MongoOperation] | None = None
    ratings: MongoOfficialRatings | None = None
    bf_id: str | None = None
