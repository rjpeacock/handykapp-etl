from datetime import datetime

from pydantic import BaseModel, Field

from .mongo_runner import MongoRunner
from .py_object_id import PyObjectId


class MongoRace(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    rapid_id: str | None = None
    racecourse: PyObjectId
    datetime: datetime
    title: str
    is_handicap: bool | None = None
    obstacle: str | None = None
    distance_description: str
    race_grade: str | None = None
    race_class: int | None = None
    age_restriction: str | None = None
    rating_restriction: str | None = None
    prize: str | None = None
    going_description: str | None = None
    bf_win_market_id: str | None = None
    bf_place_market_id: str | None = None
    runners: list[MongoRunner] = []
