from pydantic_extra_types.pendulum_dt import Duration

from .pre_mongo_entry import PreMongoEntry


class PreMongoRunner(PreMongoEntry):
    finishing_position: str | None = None
    official_position: str | None = None
    beaten_distance: float | None = None
    time: Duration | None = None
    sp: str | None = None
    non_runner: bool | None = None
