from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated

import pendulum
from pydantic import BaseModel, BeforeValidator, Field


def parse_race_datetime(v) -> datetime:
    if isinstance(v, datetime):
        return pendulum.instance(v)
    return pendulum.from_format(str(v), "DD-MM-YYYY HH:mm")


RaceDatetime = Annotated[datetime, BeforeValidator(parse_race_datetime)]


class BetfairPriceRecord(BaseModel):
    model_config = {"populate_by_name": True}

    event_id: str = Field(alias="event_id")
    course_and_date: str = Field(alias="menu_hint")
    event_name: str = Field(alias="event_name")
    race_datetime: RaceDatetime = Field(alias="event_dt")
    selection_id: str = Field(alias="selection_id")
    horse_name: str = Field(alias="selection_name")
    win: bool = Field(alias="win_lose")
    bsp: Decimal = Field(alias="bsp")
    pre_play_wap: Decimal = Field(alias="ppwap")
    morning_wap: Decimal = Field(alias="morningwap")
    pre_play_max: Decimal = Field(alias="ppmax")
    pre_play_min: Decimal = Field(alias="ppmin")
    in_play_max: Decimal = Field(alias="ipmax")
    in_play_min: Decimal = Field(alias="ipmin")
    morning_traded_volume: Decimal = Field(alias="morningtradedvol")
    pre_play_traded_volume: Decimal = Field(alias="pptradedvol")
    in_play_traded_volume: Decimal = Field(alias="iptradedvol")
