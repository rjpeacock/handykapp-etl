from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

import pendulum
from pydantic import BaseModel, BeforeValidator, Field, model_validator


def parse_race_datetime(v) -> datetime:
    if isinstance(v, datetime):
        return pendulum.instance(v)
    return pendulum.from_format(str(v), "DD-MM-YYYY HH:mm")


RaceDatetime = Annotated[datetime, BeforeValidator(parse_race_datetime)]


def parse_win_lose(data: dict) -> dict:
    v = int(data["WIN_LOSE"])
    if v > 1:
        logging.warning(
            f"Unexpected WIN_LOSE value '{v}' "
            f"for {data['EVENT_NAME']} "
            f"({data['MENU_HINT']}, "
            f"selection {data['SELECTION_NAME']}, "
            f"ID {data['EVENT_ID']})"
        )
    data["WIN_LOSE"] = bool(v)
    return data


class BetfairPriceRecord(BaseModel):
    model_config = {"populate_by_name": True}

    event_id: str = Field(alias="EVENT_ID")
    course_and_date: str = Field(alias="MENU_HINT")
    event_name: str = Field(alias="EVENT_NAME")
    race_datetime: RaceDatetime = Field(alias="EVENT_DT")
    selection_id: str = Field(alias="SELECTION_ID")
    horse_name: str = Field(alias="SELECTION_NAME")
    win: bool = Field(alias="WIN_LOSE")
    bsp: Decimal | None = Field(alias="BSP")
    pre_play_wap: Decimal | None = Field(alias="PPWAP")
    morning_wap: Decimal | None = Field(alias="MORNINGWAP")
    pre_play_max: Decimal | None = Field(alias="PPMAX")
    pre_play_min: Decimal | None = Field(alias="PPMIN")
    in_play_max: Decimal | None = Field(alias="IPMAX")
    in_play_min: Decimal | None = Field(alias="IPMIN")
    morning_traded_volume: Decimal | None = Field(alias="MORNINGTRADEDVOL")
    pre_play_traded_volume: Decimal | None = Field(alias="PPTRADEDVOL")
    in_play_traded_volume: Decimal | None = Field(alias="IPTRADEDVOL")
    country: str = ""
    market_type: str = ""

    @model_validator(mode="before")
    @classmethod
    def validate_win_lose(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return parse_win_lose(data)
        return data
