from typing import Literal

from pydantic import BaseModel, Field


class TheRacingApiRunner(BaseModel):
    horse: str = Field(..., description="Name of the horse")
    age: int = Field(..., description="Age of the horse")
    sex: str = Field(..., description="Sex of the horse (e.g., gelding, mare)")
    sex_code: str = Field(..., description="Code representing the sex of the horse")
    colour: str = Field(..., description="Colour of the horse")
    region: str = Field(..., description="Region of the horse's origin")
    dam: str = Field(..., description="Name of the dam (mother) of the horse")
    sire: str = Field(..., description="Name of the sire (father) of the horse")
    damsire: str = Field(..., description="Name of the dam's sire (grandfather)")
    trainer: str = Field(..., description="Name of the trainer")
    owner: str = Field(..., description="Name of the owner")
    number: str = Field(..., pattern=r"^(NR|R?\d+)$", description="Saddlecloth number")
    draw: int | Literal[""] = Field(..., description="Draw position")
    headgear: str | None = Field(None, description="Headgear worn by the horse")
    lbs: int | Literal[""] = Field(..., description="Weight carried by the horse in pounds")
    ofr: int | Literal["-"] = Field(..., description="Official rating of the horse")
    jockey: str = Field(..., description="Name of the jockey")
    last_run: str = Field(..., description="Days since the horse's last run")
    form: str | None = Field(None, description="Recent form of the horse")
