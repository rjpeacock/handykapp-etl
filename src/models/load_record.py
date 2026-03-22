from datetime import datetime, timezone

from pydantic import BaseModel


class LoadRecord(BaseModel):
    source: str
    last_run: datetime
    last_processed: str | None = None
    records_loaded: int = 0
    status: str = "success"
