from datetime import datetime, timezone

from models.load_record import LoadRecord


def get_last_load(db, source: str) -> LoadRecord | None:
    doc = db.loads.find_one({"source": source})
    if doc is None:
        return None
    return LoadRecord(
        source=doc["source"],
        last_run=doc["last_run"],
        last_processed=doc.get("last_processed"),
        records_loaded=doc.get("records_loaded", 0),
        status=doc.get("status", "success"),
    )


def update_load(
    db,
    source: str,
    last_processed: str | None,
    records_loaded: int,
    status: str,
) -> None:
    db.loads.update_one(
        {"source": source},
        {
            "$set": {
                "source": source,
                "last_run": datetime.now(timezone.utc),
                "last_processed": last_processed,
                "records_loaded": records_loaded,
                "status": status,
            }
        },
        upsert=True,
    )
