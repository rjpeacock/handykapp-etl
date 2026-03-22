import pytest
from datetime import datetime, timezone

from src.helpers.run_tracker import get_last_load, update_load, LoadRecord


def test_get_last_load_returns_none_when_no_record(mock_db):
    result = get_last_load(mock_db, "theracingapi")
    assert result is None


def test_update_load_creates_record(mock_db):
    update_load(mock_db, "theracingapi", "2023-01-01", 100, "success")
    
    result = get_last_load(mock_db, "theracingapi")
    assert result is not None
    assert result.source == "theracingapi"
    assert result.last_processed == "2023-01-01"
    assert result.records_loaded == 100
    assert result.status == "success"
    assert result.last_run is not None


def test_update_load_updates_existing_record(mock_db):
    update_load(mock_db, "theracingapi", "2023-01-01", 100, "success")
    update_load(mock_db, "theracingapi", "2023-01-02", 50, "success")
    
    result = get_last_load(mock_db, "theracingapi")
    assert result.last_processed == "2023-01-02"
    assert result.records_loaded == 50


def test_update_load_with_none_last_processed(mock_db):
    update_load(mock_db, "bha", None, 0, "skipped")
    
    result = get_last_load(mock_db, "bha")
    assert result is not None
    assert result.last_processed is None
    assert result.records_loaded == 0
    assert result.status == "skipped"


def test_get_last_load_returns_none_for_different_source(mock_db):
    update_load(mock_db, "theracingapi", "2023-01-01", 100, "success")
    
    result = get_last_load(mock_db, "bha")
    assert result is None


def test_load_record_model():
    record = LoadRecord(
        source="rapid",
        last_run=datetime(2023, 1, 1),
        last_processed="file123",
        records_loaded=50,
        status="success"
    )
    assert record.source == "rapid"
    assert record.last_processed == "file123"
    assert record.records_loaded == 50
    assert record.status == "success"


def test_load_record_defaults():
    record = LoadRecord(source="test", last_run=datetime.now(timezone.utc))
    assert record.last_processed is None
    assert record.records_loaded == 0
    assert record.status == "success"
