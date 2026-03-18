import pytest
from pymongo.errors import DuplicateKeyError

from models import PreMongoHorse
from processors.horse_processor import (
    make_horse_update_dictionary,
    make_horse_insert_dictionary,
    horse_processor,
)


def test_make_horse_update_dictionary_with_all_fields(mocker):
    horse = PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
        sex="M",
        colour="Bay",
    )
    db_horse = {
        "_id": "horse_1",
        "name": "Test Horse",
        "country": "GB",
        "year": 2020,
    }
    
    mocker.patch("processors.horse_processor.get_horse", return_value={"_id": "sire_1"})
    
    result = make_horse_update_dictionary(horse, db_horse)
    
    assert result["colour"] == "Bay"


def test_make_horse_update_dictionary_with_sire(mocker):
    sire = PreMongoHorse(name="Sire Horse", country="GB", year=2010, sex="M")
    horse = PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
        sex="M",
        sire=sire,
    )
    db_horse = {
        "_id": "horse_1",
        "name": "Test Horse",
        "country": "GB",
        "year": 2020,
    }
    
    mocker.patch("processors.horse_processor.get_horse", return_value={"_id": "sire_1"})
    
    result = make_horse_update_dictionary(horse, db_horse)
    
    assert result["sire"] == "sire_1"


def test_make_horse_insert_dictionary_with_all_fields(mocker):
    horse = PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
        sex="M",
        colour="Bay",
    )
    
    mocker.patch("processors.horse_processor.get_horse", return_value={"_id": "sire_1"})
    
    result = make_horse_insert_dictionary(horse)
    
    assert result["name"] == "Test Horse"
    assert result["country"] == "GB"
    assert result["year"] == 2020
    assert result["sex"] == "M"
    assert result["colour"] == "Bay"


def test_make_horse_insert_dictionary_with_sire_and_dam(mocker):
    sire = PreMongoHorse(name="Sire Horse", country="GB", year=2010, sex="M")
    dam = PreMongoHorse(name="Dam Horse", country="GB", year=2010, sex="F")
    horse = PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
        sex="M",
        sire=sire,
        dam=dam,
    )
    
    mocker.patch("processors.horse_processor.get_horse", return_value={"_id": "parent_1"})
    
    result = make_horse_insert_dictionary(horse)
    
    assert "sire" in result
    assert "dam" in result


def test_horse_processor_inserts_new_horse(mock_db, mocker):
    mock_insert_one = mocker.patch.object(mock_db.horses, "insert_one")
    mock_insert_one.return_value = mocker.MagicMock(inserted_id="generated_id")
    
    mocker.patch("processors.horse_processor.db", mock_db)
    mocker.patch("processors.horse_processor.get_run_logger")
    mocker.patch("processors.horse_processor.get_horse", return_value=None)
    
    gen = horse_processor()
    next(gen)
    
    horse = PreMongoHorse(
        name="New Horse",
        country="GB",
        year=2020,
    )
    gen.send(horse)
    gen.close()


def test_horse_processor_updates_existing_horse(mock_db, mocker):
    mock_db.horses.insert_one({"_id": "horse_1", "name": "Existing Horse", "country": "GB", "year": 2020})
    mocker.patch("processors.horse_processor.db", mock_db)
    mocker.patch("processors.horse_processor.get_run_logger")
    mocker.patch("processors.horse_processor.get_horse", return_value={"_id": "horse_1", "name": "Existing Horse"})
    
    bulk_write_calls = []
    def mock_bulk_write(operations):
        bulk_write_calls.extend(operations)
    
    mock_db.horses.bulk_write = mock_bulk_write
    
    gen = horse_processor()
    next(gen)
    
    horse = PreMongoHorse(
        name="Existing Horse",
        country="GB",
        year=2020,
        colour="Bay",
    )
    gen.send(horse)
    gen.close()

    assert len(bulk_write_calls) == 1


def test_horse_processor_handles_duplicate_key_error(mock_db, mocker):
    mock_insert_one = mocker.patch.object(mock_db.horses, "insert_one")
    mock_insert_one.side_effect = DuplicateKeyError("duplicate key")
    
    mocker.patch("processors.horse_processor.db", mock_db)
    mocker.patch("processors.horse_processor.get_run_logger")
    mocker.patch("processors.horse_processor.get_horse", return_value=None)
    
    gen = horse_processor()
    next(gen)
    
    horse = PreMongoHorse(
        name="Duplicate Horse",
        country="GB",
        year=2020,
    )
    gen.send(horse)
    gen.close()
