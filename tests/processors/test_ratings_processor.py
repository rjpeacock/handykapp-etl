import pytest
import importlib
import sys

from src.models import PreMongoHorse


def get_ratings_processor_module():
    if "src.processors.ratings_processor" in sys.modules:
        del sys.modules["src.processors.ratings_processor"]
    return importlib.import_module("src.processors.ratings_processor")


def test_ratings_processor_inserts_ratings(mock_db, mocker):
    rp_module = get_ratings_processor_module()
    
    mock_db.horses.insert_one({"_id": "horse_id_1", "name": "Test Horse"})
    
    def mock_bulk_write(operations):
        for op in operations:
            mock_db.horses.update_one(op._filter, op._doc)
    
    mock_db.horses.bulk_write = mock_bulk_write
    mocker.patch.object(rp_module, "db", mock_db)

    get_horse = mocker.patch("src.processors.ratings_processor.get_horse")
    get_horse.return_value = {"_id": "horse_id_1", "name": "Test Horse"}
    
    gen = rp_module.ratings_processor()
    next(gen)
    gen.send(PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
        ratings={"flat": 100, "aw": 95},
    ))
    gen.close()

    result = mock_db.horses.find_one({"_id": "horse_id_1"})
    assert result["ratings"]["flat"] == 100
    assert result["ratings"]["aw"] == 95


def test_ratings_processor_skips_missing_horse(mock_db, mocker):
    rp_module = get_ratings_processor_module()
    
    def mock_bulk_write(operations):
        for op in operations:
            mock_db.horses.update_one(op._filter, op._doc)
    
    mock_db.horses.bulk_write = mock_bulk_write
    mocker.patch.object(rp_module, "db", mock_db)

    get_horse = mocker.patch("src.processors.ratings_processor.get_horse")
    get_horse.return_value = None
    
    gen = rp_module.ratings_processor()
    next(gen)
    gen.send(PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
        ratings={"flat": 100, "aw": 95},
    ))
    gen.close()

    assert mock_db.horses.count_documents({}) == 0


def test_ratings_processor_handles_no_ratings(mock_db, mocker):
    rp_module = get_ratings_processor_module()
    
    mock_db.horses.insert_one({"_id": "horse_id_1", "name": "Test Horse"})
    
    def mock_bulk_write(operations):
        for op in operations:
            mock_db.horses.update_one(op._filter, op._doc)
    
    mock_db.horses.bulk_write = mock_bulk_write
    mocker.patch.object(rp_module, "db", mock_db)

    get_horse = mocker.patch("src.processors.ratings_processor.get_horse")
    get_horse.return_value = {"_id": "horse_id_1", "name": "Test Horse"}
    
    gen = rp_module.ratings_processor()
    next(gen)
    gen.send(PreMongoHorse(
        name="Test Horse",
        country="GB",
        year=2020,
    ))
    gen.close()

    result = mock_db.horses.find_one({"_id": "horse_id_1"})
    assert result["ratings"] == {}


def test_ratings_processor_bulk_operations(mock_db, mocker):
    rp_module = get_ratings_processor_module()
    
    bulk_write_calls = []
    
    def mock_bulk_write(operations):
        bulk_write_calls.append(operations)
    
    mock_db.horses.bulk_write = mock_bulk_write
    mocker.patch.object(rp_module, "db", mock_db)

    get_horse = mocker.patch("src.processors.ratings_processor.get_horse")
    get_horse.return_value = {"_id": "horse_id", "name": "Test Horse"}
    
    gen = rp_module.ratings_processor()
    next(gen)

    for i in range(60):
        gen.send(PreMongoHorse(
            name=f"Horse {i}",
            country="GB",
            year=2020,
            ratings={"flat": 100 + i},
        ))

    gen.close()

    assert len(bulk_write_calls) == 2
    assert len(bulk_write_calls[0]) == 50
    assert len(bulk_write_calls[1]) == 10


def test_ratings_processor_generator_exit_flushes_remaining(mock_db, mocker):
    rp_module = get_ratings_processor_module()
    
    bulk_write_calls = []
    
    def mock_bulk_write(operations):
        bulk_write_calls.append(operations)
    
    mock_db.horses.bulk_write = mock_bulk_write
    mocker.patch.object(rp_module, "db", mock_db)

    get_horse = mocker.patch("src.processors.ratings_processor.get_horse")
    get_horse.return_value = {"_id": "horse_id", "name": "Test Horse"}
    
    gen = rp_module.ratings_processor()
    next(gen)

    for i in range(30):
        gen.send(PreMongoHorse(
            name=f"Horse {i}",
            country="GB",
            year=2020,
            ratings={"flat": 100 + i},
        ))

    gen.close()

    assert len(bulk_write_calls) == 1
    assert len(bulk_write_calls[0]) == 30
