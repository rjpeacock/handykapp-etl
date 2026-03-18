import pytest
from pymongo.errors import DuplicateKeyError

from models import PreMongoPerson
from processors.person_processor import (
    preload_person_cache,
    person_processor,
)


def test_preload_person_cache_returns_empty_for_no_names(mock_db, mocker):
    mocker.patch("processors.person_processor.db", mock_db)
    
    result = preload_person_cache([], "source1")
    
    assert result == {}


def test_preload_person_cache_returns_matching_people(mock_db, mocker):
    mock_db.people.insert_many([
        {"_id": "p1", "references": {"source1": "John Smith"}},
        {"_id": "p2", "references": {"source1": "Jane Doe"}},
        {"_id": "p3", "references": {"source2": "Bob Brown"}},
    ])
    
    mocker.patch("processors.person_processor.db", mock_db)
    
    result = preload_person_cache(["John Smith", "Jane Doe"], "source1")
    
    assert len(result) == 2
    assert ("John Smith", "source1") in result
    assert ("Jane Doe", "source1") in result


def test_person_processor_adds_new_person(mock_db, mocker):
    def mock_insert_one(doc):
        doc["_id"] = "new_person_id"
        return mocker.MagicMock(inserted_id="new_person_id")
    mock_db.people.insert_one = mock_insert_one
    
    mock_find = mocker.MagicMock(return_value=iter([]))
    mock_db.people.find = mock_find

    mocker.patch("processors.person_processor.db", mock_db)
    mocker.patch("processors.person_processor.get_run_logger")

    gen = person_processor()
    next(gen)
    
    person = PreMongoPerson(
        name="New Person",
        role="jockey",
        race_id=None,
        runner_id=None,
    )
    gen.send((person, "source1"))
    gen.close()


def test_person_processor_updates_existing_person(mock_db, mocker):
    mock_db.people.insert_one({
        "_id": "existing_id",
        "first": "John",
        "last": "Smith",
        "references": {},
    })
    
    mocker.patch("processors.person_processor.db", mock_db)
    mocker.patch("processors.person_processor.get_run_logger")

    mock_find = mocker.MagicMock(return_value=iter([
        {"_id": "existing_id", "first": "John", "last": "Smith", "title": None}
    ]))
    mock_db.people.find = mock_find

    mock_update_one = mocker.MagicMock()
    mock_db.people.update_one = mock_update_one

    gen = person_processor()
    next(gen)
    
    person = PreMongoPerson(
        name="John Smith",
        role="jockey",
        race_id=None,
        runner_id=None,
        ratings={"flat": "100"},
    )
    gen.send((person, "source1"))
    gen.close()
    
    mock_update_one.assert_called()


def test_person_processor_handles_duplicate_key_error(mock_db, mocker):
    def mock_insert_one(doc):
        raise DuplicateKeyError("duplicate key")
    mock_db.people.insert_one = mock_insert_one

    mock_find = mocker.MagicMock(return_value=iter([]))
    mock_db.people.find = mock_find

    mocker.patch("processors.person_processor.db", mock_db)
    mocker.patch("processors.person_processor.get_run_logger")

    gen = person_processor()
    next(gen)
    
    person = PreMongoPerson(
        name="Duplicate Person",
        role="jockey",
    )
    gen.send((person, "source1"))
    gen.close()
