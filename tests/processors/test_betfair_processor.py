import pytest
from pymongo.errors import DuplicateKeyError
import importlib
import sys


def get_betfair_processor_module():
    if "src.processors.betfair_processor" in sys.modules:
        del sys.modules["src.processors.betfair_processor"]
    return importlib.import_module("src.processors.betfair_processor")


class MockPnl:
    def __init__(self, racecourse, race_datetime, pnl, selections=None):
        self.racecourse = racecourse
        self.race_datetime = race_datetime
        self.pnl = pnl
        self.selections = selections or []
    
    def model_dump(self):
        return {
            "racecourse": self.racecourse,
            "race_datetime": self.race_datetime,
            "pnl": self.pnl,
            "selections": self.selections,
        }


def test_betfair_processor_inserts_pnl(mock_db, mocker):
    bp_module = get_betfair_processor_module()
    mocker.patch.object(bp_module, "db", mock_db)

    gen = bp_module.betfair_processor()
    next(gen)
    
    pnl = MockPnl(
        racecourse="Ascot",
        race_datetime="2024-01-01T14:00:00",
        pnl=100.5,
        selections=[{"name": "Horse 1", "pnl": 50.0}],
    )
    gen.send(pnl)
    gen.close()

    assert mock_db.betfair.count_documents({}) == 1
    result = mock_db.betfair.find_one()
    assert result["racecourse"] == "Ascot"
    assert result["pnl"] == 100.5


def test_betfair_processor_handles_duplicate_key_error(mock_db, mocker):
    bp_module = get_betfair_processor_module()
    
    mock_insert_one = mocker.patch.object(mock_db.betfair, "insert_one")
    mock_insert_one.side_effect = DuplicateKeyError("duplicate key")
    
    mocker.patch.object(bp_module, "db", mock_db)

    pnl = MockPnl(
        racecourse="Ascot",
        race_datetime="2024-01-01T14:00:00",
        pnl=100.5,
    )

    gen = bp_module.betfair_processor()
    next(gen)
    gen.send(pnl)
    gen.close()

    assert mock_db.betfair.count_documents({}) == 0
