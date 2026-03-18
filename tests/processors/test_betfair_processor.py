import pytest
from pymongo.errors import DuplicateKeyError
import pendulum

from models.mongo_betfair_horserace_pnl import MongoBetfairHorseracePnl
from processors.betfair_processor import betfair_processor


def test_betfair_processor_inserts_pnl(mock_db, mocker):
    mocker.patch("processors.betfair_processor.db", mock_db)
    mocker.patch("processors.betfair_processor.get_run_logger", return_value=mocker.MagicMock())

    gen = betfair_processor()
    next(gen)
    
    pnl = MongoBetfairHorseracePnl(
        racecourse="Ascot",
        race_datetime=pendulum.datetime(2024, 1, 1, 14, 0, 0),
        profit_loss=100.5,
    )
    gen.send(pnl)
    gen.close()

    assert mock_db.betfair.count_documents({}) == 1
    result = mock_db.betfair.find_one()
    assert result["racecourse"] == "Ascot"
    assert result["profit_loss"] == 100.5


def test_betfair_processor_handles_duplicate_key_error(mock_db, mocker):
    mock_insert_one = mocker.patch.object(mock_db.betfair, "insert_one")
    mock_insert_one.side_effect = DuplicateKeyError("duplicate key")
    mocker.patch("processors.betfair_processor.db", mock_db)
    mocker.patch("processors.betfair_processor.get_run_logger", return_value=mocker.MagicMock())

    gen = betfair_processor()
    next(gen)
    
    pnl = MongoBetfairHorseracePnl(
        racecourse="Ascot",
        race_datetime=pendulum.datetime(2024, 1, 1, 14, 0, 0),
        profit_loss=100.5,
    )
    gen.send(pnl)
    gen.close()

    assert mock_db.betfair.count_documents({}) == 0
