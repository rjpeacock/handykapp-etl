import pendulum
from pymongo.errors import DuplicateKeyError

from models.mongo_betfair_horserace_pnl import MongoBetfairHorseracePnl
from processors.betfair_processor import betfair_pnl_processor, betfair_price_processor


def test_betfair_processor_inserts_pnl(mock_db, mocker):
    mocker.patch("processors.betfair_processor.db", mock_db)
    mocker.patch("processors.betfair_processor.get_run_logger")

    gen = betfair_pnl_processor()
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
    mocker.patch("processors.betfair_processor.get_run_logger")

    gen = betfair_pnl_processor()
    next(gen)

    pnl = MongoBetfairHorseracePnl(
        racecourse="Ascot",
        race_datetime=pendulum.datetime(2024, 1, 1, 14, 0, 0),
        profit_loss=100.5,
    )
    gen.send(pnl)
    gen.close()

    assert mock_db.betfair.count_documents({}) == 0


def test_betfair_price_processor_accepts_record(mock_db, mocker):
    from models.betfair_price_record import BetfairPriceRecord

    mocker.patch("processors.betfair_processor.db", mock_db)
    mocker.patch("processors.betfair_processor.get_run_logger")

    gen = betfair_price_processor()
    next(gen)

    rec = BetfairPriceRecord.model_validate({
        "EVENT_ID": "1",
        "MENU_HINT": "Southwell 21st May",
        "EVENT_NAME": "6f Hcap",
        "EVENT_DT": "21-05-2026 21:00",
        "SELECTION_ID": "42",
        "SELECTION_NAME": "Albert Cee",
        "WIN_LOSE": "1",
        "BSP": "9.5",
        "PPWAP": "8.2",
        "MORNINGWAP": "10.3",
        "PPMAX": "11.0",
        "PPMIN": "8.8",
        "IPMAX": "2.0",
        "IPMIN": "1.0",
        "MORNINGTRADEDVOL": "266.8",
        "PPTRADEDVOL": "11707.9",
        "IPTRADEDVOL": "21136.7",
    })
    rec.country = "uk"
    rec.market_type = "win"
    gen.send(rec)
    gen.close()
