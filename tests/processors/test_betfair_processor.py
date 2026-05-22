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


def test_betfair_price_processor_accepts_record(mocker):
    from models.betfair_price_record import BetfairPriceRecord

    mocker.patch("processors.betfair_processor.get_run_logger")

    gen = betfair_price_processor()
    next(gen)

    rec = BetfairPriceRecord(
        event_id="1",
        course_and_date="Southwell 21st May",
        event_name="6f Hcap",
        race_datetime=pendulum.datetime(2026, 5, 21, 21, 0, 0),
        selection_id="42",
        horse_name="Albert Cee",
        win=True,
        bsp="9.5",
        pre_play_wap="8.2",
        morning_wap="10.3",
        pre_play_max="11.0",
        pre_play_min="8.8",
        in_play_max="2.0",
        in_play_min="1.0",
        morning_traded_volume="266.8",
        pre_play_traded_volume="11707.9",
        in_play_traded_volume="21136.7",
    )
    gen.send(rec)
    gen.close()
