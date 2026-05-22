import pendulum
import pytest

from src.transformers.betfair_transformer import (
    betfair_price_transformer,
    get_places_from_place_detail,
    transform_betfair_pnl_data,
    validate_betfair_pnl_data,
)


def test_betfair_price_transformer():
    from decimal import Decimal

    row = {
        "event_id": "258376466",
        "menu_hint": "Southwell 21st May",
        "event_name": "6f Hcap",
        "event_dt": "21-05-2026 21:00",
        "selection_id": "47164177",
        "selection_name": "Albert Cee",
        "win_lose": "1",
        "bsp": "9.51597833",
        "ppwap": "8.2376",
        "morningwap": "10.2947",
        "ppmax": "11.00",
        "ppmin": "8.80",
        "ipmax": "2.02",
        "ipmin": "1.01",
        "morningtradedvol": "266.84",
        "pptradedvol": "11707.88",
        "iptradedvol": "21136.66",
    }
    record = betfair_price_transformer(row)
    assert record.horse_name == "Albert Cee"
    assert record.win is True
    assert record.bsp == Decimal("9.51597833")
    assert record.race_datetime == pendulum.datetime(2026, 5, 21, 21, 0, 0)


@pytest.fixture
def mock_data():
    return [
        row.split(",")
        for row in [
            "\ufeffMarket,Start time,Settled date,Profit/Loss (£)",
            "Horse Racing / Brighton 30th Apr : 1m2f Hcap,30-Apr-24 16:10,30-Apr-24 16:13,78.60"
        ]
    ]

def test_get_places_from_place_detail_when_tbp_in_detail():
    actual = get_places_from_place_detail("3 TBP")
    assert actual == 3

def test_get_places_from_place_detail_when_tbp_not_in_detail():
    actual = get_places_from_place_detail("Places")
    assert actual is None

def test_get_places_from_place_detail_when_tbp_has_no_value():
    actual = get_places_from_place_detail("TBP")
    assert actual is None

def test_transform_betfair_pnl_data_returns_correct_output(mock_data):
    actual = transform_betfair_pnl_data.fn(mock_data)[0]
    
    assert actual.racecourse == "Brighton"
    assert actual.race_datetime == pendulum.datetime(2024, 4, 30, 16, 10, 0, tz="UTC")
    assert actual.profit_loss == pytest.approx(78.60, 0.01)
    assert actual.places == 1
    assert actual.race_description == "1m2f Hcap"


def test_validate_betfair_pnl_data_returns_no_problems_for_correct_data(mock_data):
    problems = validate_betfair_pnl_data.fn(mock_data)
    assert len(problems.dicts()) == 0


def test_validate_betfair_pnl_data_returns_problems_for_incorrect_data(mock_data):
    mock_data[1][3] = "Lots of cash"
    problems = validate_betfair_pnl_data.fn(mock_data)
    assert len(problems.dicts()) == 1
    assert problems.dicts()[0]["field"] == "Profit/Loss (£)"
