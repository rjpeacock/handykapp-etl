import pendulum
import pytest

from loaders.betfair_loader import generate_url, is_flat_race


@pytest.mark.parametrize(
    ("event_name", "expected"),
    [
        ("6f Hcap", True),
        ("1m2f Stks", True),
        ("5f Mdn Stks", True),
        ("1m Hcap", True),
        ("2m Hrd", False),
        ("3m1f Hcap Chs", False),
        ("2m XC", False),
        ("2m NHF", False),
        ("2m Hrd Hcap", False),
        ("hrd", False),
        ("chs", False),
        ("xc", False),
        ("nhf", False),
    ],
)
def test_is_flat_race(event_name, expected):
    assert is_flat_race(event_name) is expected


def test_generate_url():
    url = generate_url("ire", "place", pendulum.date(2026, 5, 22))
    assert url == "https://promo.betfair.com/betfairsp/prices/dwbfpricesireplace22052026.csv"


def test_load_betfair_prices(mock_db, mocker):
    mocker.patch("loaders.betfair_loader.get_run_logger")
    mocker.patch("loaders.betfair_loader.db", mock_db)
    mocker.patch("loaders.betfair_loader.get_last_load", return_value=None)
    mocker.patch("loaders.betfair_loader.update_load")
    mocker.patch("processors.betfair_processor.get_run_logger")
    mocker.patch("processors.betfair_processor.db", mock_db)

    csv_content = (
        "EVENT_ID,MENU_HINT,EVENT_NAME,EVENT_DT,SELECTION_ID,SELECTION_NAME,"
        "WIN_LOSE,BSP,PPWAP,MORNINGWAP,PPMAX,PPMIN,IPMAX,IPMIN,"
        "MORNINGTRADEDVOL,PPTRADEDVOL,IPTRADEDVOL\n"
        "1,Southwell 21st May,6f Hcap,21-05-2026 21:00,42,Albert Cee,1,9.5,"
        "8.2,10.3,11.0,8.8,2.0,1.0,266.8,11707.9,21136.7\n"
        "2,Cheltenham 15th Mar,2m Hrd,15-03-2026 14:00,99,Jump King,1,4.5,"
        "4.2,4.8,5.0,4.0,1.5,1.0,500.0,10000.0,15000.0\n"
    )

    mock_fetch = mocker.patch(
        "loaders.betfair_loader.fetch_content",
        return_value=csv_content.encode("utf-8"),
    )

    from loaders.betfair_loader import load_betfair_prices

    load_betfair_prices(
        countries=["uk"],
        market_types=["win"],
        start_date=pendulum.date(2026, 5, 22),
        end_date=pendulum.date(2026, 5, 22),
    )

    assert mock_fetch.call_count == 1
