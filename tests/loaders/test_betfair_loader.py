import pendulum

from loaders.betfair_loader import generate_url


def test_generate_url():
    url = generate_url("ire", "place", pendulum.date(2026, 5, 22))
    assert url == "https://promo.betfair.com/betfairsp/prices/dwbfpricesireplace22052026.csv"


def test_load_betfair_horserace_prices(mock_db, mocker):
    mocker.patch("loaders.betfair_loader.get_run_logger")
    mocker.patch("loaders.betfair_loader.db", mock_db)
    mocker.patch("loaders.betfair_loader.get_last_load", return_value=None)
    mocker.patch("loaders.betfair_loader.update_load")
    mocker.patch("processors.betfair_processor.get_run_logger")
    mocker.patch("processors.betfair_processor.db", mock_db)

    csv_content = (
        "event_id,menu_hint,event_name,event_dt,selection_id,selection_name,"
        "win_lose,bsp,ppwap,morningwap,ppmax,ppmin,ipmax,ipmin,"
        "morningtradedvol,pptradedvol,iptradedvol\n"
        "1,Southwell 21st May,6f Hcap,21-05-2026 21:00,42,Albert Cee,1,9.5,"
        "8.2,10.3,11.0,8.8,2.0,1.0,266.8,11707.9,21136.7\n"
    )

    mock_fetch = mocker.patch(
        "loaders.betfair_loader.fetch_content",
        return_value=csv_content.encode("utf-8"),
    )

    from loaders.betfair_loader import load_betfair_horserace_prices

    load_betfair_horserace_prices(
        countries=["uk"],
        market_types=["win"],
        start_date=pendulum.date(2026, 5, 22),
        end_date=pendulum.date(2026, 5, 22),
    )

    assert mock_fetch.call_count == 1
