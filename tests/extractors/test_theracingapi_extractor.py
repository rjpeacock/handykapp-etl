import pendulum

from src.extractors.theracingapi_extractor import (
    DESTINATION,
    SOURCE,
    extract_countries,
    extract_racecards,
    get_headers,
    identify_missing_dates,
)


def test_theracingapi_source():
    expected = "https://the-racing-api1.p.rapidapi.com/v1/"
    assert expected == SOURCE


def test_theracingapi_destination():
    expected = "handykapp/theracingapi/"
    assert expected == DESTINATION


def test_get_headers(mocker):
    mocker.patch(
        "src.extractors.theracingapi_extractor.Secret.load"
    ).return_value.get.return_value = "<key>"
    headers = get_headers()
    assert headers["x-rapidapi-host"] == "the-racing-api1.p.rapidapi.com"
    assert headers["x-rapidapi-key"] == "<key>"


def test_extract_countries(mocker):
    mocker.patch("src.extractors.theracingapi_extractor.get_headers").return_value = {
        "x-rapidapi-host": "the-racing-api1.p.rapidapi.com",
        "x-rapidapi-key": "mock_key",
    }
    mocker.patch("src.extractors.theracingapi_extractor.fetch_content").return_value = {
        "name": "foobaristan"
    }
    assert extract_countries.fn() == {"name": "foobaristan"}


def test_extract_racecards_for_tomorrow_as_default(mocker):
    write_file = mocker.patch("src.extractors.theracingapi_extractor.SpacesClient.write_file")
    mocker.patch("src.extractors.theracingapi_extractor.get_headers").return_value = {
        "x-rapidapi-host": "the-racing-api1.p.rapidapi.com",
        "x-rapidapi-key": "mock_key",
    }
    mocker.patch("src.extractors.theracingapi_extractor.fetch_content").return_value = [
        {}
    ]
    mocker.patch("src.extractors.theracingapi_extractor.DESTINATION", "dir/")
    mocker.patch("pendulum.now").return_value = pendulum.parse("2020-01-01")

    extract_racecards.fn()

    expected_destination = "dir/racecards/theracingapi_racecards_20200102.json"
    assert write_file.call_count == 1
    assert mocker.call([{}], expected_destination) == write_file.call_args


def test_identify_missing_dates(mocker):
    mocker.patch("src.extractors.theracingapi_extractor.get_run_logger")
    get_files = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.get_files"
    )
    write_file = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.write_file"
    )
    mocker.patch("src.extractors.theracingapi_extractor.DESTINATION", "dir/")

    get_files.return_value = [
        "dir/racecards/theracingapi_racecards_20260101.json",
        "dir/racecards/theracingapi_racecards_20260102.json",
        "dir/racecards/theracingapi_racecards_20260104.json",
    ]

    identify_missing_dates.fn()

    write_file.assert_called_once_with(
        "20260103\n",
        "dir/missing_racecard_dates.txt",
    )


def test_identify_missing_dates_no_gaps(mocker):
    mocker.patch("src.extractors.theracingapi_extractor.get_run_logger")
    get_files = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.get_files"
    )
    delete_file = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.delete_file"
    )
    mocker.patch("src.extractors.theracingapi_extractor.DESTINATION", "dir/")

    get_files.return_value = [
        "dir/racecards/theracingapi_racecards_20260101.json",
        "dir/racecards/theracingapi_racecards_20260102.json",
        "dir/racecards/theracingapi_racecards_20260103.json",
    ]

    identify_missing_dates.fn()

    delete_file.assert_called_once_with("dir/missing_racecard_dates.txt")


def test_identify_missing_dates_single_file(mocker):
    mocker.patch("src.extractors.theracingapi_extractor.get_run_logger")
    get_files = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.get_files"
    )
    delete_file = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.delete_file"
    )
    mocker.patch("src.extractors.theracingapi_extractor.DESTINATION", "dir/")

    get_files.return_value = [
        "dir/racecards/theracingapi_racecards_20260101.json",
    ]

    identify_missing_dates.fn()

    delete_file.assert_called_once_with("dir/missing_racecard_dates.txt")


def test_identify_missing_dates_empty_dir(mocker):
    mocker.patch("src.extractors.theracingapi_extractor.get_run_logger")
    get_files = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.get_files"
    )
    write_file = mocker.patch(
        "src.extractors.theracingapi_extractor.SpacesClient.write_file"
    )
    mocker.patch("src.extractors.theracingapi_extractor.DESTINATION", "dir/")

    get_files.return_value = []

    identify_missing_dates.fn()

    write_file.assert_not_called()
