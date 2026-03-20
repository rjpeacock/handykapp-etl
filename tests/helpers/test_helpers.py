import pytest
from pendulum import TUESDAY, parse

from src.helpers.helpers import (
    fetch_content,
    get_last_occurrence_of,
    horse_name_to_pre_mongo_horse,
    log_validation_problem,
)
from src.models import PreMongoHorse

PENDULUM_IMPORT = "src.helpers.helpers.pendulum"


def test_fetch_content_when_successful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.content = "foobar"
    resp.return_value.status_code = 200
    assert fetch_content("https://example.com") == "foobar"


def test_fetch_content_when_unsuccessful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.raise_for_status.side_effect = Exception
    with pytest.raises(Exception):
        fetch_content("https://example.com")


def test_get_last_occurrence_of_when_day_is_tomorrow(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-13")  # A Monday
    assert get_last_occurrence_of(TUESDAY) == parse("2023-02-07").date()


def test_get_last_occurrence_of_when_day_is_today(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-14")  # A Tuesday
    assert get_last_occurrence_of(TUESDAY) == parse("2023-02-14").date()


def test_get_last_occurrence_of_when_day_is_yesterday(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-15")  # A Wednesday
    assert get_last_occurrence_of(TUESDAY) == parse("2023-02-14").date()


def test_log_validation_problem(mocker):
    logger = mocker.patch("src.helpers.helpers.get_run_logger")
    problem = {
        "name": "name_str",
        "field": "year",
        "row": "1",
        "value": "foobar",
        "error": "ValueError",
    }
    log_validation_problem(problem)
    assert logger().warning.called


def test_horse_name_to_pre_mongo_horse_with_basic_name():
    result = horse_name_to_pre_mongo_horse("Sea The Stars (IRE)")
    assert result.name == "SEA THE STARS"
    assert result.country == "IRE"
    assert result.sex is None
    assert result.sire is None


def test_horse_name_to_pre_mongo_horse_with_sex():
    result = horse_name_to_pre_mongo_horse("Frankel (GB)", sex="M")
    assert result.name == "FRANKEL"
    assert result.country == "GB"
    assert result.sex == "M"


def test_horse_name_to_pre_mongo_horse_with_sire():
    sire = PreMongoHorse(name="Galileo", sex="M")
    result = horse_name_to_pre_mongo_horse("Enable (GB)", sex="F", sire=sire)
    assert result.name == "ENABLE"
    assert result.country == "GB"
    assert result.sex == "F"
    assert result.sire.name == "Galileo"
    assert result.sire.sex == "M"


def test_horse_name_to_pre_mongo_horse_with_default_country():
    result = horse_name_to_pre_mongo_horse("Mystery Horse", default_country="FR")
    assert result.name == "MYSTERY HORSE"
    assert result.country == "FR"
