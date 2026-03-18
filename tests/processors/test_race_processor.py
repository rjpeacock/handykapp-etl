import pytest
from pendulum import DateTime
import importlib
import sys


def get_race_processor_module():
    if "src.processors.race_processor" in sys.modules:
        del sys.modules["src.processors.race_processor"]
    return importlib.import_module("src.processors.race_processor")


def test_make_update_dictionary(mock_db, mocker):
    rp_module = get_race_processor_module()
    mocker.patch.object(rp_module, "db", mock_db)
    
    race = mocker.MagicMock()
    race.datetime = DateTime(2024, 1, 1, 14, 0, 0)
    race.title = "1:00 Ascot"
    race.is_handicap = False
    race.distance_description = "1 mile"
    race.going_description = "Good"
    race.race_grade = "Class 1"
    race.race_class = "1"
    race.age_restriction = "3yo+"
    race.rating_restriction = None
    race.prize = 10000
    race.rapid_id = "12345"
    
    result = rp_module.make_update_dictionary(race, "course_1")
    
    assert result["racecourse"] == "course_1"
    assert result["title"] == "1:00 Ascot"
    assert result["is_handicap"] is False
