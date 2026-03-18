import pytest
import importlib
from pendulum import DateTime

from processors.race_processor import make_update_dictionary


def test_make_update_dictionary(mock_db, mocker):
    mocker.patch("processors.race_processor.db", mock_db)
    
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
    
    result = make_update_dictionary(race, "course_1")
    
    assert result["racecourse"] == "course_1"
    assert result["title"] == "1:00 Ascot"
    assert result["is_handicap"] is False
