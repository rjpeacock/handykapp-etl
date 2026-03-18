import pytest
import importlib

rp_module = importlib.import_module("processors.runner_processor")


def test_make_runner_dict(mocker):
    horse = mocker.MagicMock()
    horse._id = "horse_1"
    
    result = rp_module.make_runner_dict(horse, "horse_1")
    
    assert result["horse"] == "horse_1"


def test_collect_people_with_trainer_and_jockey(mocker):
    horse = mocker.MagicMock()
    horse.trainer = "John Smith"
    horse.jockey = "Jane Doe"
    
    result = rp_module.collect_people(horse, "race_1", "runner_1", "source1")
    
    assert len(result) == 2
    assert result[0][0].name == "John Smith"
    assert result[0][0].role == "trainer"
    assert result[1][0].name == "Jane Doe"
    assert result[1][0].role == "jockey"


def test_collect_people_with_only_jockey(mocker):
    horse = mocker.MagicMock()
    horse.trainer = None
    horse.jockey = "Jane Doe"
    
    result = rp_module.collect_people(horse, "race_1", "runner_1", "source1")
    
    assert len(result) == 1
    assert result[0][0].name == "Jane Doe"
    assert result[0][0].role == "jockey"
