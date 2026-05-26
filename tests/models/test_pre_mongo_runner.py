import pytest
from pydantic import ValidationError

from models.pre_mongo_runner import PreMongoRunner


def test_pre_mongo_runner_default_non_runner_is_none():
    runner = PreMongoRunner(name="Test Horse", country="GB", year=2020)
    assert runner.non_runner is None


def test_pre_mongo_runner_with_non_runner_true():
    runner = PreMongoRunner(name="Test Horse", country="GB", year=2020, non_runner=True)
    assert runner.non_runner is True


def test_pre_mongo_runner_with_non_runner_false():
    runner = PreMongoRunner(
        name="Test Horse", country="GB", year=2020, non_runner=False
    )
    assert runner.non_runner is False


def test_pre_mongo_runner_non_runner_accepts_bool():
    runner = PreMongoRunner(name="Test Horse", country="GB", year=2020, non_runner=True)
    assert runner.non_runner is True
