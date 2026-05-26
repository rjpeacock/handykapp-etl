import pendulum
import pytest

from models import PreMongoHorse, PreMongoRace, PreMongoRunner
from models.rapid_record import RapidRecord
from models.rapid_runner import RapidRunner
from transformers.rapid_horseracing_transformer import (
    standardise_name,
    transform_horse,
    transform_results,
    transform_to_entries,
)


@pytest.fixture
def horse_data():
    return RapidRunner(
        **{
            "horse": "Dobbin(IRE)",
            "id_horse": "123456",
            "jockey": "A Jockey",
            "trainer": "A Trainer",
            "age": "3",
            "weight": "10-0",
            "number": "1",
            "last_ran_days_ago": "1",
            "non_runner": "0",
            "form": "1-2-3",
            "position": "1",
            "distance_beaten": "1 1/2",
            "owner": "A Owner",
            "sire": "THE SIRE",
            "dam": "THE DAM(FR)",
            "OR": "",
            "sp": "8",
            "odds": [],
        }
    )


@pytest.fixture
def result_data(horse_data):
    return RapidRecord(
        **{
            "id_race": "123456",
            "course": "Lucksin Downs",
            "date": "2023-03-08 16:00:00",
            "title": "LUCKSIN HANDICAP (5)",
            "distance": "1m2f",
            "age": "3",
            "going": "Soft (Good to Soft in places)",
            "finished": "1",
            "canceled": "0",
            "finish_time": "",
            "prize": "\u00a32794",
            "class": "5",
            "horses": [horse_data.model_dump()],
        }
    )


@pytest.fixture
def expected_sire():
    return PreMongoHorse(
        name="THE SIRE",
        country="GB",
        sex="M",
    )


@pytest.fixture
def expected_dam():
    return PreMongoHorse(
        name="THE DAM",
        country="FR",
        sex="F",
    )


@pytest.fixture
def expected_runner(expected_sire, expected_dam):
    return PreMongoRunner(
        name="DOBBIN",
        country="IRE",
        year=2020,
        rapid_id="123456",
        jockey="A Jockey",
        trainer="A Trainer",
        lbs_carried=140,
        saddlecloth="1",
        finishing_position="1",
        official_position="1",
        beaten_distance=1.5,
        owner="A Owner",
        sire=expected_sire,
        dam=expected_dam,
        official_rating=None,
        sp="8",
        time=None,
        non_runner=False,
    )


@pytest.fixture
def expected_entry(expected_sire, expected_dam):
    return PreMongoRunner(
        name="DOBBIN",
        country="IRE",
        year=2020,
        rapid_id="123456",
        jockey="A Jockey",
        trainer="A Trainer",
        lbs_carried=140,
        saddlecloth="1",
        owner="A Owner",
        sire=expected_sire,
        dam=expected_dam,
        official_rating=None,
    )


def test_standardise_name_non_runner_hyphenated_returns_empty_string():
    assert standardise_name("non-runner") == ""


def test_standardise_name_non_runner_with_space_returns_empty_string():
    assert standardise_name("Non Runner") == ""


def test_standardise_name_irish_obrien_converted_to_o_brien():
    assert standardise_name("A P Obrien, Ireland") == "A P O'Brien"


def test_standardise_name_non_irish_owen_not_converted():
    assert standardise_name("Owen") == "Owen"


def test_transform_horse_propagates_non_runner_true(horse_data):
    horse_data.non_runner = "1"
    actual = transform_horse(horse_data, pendulum.parse("2023-03-08"))
    assert actual.non_runner is True


def test_transform_horse_propagates_non_runner_false(horse_data):
    actual = transform_horse(horse_data, pendulum.parse("2023-03-08"))
    assert actual.non_runner is False


def test_transform_horse_returns_correct_output(horse_data, expected_runner):
    actual = transform_horse(horse_data, pendulum.parse("2023-03-08"))
    assert actual == expected_runner


def test_transform_results_returns_correct_output(result_data, expected_runner):
    expected = PreMongoRace(
        rapid_id="123456",
        course="Lucksin Downs",
        datetime="2023-03-08T16:00:00+00:00",
        title="LUCKSIN HANDICAP (5)",
        is_handicap=True,
        obstacle=None,
        surface="Turf",
        code="Flat",
        distance_description="1m2f",
        age_restriction="3",
        going_description="Soft (Good to Soft in places)",
        prize="\u00a32794",
        race_class="5",
        runners=[expected_runner],
    )
    actual = transform_results(result_data)[0]
    assert actual == expected


def test_transform_to_entries_returns_correct_output(result_data, expected_entry):
    expected = PreMongoRace(
        rapid_id="123456",
        course="Lucksin Downs",
        datetime="2023-03-08T16:00:00+00:00",
        title="LUCKSIN HANDICAP (5)",
        is_handicap=True,
        obstacle=None,
        surface="Turf",
        code="Flat",
        distance_description="1m2f",
        age_restriction="3",
        going_description="Soft (Good to Soft in places)",
        prize="\u00a32794",
        race_class="5",
        runners=[expected_entry],
    )

    actual = transform_to_entries(result_data)[0]
    assert actual == expected
