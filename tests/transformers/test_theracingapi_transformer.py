import pendulum
import pytest

from models import PreMongoHorse, PreMongoRace, PreMongoRunner
from models.theracingapi_racecard import TheRacingApiRacecard
from models.theracingapi_runner import TheRacingApiRunner
from transformers.theracingapi_transformer import (
    build_datetime,
    transform_horse,
    transform_races,
)


@pytest.fixture
def horse_1_data():
    return TheRacingApiRunner(
        **{
            "horse": "Hortzadar",
            "age": "8",
            "sex": "gelding",
            "sex_code": "G",
            "colour": "b",
            "region": "GB",
            "dam": "Clouds Of Magellan",
            "sire": "Sepoy",
            "damsire": "Dynaformer",
            "trainer": "David Omeara",
            "owner": "Akela Thoroughbreds Limited",
            "number": "1",
            "draw": "4",
            "headgear": "",
            "lbs": "141",
            "ofr": "76",
            "jockey": "Mark Winn",
            "last_run": "12",
            "form": "476601",
        }
    )


@pytest.fixture
def horse_2_data():
    return TheRacingApiRunner(
        **{
            "horse": "Tele Red",
            "age": "6",
            "sex": "filly",
            "sex_code": "F",
            "colour": "b",
            "region": "GB",
            "dam": "Hardy Blue",
            "sire": "Telescope",
            "damsire": "Red Clubs",
            "trainer": "K R Burke",
            "owner": "John Kenny",
            "number": "2",
            "draw": "7",
            "headgear": "bl",
            "lbs": "141",
            "ofr": "76",
            "jockey": "Brandon Wilkie(5)",
            "last_run": "12",
            "form": "113246",
        }
    )


@pytest.fixture
def horse_3_data():
    return TheRacingApiRunner(
        **{
            "horse": "Spycatcher",
            "age": "5",
            "sex": "horse",
            "sex_code": "H",
            "colour": "b",
            "region": "IRE",
            "dam": "Damask",
            "sire": "Vadamos",
            "damsire": "Red Clubs",
            "trainer": "K R Burke",
            "owner": "Highclere Tbredracing-Adriana Zaefferer",
            "number": "6",
            "draw": "5",
            "headgear": "",
            "lbs": "130",
            "ofr": "106",
            "jockey": "Pierre-Louis Jamin(3)",
            "last_run": "301",
            "form": "13270-",
        },
    )


@pytest.fixture
def racecard_data(horse_1_data, horse_2_data):
    return TheRacingApiRacecard(
        **{
            "course": "Wolverhampton (AW)",
            "date": "2023-10-03",
            "off_time": "1:42",
            "race_name": "Virgin Bet Apprentice Handicap",
            "distance_f": "8.0",
            "region": "GB",
            "pattern": "",
            "race_class": "Class 5",
            "type": "Flat",
            "age_band": "3yo+",
            "rating_band": "0-75",
            "prize": "£4,187",
            "field_size": "7",
            "going": "Soft",
            "surface": "AW",
            "runners": [
                horse_1_data,
                horse_2_data,
            ],
        }
    )


def test_build_datetime_with_morning_race():
    assert build_datetime("2023-10-03", "11:15") == "2023-10-03T11:15:00+00:00"


def test_build_datetime_with_afternoon_race():
    assert build_datetime("2023-10-03", "3:15") == "2023-10-03T15:15:00+00:00"


def test_build_datetime_with_evening_race():
    assert build_datetime("2023-10-03", "9:15") == "2023-10-03T21:15:00+00:00"


def test_build_datetime_with_minutes_below_ten():
    assert build_datetime("2023-10-03", "3:05") == "2023-10-03T15:05:00+00:00"


def test_transform_horse_returns_correct_output_when_professional_jockey(
    horse_1_data, mocker
):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    sire = PreMongoHorse(name="SEPOY", sex="M")
    damsire = PreMongoHorse(name="DYNAFORMER", sex="M")
    dam = PreMongoHorse(name="CLOUDS OF MAGELLAN", sex="F", sire=damsire)
    expected = PreMongoRunner(
        name="HORTZADAR",
        sex="M",
        is_gelded=True,
        country="GB",
        year=2015,
        colour="Bay",
        sire=sire,
        dam=dam,
        damsire=damsire,
        trainer="David Omeara",
        owner="Akela Thoroughbreds Limited",
        jockey="Mark Winn",
        allowance=0,
        saddlecloth=1,
        draw=4,
        headgear=None,
        lbs_carried=141,
        official_rating=76,
        non_runner=False,
    )
    actual = transform_horse(horse_1_data, pendulum.parse("2023-10-03"))
    assert actual == expected


def test_transform_horse_returns_correct_output_when_apprentice_jockey(
    horse_2_data, mocker
):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    sire = PreMongoHorse(name="TELESCOPE", sex="M")
    damsire = PreMongoHorse(name="RED CLUBS", sex="M")
    dam = PreMongoHorse(name="HARDY BLUE", sex="F", sire=damsire)
    expected = PreMongoRunner(
        name="TELE RED",
        sex="F",
        country="GB",
        year=2017,
        colour="Bay",
        sire=sire,
        dam=dam,
        damsire=damsire,
        trainer="K R Burke",
        owner="John Kenny",
        jockey="Brandon Wilkie",
        allowance=5,
        saddlecloth=2,
        draw=7,
        headgear="Blinkers",
        lbs_carried=141,
        official_rating=76,
        non_runner=False,
    )
    actual = transform_horse(horse_2_data, pendulum.parse("2023-10-03"))
    assert actual == expected


def test_transform_horse_returns_correct_output_when_entire(horse_3_data, mocker):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    sire = PreMongoHorse(name="VADAMOS", sex="M")
    damsire = PreMongoHorse(name="RED CLUBS", sex="M")
    dam = PreMongoHorse(name="DAMASK", sex="F", sire=damsire)
    expected = PreMongoRunner(
        name="SPYCATCHER",
        sex="M",
        is_gelded=False,
        country="IRE",
        year=2018,
        colour="Bay",
        sire=sire,
        dam=dam,
        damsire=damsire,
        trainer="K R Burke",
        owner="Highclere Tbredracing-Adriana Zaefferer",
        jockey="Pierre-Louis Jamin",
        allowance=3,
        saddlecloth=6,
        draw=5,
        headgear=None,
        lbs_carried=130,
        official_rating=106,
        non_runner=False,
    )
    actual = transform_horse(horse_3_data, pendulum.parse("2023-10-03"))
    assert actual == expected


def test_transform_horse_returns_correct_output_when_trainer_is_from_ireland(
    horse_3_data, mocker
):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    horse_3_data.trainer = "Paddy O'Reilly, Ireland"
    sire = PreMongoHorse(name="VADAMOS", sex="M")
    damsire = PreMongoHorse(name="RED CLUBS", sex="M")
    dam = PreMongoHorse(name="DAMASK", sex="F", sire=damsire)
    expected = PreMongoRunner(
        name="SPYCATCHER",
        sex="M",
        is_gelded=False,
        country="IRE",
        year=2018,
        colour="Bay",
        sire=sire,
        dam=dam,
        damsire=damsire,
        trainer="Paddy O'Reilly",
        owner="Highclere Tbredracing-Adriana Zaefferer",
        jockey="Pierre-Louis Jamin",
        allowance=3,
        saddlecloth=6,
        draw=5,
        headgear=None,
        lbs_carried=130,
        official_rating=106,
        non_runner=False,
    )
    actual = transform_horse(horse_3_data, pendulum.parse("2023-10-03"))
    assert actual == expected


def test_transform_horse_with_nr_saddlecloth_sets_non_runner_true(mocker):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    nr_horse = TheRacingApiRunner(
        **{
            "horse": "Non Runner",
            "age": "5",
            "sex": "gelding",
            "sex_code": "G",
            "colour": "b",
            "region": "GB",
            "dam": "Some Dam",
            "sire": "Some Sire",
            "damsire": "Some Damsire",
            "trainer": "A Trainer",
            "owner": "An Owner",
            "number": "NR",
            "draw": "",
            "headgear": "",
            "lbs": "",
            "ofr": "-",
            "jockey": "A Jockey",
            "last_run": "",
            "form": None,
        }
    )
    actual = transform_horse(nr_horse, pendulum.parse("2023-10-03"))
    assert actual.non_runner is True
    assert actual.saddlecloth is None


def test_transform_races_returns_correct_output(racecard_data):
    expected = PreMongoRace(
        course="Wolverhampton",
        surface="AW",
        code="Flat",
        datetime="2023-10-03T13:42:00+00:00",
        title="Virgin Bet Apprentice Handicap",
        is_handicap=True,
        obstacle=None,
        distance_description="1m",
        race_grade=None,
        race_class=5,
        age_restriction="3yo+",
        rating_restriction="0-75",
        going_description="Soft",
        prize="£4187",
    )

    actual = transform_races(racecard_data)[0]

    assert len(actual.runners) == 2

    assert actual.model_dump(exclude={"runners"}) == expected.model_dump(
        exclude={"runners"}
    )
