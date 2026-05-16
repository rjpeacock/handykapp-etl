import pendulum
from horsetalk import RacingCode

from models import FormdataRun
from transformers.formdata_transformer import (
    adjust_rr_name,
    extract_dist_going,
    extract_middle_details,
    extract_prize,
    extract_rating,
    extract_weight,
    get_formdata_date,
    get_formdatas,
    is_horse,
    is_race_date,
    transform_run,
)

FORMDATA_FETCH = "transformers.formdata_transformer.SpacesClient.get_files"
FORMDATA_FILENAMES = [
    "formdata_190630.pdf",
    "formdata_200628.pdf",
    "formdata_200705.pdf",
    "formdata_210530.pdf",
    "formdata_220529.pdf",
    "formdata_230528.pdf",
    "formdata_230604.pdf",
    "formdata_230611.pdf",
    "formdata_230618.pdf",
    "formdata_230625.pdf",
    "formdata_230702.pdf",
    "formdata_nh_230611.pdf",
    "formdata_nh_230618.pdf",
    "formdata_nh_230625.pdf",
    "formdata_nh_230702.pdf",
]


def test_adjust_rr_name_on_basic_name():
    assert adjust_rr_name("JSmith") == "J Smith"


def test_adjust_rr_name_when_first_name_spelt_out():
    assert adjust_rr_name("JohnSmith") == "John Smith"


def test_adjust_rr_name_when_middle_initial_given():
    assert adjust_rr_name("JFSmith") == "J F Smith"


def test_adjust_rr_name_when_country_given():
    assert adjust_rr_name("JSmith (IRE)") == "J Smith (IRE)"


def test_adjust_rr_name_when_title_given():
    assert adjust_rr_name("MsJSmith") == "Ms J Smith"


def test_adjust_rr_name_when_irish():
    assert adjust_rr_name("JO'Smith") == "J O'Smith"


def test_adjust_rr_name_when_scottish():
    assert adjust_rr_name("JMcSmith") == "J McSmith"


def test_extract_dist_going_for_turf_going():
    assert extract_dist_going("5G") == (float("5"), "G")


def test_extract_dist_going_for_aw_going():
    assert extract_dist_going("5d") == (float("5"), "d")


def test_extract_dist_going_for_decimal_dist():
    assert extract_dist_going("9.1G") == (float("9.1"), "G")


def test_extract_middle_details_when_jockey_and_single_digit_position():
    expected = {
        "headgear": None,
        "allowance": 0,
        "jockey": "JFanning",
        "position": "3",
    }

    assert extract_middle_details("JFanning3") == expected


def test_extract_middle_details_when_jockey_and_double_digit_position():
    expected = {
        "headgear": None,
        "allowance": 0,
        "jockey": "JFanning",
        "position": "12",
    }

    assert extract_middle_details("JFanning12") == expected


def test_extract_middle_details_when_headgear_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": 0,
        "jockey": "JFanning",
        "position": "12",
    }

    assert extract_middle_details("tJFanning12") == expected


def test_extract_middle_details_when_allowance_jockey_position():
    expected = {
        "headgear": None,
        "allowance": 3,
        "jockey": "HDavies",
        "position": "12",
    }

    assert extract_middle_details("3HDavies12") == expected


def test_extract_middle_details_when_headgear_allowance_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "12",
    }

    assert extract_middle_details("t3HDavies12") == expected


def test_extract_middle_details_when_position_equal():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "=12",
    }

    assert extract_middle_details("t3HDavies=12") == expected


def test_extract_middle_details_when_position_reordered():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "2p3",
    }

    assert extract_middle_details("t3HDavies2p3") == expected


def test_extract_middle_details_when_position_is_disaster():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "p",
    }

    assert extract_middle_details("t3HDaviesp") == expected


def test_extract_middle_details_when_position_includes_disqualification():
    expected = {
        "headgear": "t",
        "allowance": 3,
        "jockey": "HDavies",
        "position": "3d",
    }

    assert extract_middle_details("t3HDavies3d") == expected


def test_extract_prize():
    assert extract_prize("2CG1156") == ("2CG", "1156")


def test_extract_rating_when_rating_only():
    assert extract_rating("115") == 115


def test_extract_rating_when_hurdle_rating():
    assert extract_rating("115h") == 115


def test_extract_rating_when_chase_rating():
    assert extract_rating("115c") == 115


def test_extract_rating_when_disaster():
    assert extract_rating("p19c") is None


def test_extract_rating_when_no_rating():
    assert extract_rating("-") is None


def test_extract_rating_when_no_rating_over_jumps():
    assert extract_rating("-c") is None


def test_extract_rating_when_less_than_zero():
    assert extract_rating("-2") is None


def test_extract_weight():
    assert extract_weight("9-13t3RyanSexton") == ("9-13", "t3RyanSexton")


def test_get_formdata_date():
    assert get_formdata_date(FORMDATA_FILENAMES[0]) == pendulum.date(2019, 6, 30)


def test_get_formdatas_default(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    assert get_formdatas() == FORMDATA_FILENAMES


def test_get_formdatas_with_code(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_nh_230611.pdf",
        "formdata_nh_230618.pdf",
        "formdata_nh_230625.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert get_formdatas(code=RacingCode.NH) == expected


def test_get_formdatas_with_after_year(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_230528.pdf",
        "formdata_230604.pdf",
        "formdata_230611.pdf",
        "formdata_230618.pdf",
        "formdata_230625.pdf",
        "formdata_230702.pdf",
        "formdata_nh_230611.pdf",
        "formdata_nh_230618.pdf",
        "formdata_nh_230625.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert get_formdatas(after_year=22) == expected


def test_get_formdatas_with_for_refresh(mocker):
    mocker.patch(FORMDATA_FETCH).return_value = FORMDATA_FILENAMES
    expected = [
        "formdata_190630.pdf",
        "formdata_200628.pdf",
        "formdata_210530.pdf",
        "formdata_220529.pdf",
        "formdata_230528.pdf",
        "formdata_230702.pdf",
        "formdata_nh_230611.pdf",
        "formdata_nh_230702.pdf",
    ]
    assert get_formdatas(for_refresh=True) == expected


def test_is_horse_true_without_country():
    assert is_horse("AADDEEY")


def test_is_horse_true_with_country():
    assert is_horse("AADDEEY (IRE)")


def test_is_horse_true_with_multiple_word_name():
    assert is_horse("HURRICANE LANE")


def test_is_horse_not_true_for_dist_going():
    assert not is_horse("5G")


def test_is_horse_not_true_for_title():
    assert not is_horse("FORMDATA")


def test_is_horse_not_true_for_trainer():
    assert not is_horse("D B O'Meara")


def test_is_horse_not_true_for_trainer_initials():
    assert not is_horse("D B O")


def test_is_horse_not_true_for_date_range():
    assert not is_horse("21 MAY 22 - 21 MAY 23")


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_false_with_non_date():
    assert not is_race_date("JMitchell")


def test_transform_horse_returns_correct_output():
    data = FormdataRun(
        date="2024-06-01",
        race_type="Hc",
        win_prize="5",
        course="Kel",
        number_of_runners=5,
        weight="10-0",
        jockey="D Tudhope",
        position="2p3",
        beaten_distance=2.0,
        time_rating=80,
        distance=24.0,
        going="G",
        form_rating=80,
    )
    expected = {
        "date": "2024-06-01",
        "race_type": "Hc",
        "win_prize": "5",
        "course": "Kel",
        "number_of_runners": 5,
        "lbs_carried": 140,
        "headgear": None,
        "allowance": None,
        "beaten_distance": 2.0,
        "distance": 24.0,
        "going": "G",
        "going_assessment": "Good",
        "finishing_position": "2",
        "official_position": "3",
        "ratings": {"rr_time": 80, "rr_form": 80},
    }

    actual = transform_run(data)
    assert actual == expected
