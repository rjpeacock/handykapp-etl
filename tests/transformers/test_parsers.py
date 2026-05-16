from transformers.parsers import (
    parse_code,
    parse_obstacle,
)


def test_parse_code_returns_correct_value_for_obstacle():
    assert parse_code("CHASE", None) == "National Hunt"


def test_parse_code_returns_correct_value_for_national_hunt_in_title():
    assert parse_code(None, "Big National Hunt Flat Race") == "National Hunt"


def test_parse_code_works_case_insensitively():
    assert parse_code(None, "BIG NATIONAL HUNT FLAT RACE") == "National Hunt"


def test_parse_code_returns_correct_value_for_nh_in_title():
    assert parse_code(None, "Big NHF Race") == "National Hunt"


def test_parse_code_returns_correct_value_for_dot_separated_nh_in_title():
    assert parse_code(None, "Big N.H.F Race") == "National Hunt"


def test_parse_code_returns_correct_value_when_not_obstacle_or_nh():
    assert parse_code(None, "Big Handicap") == "Flat"


def test_parse_obstacle_returns_correct_value_for_none():
    assert None is parse_obstacle("A RACE")


def test_parse_obstacle_returns_correct_value_for_chase():
    assert parse_obstacle("A CHASE") == "CHASE"


def test_parse_obstacle_returns_correct_value_for_steeplechase():
    assert parse_obstacle("A STEEPLECHASE") == "CHASE"


def test_parse_obstacle_returns_correct_value_for_embedded_use_of_chase():
    assert None is parse_obstacle("A PURCHASE")


def test_parse_obstacle_returns_none_if_name_is_none():
    assert None is parse_obstacle(None)


def test_parse_obstacle_returns_correct_value_for_hurdle():
    assert parse_obstacle("A HURDLE") == "HURDLE"


def test_parse_obstacle_returns_correct_value_for_cross_country():
    assert parse_obstacle("A CROSS COUNTRY") == "CROSS-COUNTRY"


def test_parse_obstacle_returns_correct_value_for_cross_country_chase():
    assert parse_obstacle("A CROSS COUNTRY CHASE") == "CROSS-COUNTRY"


def test_parse_obstacle_returns_correct_value_for_national_hunt_flat():
    assert None is parse_obstacle("A NATIONAL HUNT FLAT")


def test_parse_obstacle_returns_correct_value_for_flat_race():
    assert None is parse_obstacle("A BIG STAKES RACE")


def test_parse_obstacle_is_case_insensitive():
    assert parse_obstacle("a chase") == "CHASE"


def test_parse_obstacle_returns_none_for_chase_embedded_in_horse_name():
    assert None is parse_obstacle("Hullabaloos Chase Handicap")


def test_parse_obstacle_returns_none_for_hurdle_embedded_in_horse_name():
    assert None is parse_obstacle("My Hurdle Maiden")


def test_parse_obstacle_matches_chase_at_end_with_parenthetical():
    assert parse_obstacle("Novices Chase (GBB Race)") == "CHASE"


def test_parse_obstacle_matches_hurdle_at_end_with_division():
    assert parse_obstacle("Handicap Hurdle (Div I)") == "HURDLE"


def test_parse_obstacle_matches_chase_as_single_word():
    assert parse_obstacle("CHASE") == "CHASE"
    assert parse_obstacle("chase") == "CHASE"


def test_parse_code_returns_flat_for_nh_inside_word():
    assert parse_code(None, "decronhorsecare.co.uk Nursery Handicap") == "Flat"


def test_parse_code_returns_flat_for_chase_inside_horse_name():
    assert parse_code(None, "Hullabaloos Chase Handicap") == "Flat"


def test_parse_code_returns_flat_for_hurdle_inside_horse_name():
    assert parse_code(None, "My Hurdle Maiden") == "Flat"


def test_parse_code_returns_national_hunt_for_nh_as_standalone_word():
    assert parse_code(None, "Open NH Flat") == "National Hunt"


def test_parse_code_returns_national_hunt_for_nhf_as_word():
    assert parse_code(None, "Standard NHF Race") == "National Hunt"


def test_parse_code_returns_national_hunt_for_national_hunt_with_parenthetical():
    assert parse_code(None, "National Hunt Flat (Category 1)") == "National Hunt"
