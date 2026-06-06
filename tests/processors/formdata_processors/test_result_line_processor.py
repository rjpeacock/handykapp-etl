from processors.formdata_processors.result_line_processor import (
    _parse_db_age,
    _parse_fd_age,
)


def test_fd_age_handicap_with_age():
    assert _parse_fd_age("3H") == "3"


def test_fd_age_group_race():
    assert _parse_fd_age("2CG2") == "2"


def test_fd_age_only():
    assert _parse_fd_age("2") == "2"


def test_fd_age_no_digit_returns_none():
    assert _parse_fd_age("H") is None


def test_fd_age_empty_returns_none():
    assert _parse_fd_age("") is None


def test_fd_age_older_race_no_digit():
    assert _parse_fd_age("C") is None


def test_db_age_single_digit():
    assert _parse_db_age("2") == "2"


def test_db_age_with_suffix():
    assert _parse_db_age("4+") == "4"


def test_db_age_none_returns_none():
    assert _parse_db_age(None) is None


def test_db_age_empty_returns_none():
    assert _parse_db_age("") is None
