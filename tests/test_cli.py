import pytest

from utilities.non_runners import mark_non_runners


@pytest.fixture(autouse=True)
def mock_logger(mocker):
    mocker.patch("utilities.non_runners.get_run_logger")


def test_mark_non_runners_none_found(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": "2"},
        ],
    })

    total, modified = mark_non_runners.fn()

    assert total == 0
    assert modified == []


def test_mark_non_runners_dry_run(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    total, modified = mark_non_runners.fn(dry_run=True)

    assert total == 1
    assert len(modified) == 1

    race = mock_db.races.find_one()
    runner_h2 = next(r for r in race["runners"] if r["horse"] == "h2")
    assert runner_h2.get("non_runner") is None


def test_mark_non_runners_updates_db(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    total, modified = mark_non_runners.fn()

    assert total == 1
    assert len(modified) == 1

    race = mock_db.races.find_one()
    runner_h2 = next(r for r in race["runners"] if r["horse"] == "h2")
    assert runner_h2["non_runner"] is True
    assert runner_h2.get("finishing_position") is None


def test_mark_non_runners_with_set_position(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    total, modified = mark_non_runners.fn(set_position=True)

    assert total == 1
    assert len(modified) == 1

    race = mock_db.races.find_one()
    runner_h2 = next(r for r in race["runners"] if r["horse"] == "h2")
    assert runner_h2["non_runner"] is True
    assert runner_h2["finishing_position"] == "N"


def test_mark_non_runners_skips_already_tagged(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None, "non_runner": True},
        ],
    })

    total, modified = mark_non_runners.fn()

    assert total == 0
    assert modified == []


def test_mark_non_runners_skips_races_without_results(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    total, modified = mark_non_runners.fn()

    assert total == 0
    assert modified == []


def test_mark_non_runners_limit(mock_db, mocker):
    mocker.patch("utilities.non_runners.db", mock_db)
    mock_db.races.insert_many([
        {
            "runners": [
                {"horse": "h1", "finishing_position": "1"},
                {"horse": "h2", "finishing_position": None},
            ],
        },
        {
            "runners": [
                {"horse": "h3", "finishing_position": "2"},
                {"horse": "h4", "finishing_position": None},
            ],
        },
    ])

    total, modified = mark_non_runners.fn(limit=1)

    assert total == 1
    assert len(modified) == 1
