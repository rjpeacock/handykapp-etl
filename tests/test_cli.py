from click.testing import CliRunner

from cli import cli


def test_mark_non_runners_none_found(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": "2"},
        ],
    })

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners"])

    assert result.exit_code == 0
    assert "No non-runners found." in result.output


def test_mark_non_runners_dry_run(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners", "--dry-run"])

    assert result.exit_code == 0
    assert "Found 1 non-runner(s)" in result.output
    assert "dry" not in result.output.lower() or "Would" not in result.output

    race = mock_db.races.find_one()
    runner_h2 = next(r for r in race["runners"] if r["horse"] == "h2")
    assert runner_h2.get("non_runner") is None


def test_mark_non_runners_updates_db(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners"])

    assert result.exit_code == 0
    assert "Marked 1 non-runner(s) across 1 race(s)." in result.output

    race = mock_db.races.find_one()
    runner_h2 = next(r for r in race["runners"] if r["horse"] == "h2")
    assert runner_h2["non_runner"] is True
    assert runner_h2.get("finishing_position") is None


def test_mark_non_runners_with_set_position(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners", "--set-position"])

    assert result.exit_code == 0
    assert "Marked 1 non-runner(s) across 1 race(s)." in result.output

    race = mock_db.races.find_one()
    runner_h2 = next(r for r in race["runners"] if r["horse"] == "h2")
    assert runner_h2["non_runner"] is True
    assert runner_h2["finishing_position"] == "N"


def test_mark_non_runners_skips_already_tagged(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1", "finishing_position": "1"},
            {"horse": "h2", "finishing_position": None, "non_runner": True},
        ],
    })

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners"])

    assert result.exit_code == 0
    assert "No non-runners found." in result.output


def test_mark_non_runners_skips_races_without_results(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
    mock_db.races.insert_one({
        "runners": [
            {"horse": "h1"},
            {"horse": "h2", "finishing_position": None},
        ],
    })

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners"])

    assert result.exit_code == 0
    assert "No non-runners found." in result.output


def test_mark_non_runners_limit(mock_db, mocker):
    mocker.patch("cli.db", mock_db)
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

    runner = CliRunner()
    result = runner.invoke(cli, ["mark-non-runners", "--limit", "1"])

    assert result.exit_code == 0
    assert "Marked 1 non-runner(s) across 1 race(s)." in result.output
