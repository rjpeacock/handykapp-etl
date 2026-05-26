
from processors.runner_processor import collect_people, make_runner_dict


def test_make_runner_dict(mocker):
    horse = mocker.MagicMock()
    horse._id = "horse_1"

    result = make_runner_dict(horse, "horse_1")

    assert result["horse"] == "horse_1"


def test_make_runner_dict_includes_non_runner_when_true(mocker):
    horse = mocker.MagicMock()
    horse.non_runner = True

    result = make_runner_dict(horse, "horse_1")

    assert result.get("non_runner") is True


def test_make_runner_dict_omits_non_runner_when_none(mocker):
    horse = mocker.MagicMock()
    horse.non_runner = None

    result = make_runner_dict(horse, "horse_1")

    assert "non_runner" not in result


def test_make_runner_dict_includes_non_runner_when_false(mocker):
    horse = mocker.MagicMock()
    horse.non_runner = False

    result = make_runner_dict(horse, "horse_1")

    assert result.get("non_runner") is False


def test_collect_people_with_trainer_and_jockey(mocker):
    horse = mocker.MagicMock()
    horse.trainer = "John Smith"
    horse.jockey = "Jane Doe"
    
    result = collect_people(horse, "race_1", "runner_1", "source1")
    
    assert len(result) == 2
    assert result[0][0].name == "John Smith"
    assert result[0][0].role == "trainer"
    assert result[1][0].name == "Jane Doe"
    assert result[1][0].role == "jockey"


def test_collect_people_with_only_jockey(mocker):
    horse = mocker.MagicMock()
    horse.trainer = None
    horse.jockey = "Jane Doe"
    
    result = collect_people(horse, "race_1", "runner_1", "source1")
    
    assert len(result) == 1
    assert result[0][0].name == "Jane Doe"
    assert result[0][0].role == "jockey"
