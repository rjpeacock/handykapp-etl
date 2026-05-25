from unittest.mock import MagicMock

from bson import ObjectId

from utilities.race_duplicates import resolve_duplicates

AAA = ObjectId()
BBB = ObjectId()
CCC = ObjectId()


def make_runner(horse_id, ratings=None, **kwargs):
    runner = {"horse": horse_id, **kwargs}
    if ratings:
        runner["ratings"] = ratings
    return runner


def test_race_not_found():
    races = MagicMock()
    races.find_one.return_value = None

    result = resolve_duplicates(races, ObjectId())

    assert result == 0


def test_no_runners():
    races = MagicMock()
    races.find_one.return_value = {"runners": []}

    result = resolve_duplicates(races, ObjectId())

    assert result == 0


def test_no_duplicates():
    races = MagicMock()
    races.find_one.return_value = {
        "runners": [make_runner(AAA), make_runner(BBB)]
    }

    result = resolve_duplicates(races, ObjectId())

    assert result == 0
    races.update_one.assert_not_called()


def test_duplicate_keeps_entry_with_ratings():
    races = MagicMock()
    races.find_one.return_value = {
        "runners": [
            make_runner(AAA),
            make_runner(AAA, ratings={"rr_time": 42}),
        ]
    }

    result = resolve_duplicates(races, ObjectId())

    assert result == 1
    kept = races.update_one.call_args[0][1]["$set"]["runners"]
    assert len(kept) == 1
    assert kept[0]["ratings"] == {"rr_time": 42}


def test_duplicate_no_ratings_keeps_first():
    races = MagicMock()
    races.find_one.return_value = {
        "runners": [
            make_runner(AAA, lbs_carried=126),
            make_runner(AAA, lbs_carried=130),
        ]
    }

    result = resolve_duplicates(races, ObjectId())

    assert result == 1
    kept = races.update_one.call_args[0][1]["$set"]["runners"]
    assert len(kept) == 1
    assert kept[0]["lbs_carried"] == 126


def test_multiple_horses_some_duplicated():
    races = MagicMock()
    races.find_one.return_value = {
        "runners": [
            make_runner(AAA),
            make_runner(BBB),
            make_runner(AAA, ratings={"rr_time": 50}),
            make_runner(CCC),
            make_runner(BBB),
        ]
    }

    result = resolve_duplicates(races, ObjectId())

    assert result == 2
    kept = races.update_one.call_args[0][1]["$set"]["runners"]
    assert len(kept) == 3
    aaa = [r for r in kept if r["horse"] == AAA][0]
    assert aaa["ratings"] == {"rr_time": 50}
    bbb = [r for r in kept if r["horse"] == BBB][0]
    assert "ratings" not in bbb
