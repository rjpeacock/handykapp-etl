from src.extractors.bha_extractor import (
    DESTINATION,
    SOURCE,
    fetch,
    get_signed_urls,
    save,
)


def test_bha_source():
    expected = "https://api09.horseracing.software/bha/v1/ratings/csv/"
    assert expected == SOURCE


def test_bha_destination():
    expected = "handykapp/bha/"
    assert expected == DESTINATION


def test_fetch(mocker):
    mock_fetch_content = mocker.patch("src.extractors.bha_extractor.fetch_content")
    mock_fetch_content.return_value = "foobar"
    signed_urls = {"foo": "https://example.com/data?expires=123&sig=abc"}
    assert fetch.fn("foo", signed_urls) == "foobar"
    mock_fetch_content.assert_called_once_with(
        "https://example.com/data?expires=123&sig=abc", headers=mocker.ANY
    )


def test_get_signed_urls(mocker):
    html = (
        '<a href="https://api09.horseracing.software/bha/v1/ratings/csv/ratings?expires=123&sig=abc">'
        "the full list of ratings</a>\n"
        '<a href="https://api09.horseracing.software/bha/v1/ratings/csv/ratings?diff=&expires=123&sig=abc">'
        "weekly rating changes</a>\n"
        '<a href="https://api09.horseracing.software/bha/v1/ratings/csv/performance-figures?expires=123&sig=abc">'
        "latest performance figures</a>"
    )
    mocker.patch("src.extractors.bha_extractor.fetch_content").return_value = html.encode()
    result = get_signed_urls.fn()
    assert result["ratings"] == (
        "https://api09.horseracing.software/bha/v1/ratings/csv/ratings?expires=123&sig=abc"
    )
    assert result["rating_changes"] == (
        "https://api09.horseracing.software/bha/v1/ratings/csv/ratings?diff=&expires=123&sig=abc"
    )
    assert result["perf_figs"] == (
        "https://api09.horseracing.software/bha/v1/ratings/csv/performance-figures?expires=123&sig=abc"
    )


def test_save(mocker):
    write_file = mocker.patch("src.extractors.bha_extractor.SpacesClient.write_file")
    mocker.patch("src.extractors.bha_extractor.DESTINATION", "example/")
    mocker.patch("src.extractors.bha_extractor.LAST_UPDATE_STR", "20210101")

    save.fn("foo", "foobar")

    assert write_file.call_count == 1
    assert mocker.call("foobar", "example/bha_foo_20210101.csv") == write_file.call_args
