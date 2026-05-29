def test_text_replacement_merges_newline_and_replacement_char_into_apostrophe():
    text = "KTO\n\uFFFDNeill 10"
    words = (
        text.replace(f"{chr(10)}{chr(25)}", "'")
        .replace(f"{chr(10)}{chr(65533)}", "'")
        .replace(f"{chr(10)}{chr(32)}{chr(25)}", "'")
        .replace(f"{chr(10)}{chr(32)}{chr(65533)}", "'")
        .replace(f"{chr(32)}{chr(25)}", "'")
        .replace(f"{chr(32)}{chr(65533)}", "'")
        .replace(chr(25), "'")
        .replace(chr(65533), "'")
        .replace(chr(0), "'")
        .split("\n")
    )
    assert words == ["KTO'Neill 10"]


def test_text_replacement_merges_newline_space_and_replacement_char():
    text = "KTO\n \uFFFDNeill 10"
    words = (
        text.replace(f"{chr(10)}{chr(25)}", "'")
        .replace(f"{chr(10)}{chr(65533)}", "'")
        .replace(f"{chr(10)}{chr(32)}{chr(25)}", "'")
        .replace(f"{chr(10)}{chr(32)}{chr(65533)}", "'")
        .replace(f"{chr(32)}{chr(25)}", "'")
        .replace(f"{chr(32)}{chr(65533)}", "'")
        .replace(chr(25), "'")
        .replace(chr(65533), "'")
        .replace(chr(0), "'")
        .split("\n")
    )
    assert words == ["KTO'Neill 10"]


def test_traditional_apostrophe_still_merged():
    text = "KTO\n\x19Neill 10"
    words = (
        text.replace(f"{chr(10)}{chr(25)}", "'")
        .replace(f"{chr(10)}{chr(65533)}", "'")
        .replace(f"{chr(10)}{chr(32)}{chr(25)}", "'")
        .replace(f"{chr(10)}{chr(32)}{chr(65533)}", "'")
        .replace(f"{chr(32)}{chr(25)}", "'")
        .replace(f"{chr(32)}{chr(65533)}", "'")
        .replace(chr(25), "'")
        .replace(chr(65533), "'")
        .replace(chr(0), "'")
        .split("\n")
    )
    assert words == ["KTO'Neill 10"]
