from prefect import get_run_logger

from .word_processor import word_processor


def page_processor():
    logger = get_run_logger()
    logger.info("Starting page processor")

    w = word_processor()
    next(w)

    try:
        while True:
            item = yield
            page, date = item
            text = page.get_text()
            # Replace non-ascii characters with apostrophes
            words = (
                text.replace(f"{chr(10)}{chr(25)}", "'")  # Newline + apostrophe
                .replace(f"{chr(10)}{chr(65533)}", "'")  # Newline + replacement char
                .replace(f"{chr(10)}{chr(32)}{chr(25)}", "'")  # Newline + space + apostrophe
                .replace(f"{chr(10)}{chr(32)}{chr(65533)}", "'")  # Newline + space + replacement char
                .replace(f"{chr(32)}{chr(25)}", "'")  # Space + apostrophe
                .replace(f"{chr(32)}{chr(65533)}", "'")  # Space + replacement char
                .replace(chr(25), "'")  # Regular apostrophe
                .replace(chr(65533), "'")  # Replacement character
                .replace(chr(0), "'")  # Null byte → apostrophe
                .split("\n")
            )
            for word in words:
                w.send((word, date))

    except GeneratorExit:
        w.close()
