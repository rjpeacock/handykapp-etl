# To allow running as a script, need path
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import re

import pendulum
import petl  # type: ignore
import tomllib
from horsetalk import (
    Going,
    Horselength,
    RaceWeight,
    RacingCode,
)  # type: ignore
from peak_utility.listish import compact  # type: ignore
from peak_utility.names.corrections import (
    eirify,
    scotify,  # type: ignore
)
from peak_utility.text.case import normal  # type: ignore
from prefect import get_run_logger

from clients import SpacesClient
from models import (
    FormdataHorse,
    FormdataRun,
)

with Path("settings.toml").open("rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]


def adjust_rr_name(name):
    country = name.split("(")[-1].replace(")", "") if "(" in name else None
    name = name.replace(" (" + country + ")", "") if country else name
    name = scotify(normal(name))
    name = re.sub(
        r"([a-z])'([A-Z])",
        lambda match: match.group(1) + "'" + match.group(2).lower(),
        name,
    )

    return f"{name} ({country})" if country else name


def create_horse(words: list[str], year: int) -> FormdataHorse | None:
    logger = get_run_logger()
    words = [w for w in words if w]  # Occasional lines have empty strings at end

    name = words[0].split("(")[0].strip()
    country = words[0].split("(")[1].split(")")[0] if "(" in words[0] else "GB"

    try:
        if len(words) == 5:
            # Base case
            horse = FormdataHorse(
                name=name,
                country=country,
                year=year - int(words[1]),
                trainer=eirify(words[2]),
                trainer_form=words[3],
                prize_money=words[4],
                runs=[],
            )
        elif 2 <= len(words) < 5:
            # Handle cases where horse line has been insufficiently split
            further_split = ("".join(words[1:])).split(" ")
            horse = FormdataHorse(
                name=name,
                country=country,
                year=year - int(further_split[0]),
                trainer=eirify(" ".join(further_split[1:-2])),
                trainer_form=further_split[-2],
                prize_money=further_split[-1],
                runs=[],
            )
        else:
            # Handle cases where trainer name has been split
            horse = FormdataHorse(
                name=name,
                country=country,
                year=year - int(words[1]),
                trainer=eirify("".join(words[2:-2])),
                trainer_form=words[-2],
                prize_money=words[-1],
                runs=[],
            )

        if horse.trainer[0].isdigit():
            logger.error(f"Trainer error on {horse.name}: {horse.trainer}")

    except Exception as e:
        logger.error(f"Error creating horse from {words}: {e}")
        horse = None

    return horse


def create_run(words: list[str]) -> FormdataRun | None:
    logger = get_run_logger()
    try:
        # Handle extra empty string at end of some lines
        words = words if words[-1] != "" else words[:-1]

        # Handle cases where words are insufficiently split
        words = " ".join(words).split(" ")

        # Handle odd case of Phoenix Dawn (missing data)
        if len(words) == 10 and words[6:] == ["b", "RHavlin", "p", "p12"]:
            words = [*words[:9], "", "p12", "16d", "p12"]

        # Handle odd case of Arctic Mountain (incorrect data - says placed when dsq)
        if len(words) == 12 and words[7] == "14p940.0":
            words = [*words[:7], "14d", "40.0", *words[8:]]

        # Split overlong prize money
        if len(words[1]) > 5:
            racetype, prize = extract_prize(words[1]) or ("", "")
            words[1] = racetype
            words.insert(2, prize)

        # Split conjoined weight
        if (len(words[5]) > 5 and words[5][0].isdigit() and "-" in words[5][:3]) or (
            len(words[5]) == 5 and not words[5][-1].isdigit()
        ):
            weight, jockey = extract_weight(words[5]) or ("", "")
            words[5] = weight
            words.insert(6, jockey)

        # Split conjoined finishing distance
        for i, word in enumerate(words[7:10]):
            if "p" in word and ("*" in word or "." in word):
                position, beaten_distance = (
                    word.split("*") if "*" in word else (word[:3], word[3:])
                )
                words[7 + i] = position
                words.insert(8 + i, f"-{beaten_distance}")

        # Join non-finishing jump_details
        indices_to_join = []
        for i, word in enumerate(words[-5:-1]):
            if word in ["-", "n", "o"] and words[-5 + i + 1] in ["b", "c", "h"]:
                indices_to_join.append(-5 + i)

        for index in indices_to_join:
            words = (
                words[:index]
                + ["".join(words[index : index + 2])]
                + (words[index + 2 :] if index < -3 else [])
            )

        # Join jockey details to be processed separately
        flat_non_finisher = any(
            letter in words[-4] for letter in ["b", "f", "n", "p", "u"]
        )
        joined_middle = "".join(
            words[6:-3] if flat_non_finisher and words[-4] != "alone" else words[6:-4]
        )
        middle_details = extract_middle_details(joined_middle)

        if not middle_details:
            raise ValueError(f"Insufficient detail in middle of run: {middle_details}")

        (dist, going) = extract_dist_going(words[-2]) or (None, None)

        if not dist or not going:
            raise ValueError("Insufficient detail in distance or going")

        run = FormdataRun(
            date=pendulum.from_format(words[0], "DDMMMYY").date().format("YYYY-MM-DD"),
            race_type=words[1],
            win_prize=words[2],
            course=words[3],
            number_of_runners=int(words[4]),
            weight=words[5],
            headgear=middle_details["headgear"],
            allowance=middle_details["allowance"],
            jockey=middle_details["jockey"],
            position=middle_details["position"],
            beaten_distance=float(words[-4].replace("*", "-"))
            if "*" in words[-4]
            else float(words[-4])
            if "." in words[-4] and words[-4] != "w.o."
            else None,
            time_rating=extract_rating(words[-3]),
            distance=dist,
            going=going,
            form_rating=extract_rating(words[-1]),
        )
    except Exception as e:
        logger.error(f"Error creating run from {words}: {e}")
        run = None

    return run


DIST_GOING_PATTERN = re.compile(
    r"""
    ^                                       # Start of the string
    (?:r)?                                  # Optional r
    (?P<dist>\d\.?\d?)                      # Distance
    (?P<going>[H|F|M|G|D|S|V|f|m|g|d|s])    # Going
    $                                       # End of the string
""",
    re.VERBOSE,
)


def extract_dist_going(string: str) -> tuple[float, str] | None:
    match = re.match(DIST_GOING_PATTERN, string)
    if match:
        dist = match.group("dist")
        going = match.group("going")
        return (float(dist), going)

    return None


GRADE_PATTERN = re.compile(
    r"""
    G           # Literal 'G'
    [123]       # Single digit 1, 2, or 3
    (?![0-9])   # Not followed by another digit
""",
    re.VERBOSE,
)


MIDDLE_DETAILS_PATTERN = re.compile(
    r"""
    ^                                       # Start of the string
    (?P<headgear>[a-z])?                    # Lowercase letter as the headgear
    (?P<allowance>\d+)?                     # Optional number as the allowance
    (?P<jockey>[a-zA-Z\-\']+)               # Remaining characters as the jockey
    (?P<position>(\=?\d+(?:p\d+|d)?|[a-z])) # Last number or char as the position (With optional = or #p# formatting)
    $                                       # End of the string
""",
    re.VERBOSE,
)


def extract_middle_details(details: str) -> dict | None:
    match = re.match(MIDDLE_DETAILS_PATTERN, details)
    if match:
        return {
            "headgear": match.group("headgear"),
            "allowance": int(match.group("allowance") or 0),
            "jockey": match.group("jockey"),
            "position": match.group("position"),
        }

    return None


PRIZE_PATTERN = re.compile(
    r"""
    ^                                   # Start of the string
    (?P<racetype>\d*[A-Za-z]+)?         # Race type
    (?P<prize>\d{3,4})                  # Prize money
    $                                   # End of the string
""",
    re.VERBOSE,
)


def extract_prize(string: str) -> tuple[str, str] | None:
    match = re.match(PRIZE_PATTERN, string)
    if match:
        racetype = match.group("racetype")
        prize = match.group("prize")
        return (racetype, prize)

    return None


RATING_PATTERN = re.compile(
    r"""
    ^                                   # Start of the string
    (?P<disaster>[a-z-]?)               # Disaster
    (?P<rating>\d{1,3})                 # Rating
    (?P<jumps_category>([a-z]-?)?)      # Jumps category
    $                                   # End of the string
""",
    re.VERBOSE,
)


def extract_rating(string: str) -> int | None:
    match = re.match(RATING_PATTERN, string)
    if match:
        return int(match.group("rating")) if not match.group("disaster") else None

    return None


WEIGHT_PATTERN = re.compile(
    r"""
    ^                                   # Start of the string
    (?P<weight>\d{1,2}\-\d{2})          # Weight
    (?P<jockey>.*)                      # Jockey
    $                                   # End of the string
""",
    re.VERBOSE,
)


def extract_weight(string: str) -> tuple[str, str] | None:
    match = re.match(WEIGHT_PATTERN, string)
    if match:
        weight = match.group("weight")
        jockey = match.group("jockey")
        return (weight, jockey)

    return None


def get_formdata_date(filename: str) -> pendulum.Date:
    return pendulum.from_format(filename[-10:-4], "YYMMDD").date()


def get_formdatas(
    *, code: RacingCode | None = None, after_year: int = 0, for_refresh: bool = False
) -> list[str]:
    base = [
        file
        for file in SpacesClient.get_files(SOURCE)
        if int(file[-10:-8]) > after_year
    ]
    flat = [file for file in base if "nh" not in file]
    nh = [file for file in base if "nh" in file]

    def select_for_refresh(files):
        if not for_refresh:
            return files

        selected_files = []
        next_date = get_formdata_date(files[0])
        current_file = files[0]
        for file in files:
            if get_formdata_date(file) > next_date:
                selected_files.append(current_file)
                next_date = get_formdata_date(current_file).add(years=1)
            current_file = file
        if files[-1] not in selected_files:
            selected_files.append(files[-1])

        return selected_files

    if code == RacingCode.FLAT:
        return select_for_refresh(flat)
    if code == RacingCode.NH:
        return select_for_refresh(nh)

    return select_for_refresh(flat) + select_for_refresh(nh)


def is_horse(string: str) -> bool:
    horse_regex = r"^(?!.*FORMDATA)[A-Z\s]{3,21}(?: \([A-Z]{1,3}\))?$"
    initial_regex = r"^[A-Z] [A-Z] [A-Z]$"
    return (
        string == string.upper()
        and bool(re.match(horse_regex, string))
        and not bool(re.match(initial_regex, string))
    )


def is_race_date(string: str) -> bool:
    date_regex = r"\d{1,2}[A-Z][a-z]{2}\d{2}"
    return bool(re.match(date_regex, string))


FORMDATA_AW_GOINGS = {
    "s": "Slow",
    "d": "Standard to Slow",
    "g": "Standard",
    "m": "Standard to Fast",
    "f": "Fast",
}


def transform_run(run: FormdataRun) -> dict:
    data = petl.fromdicts([run.model_dump()])
    return (
        petl.convert(
            data,
            {
                "weight": lambda x: RaceWeight(x).lb,
                "beaten_distance": lambda x: float(Horselength(x)) if x else None,
                "jockey": lambda x: adjust_rr_name(x),
            },
        )
        .rename({"weight": "lbs_carried"})
        .addfield("finishing_position", lambda rec: rec["position"].split("p")[0])
        .addfield(
            "official_position",
            lambda rec: (
                rec["position"].split("p")[1]
                if "p" in rec["position"]
                else rec["finishing_position"]
            ),
        )
        .addfield(
            "ratings",
            lambda rec: (
                compact({"rr_time": rec["time_rating"], "rr_form": rec["form_rating"]})
                or None
            ),
        )
        .addfield(
            "going_assessment",
            lambda rec: str(
                Going(
                    rec["going"]
                    if rec["going"] != rec["going"].lower()
                    else FORMDATA_AW_GOINGS.get(rec["going"], "Standard")
                )
            ),
        )
        .cutout(
            "position",
            "time_rating",
            "form_rating",
            "jockey",
            "date",
            "race_type",
            "win_prize",
            "course",
            "number_of_runners",
            "distance",
            "going",
        )
        .dicts()[0]
    )
