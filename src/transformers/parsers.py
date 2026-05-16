import re


def parse_code(obstacle: str | None, title: str) -> str:
    if obstacle:
        return "National Hunt"

    if re.search(r"\bnhf?\b", title, re.IGNORECASE) or "n.h." in title.lower() or "national hunt" in title.lower():
        return "National Hunt"

    return "Flat"


def parse_obstacle(race_title):
    if not race_title:
        return None

    obstacle_types = {
        r"\bCROSS(-|\s)COUNTRY\b": "CROSS-COUNTRY",
        r"\b(STEEPLE)?CHASE\b(?:\s*\([^)]*\))*\s*$": "CHASE",
        r"\bHURDLE\b(?:\s*\([^)]*\))*\s*$": "HURDLE",
    }

    for regex, obstacle in obstacle_types.items():
        if re.compile(regex).search(race_title.upper()):
            return obstacle
    return None
