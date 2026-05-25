import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

from datetime import datetime as dt

import click
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from clients.mongo_client import get_horse
from models import PreMongoHorse
from utilities.race_duplicates import find_duplicate_horses, resolve_duplicates

db = client.handykapp


@click.group()
def cli():
    pass


@cli.command()
@click.option("--name", required=True, help="Horse name")
@click.option("--country", default=None, help="Country code (e.g. GB, IRE)")
@click.option("--year", type=int, required=True, help="Foaling year")
def create_horse(name, country, year):
    """Insert a horse document into the database."""
    horse = PreMongoHorse(name=name, country=country, year=year)
    existing = get_horse(horse)
    if existing:
        click.echo(f"Horse already exists: {existing['_id']}")
        return
    try:
        result = db.horses.insert_one(horse.model_dump())
        click.echo(f"Created horse: {result.inserted_id}")
    except DuplicateKeyError:
        click.echo("Horse already exists (duplicate key)")


@cli.command()
@click.option("--race-id", required=True, help="Race ObjectId")
@click.option("--horse-id", required=True, help="Horse ObjectId")
def add_horse_to_race(race_id, horse_id):
    """Add a horse to a race's runners array ($addToSet)."""
    result = db.races.update_one(
        {"_id": ObjectId(race_id)},
        {"$addToSet": {"runners": {"horse": ObjectId(horse_id)}}},
    )
    if result.modified_count:
        click.echo(f"Horse {horse_id} added to race {race_id}")
    else:
        click.echo("No change (horse may already be a runner, or race not found)")


@cli.command()
@click.option("--course", required=True, help="Course name (e.g. Lucksin)")
@click.option("--date", required=True, help="Race date (YYYY-MM-DD)")
def list_races(course, date):
    """List races at a course on a date, showing IDs and titles."""
    try:
        dt.strptime(date, "%Y-%m-%d")
    except ValueError:
        click.echo(f"Invalid date format: {date}. Use YYYY-MM-DD.")
        return

    racecourses = list(
        db.racecourses.find(
            {
                "$or": [
                    {"name": {"$regex": f"^{course}$", "$options": "i"}},
                    {"formal_name": {"$regex": f"^{course}$", "$options": "i"}},
                ]
            }
        )
    )

    if not racecourses:
        click.echo(f"No racecourse found matching '{course}'")
        return

    courses_str = ", ".join(
        f"{c['name']} ({c.get('surface', '?')})" for c in racecourses
    )
    click.echo(f"Matching racecourses: {courses_str}")

    races = list(
        db.races.find(
            {
                "racecourse": {"$in": [c["_id"] for c in racecourses]},
                "$expr": {
                    "$eq": [
                        {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$datetime",
                            }
                        },
                        date,
                    ]
                },
            },
            {
                "_id": 1,
                "title": 1,
                "distance_description": 1,
                "code": 1,
                "datetime": 1,
            },
        ).sort("datetime", 1)
    )

    if not races:
        click.echo(f"No races found at '{course}' on {date}")
        return

    click.echo(f"\nFound {len(races)} race(s):")
    for race in races:
        click.echo(
            f"  {race['_id']}  "
            f"{race.get('datetime', '?'):%H:%M}  "
            f"{race.get('code', '?'):17s}  "
            f"{race.get('distance_description', '?'):10s}  "
            f"{race.get('title', '?')}"
        )


@cli.command()
@click.option(
    "--apply", is_flag=True, help="Actually fix duplicates (default is dry-run)"
)
def fix_duplicates(apply):
    """Find and fix duplicate horse entries in race runners."""
    msg = "and fixing" if apply else "(dry-run)"
    click.echo(f"Scanning for duplicate horse entries {msg}...")
    results = find_duplicate_horses(db.races)

    if not results:
        click.echo("No duplicates found.")
        return

    total = 0
    for r in results:
        race_id = r["_id"]
        dup_count = (
            resolve_duplicates(db.races, race_id)
            if apply
            else sum(d["count"] - 1 for d in r["duplicates"])
        )
        click.echo(f"  Race {race_id}: {dup_count} duplicate(s)")
        total += dup_count

    action = "Removed" if apply else "Would remove"
    click.echo(f"\n{action} {total} duplicate entries across {len(results)} races.")
    if not apply:
        click.echo("Run with --apply to fix.")


if __name__ == "__main__":
    cli()
