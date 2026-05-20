## ~~Generator Pipeline Crash Cascade~~ FIXED

### What was fixed

- `runner_processor`: inner try/except added; `flush_races_and_people()` catches dead generators, reinitializes them, retries the send
- Remaining work extracted below (unhandled exceptions in `person_processor`, `race_processor`, `horse_processor` will now cause data loss rather than a full cascade crash, thanks to the `runner_processor` defence)

---

## Empty `lbs` in TheRacingApi drops races silently

### Symptom
`Unable to process Racing API racecard ... runners.14.lbs Input should be a valid integer, unable to parse string as an integer`. An entire race is silently dropped from the DB when any runner has `lbs=""`.

### Root Cause
`TheRacingApiRunner.lbs: int` (src/models/theracingapi_runner.py:21) rejects `""` during Pydantic validation. The `except Exception` in theracingapi_loader.py:85 catches it and skips the race with a log line — no retry, no recovery.

Downstream, `PreMongoEntry.lbs_carried: int | None = None` (src/models/pre_mongo_entry.py:5) already accepts `None`, so the transformer at src/transformers/theracingapi_transformer.py:87 would also need to handle the empty case.

### Options

1. **Accept `""` in the model** — `lbs: int | Literal[""]` matches the existing `draw` pattern. Then convert `""` to `None` in the transformer.
2. **Let formdata fill the gap** — If empty `lbs` means a non-runner, formdata won't have a result for it anyway, so we may not need to store it at all. But the race would still be lost.
3. **Skip just that runner** — Filter out runners with `lbs=""` before creating the racecard, preserving the race with its remaining runners.

---

## ~~Memory Pressure on 2GB Droplet (Nuclear Reload)~~ FIXED

### What was fixed
| Change | File | Effect |
|--------|------|--------|
| `time.sleep(0.05)` removed from `get_horse()` | `mongo_client.py:59` | Eliminated 42+ minutes of artificial delay |
| Cache `maxsize` 50000→20000 | `mongo_client.py:57` | Saves ~200MB, Python drops to ~387MB |
| `bulk_threshold` 500→100 | `horse_processor.py:70` | Smaller bulk_write commands finish before 120s timeout |
| Tenacity retry on `bulk_write` | `horse_processor.py:53-59` | 3 attempts with expo backoff (4-60s) |
| `ordered=False` on bulk writes | `horse_processor.py:59` | MongoDB processes ops concurrently |
| Tenacity retry on person_processor `bulk_write` | `person_processor.py:30-36` | Same pattern as horse_processor |
| Batch 50→20 + remove sleeps | `person_processor.py` | Smaller batches, no 0.5s/0.02s wasted delays |
| Re-queue current person on batch failure | `person_processor.py:136-140` | Prevents death spiral: keeps `person_updates[-1]`, doesn't skip |

---

## ~~Formdata race matching: apostrophe-variant horse names~~ FIXED

### Fix Applied
`src/clients/mongo_client.py:67-79`: Removed the `if "'" in horse.name:` guard so the apostrophe-flexible regex fallback always runs, not just when the input name contains an apostrophe.

---

## ~~RapidAPI going description parsing (horsetalk bug)~~ FIXED

Fixed by bumping horsetalk dependency.

---

## person_processor: stale `found_id` and lost updates on DuplicateKeyError

### Symptoms

- Runners linked to wrong jockey/trainer (stale `found_id` reused from previous person)
- Bulk update for the `DuplicateKeyError`-triggering person silently dropped from `person_updates`
- Could cause data corruption that only surfaces when comparing formdata results against racecard data

### Root Cause

`person_processor.py:104-121` — when `person_collection.update_one(...)` raises `DuplicateKeyError`, the code does not re-assign `found_id`. The `continue` at line 121 skips `person_updates.append(...)`, so:

1. `found_id` retains the value from a *previous* person → subsequent code at lines 110-113 uses this stale `found_id` to link runners
2. The current person's update is never appended to `person_updates` → silently lost

### Fix

1. On `DuplicateKeyError`, assign `found_id` from the caught error's details (the duplicate doc's `_id`) or skip it with `continue` to avoid stale usage
2. Remove the bare `continue` so the current person isn't silently dropped from `person_updates` — either append a no-op or re-queue it
3. Log a warning so the event is visible in production

---

## ~~Race code/obstacle misclassification from title parsing~~ FIXED

### Symptoms

- Thirsk 2:05 "decronhorsecare.co.uk Nursery Handicap" missing from DB — formdata reported "No race found" for runners at that race
- Flat races with "Chase" in a horse name (e.g. "Hullabaloos Chase Handicap") incorrectly classified as National Hunt, silently dropped by `record_processor.py:46` (`if race.code == "Flat"`)

### Root Cause

`src/transformers/parsers.py` used title-based heuristics that were too greedy:

1. **`parse_code`** (line 8): `"nh" in title.lower()` matched "nh" inside any word — "decron**horse**care" triggered a false positive
2. **`parse_obstacle`** (line 20): `\b(STEEPLE)?CHASE\b` matched standalone "Chase" anywhere — horse names like "Hullabaloos **Chase** Handicap" matched regardless of position in title

### Fix

Two-pronged:

1. **`src/transformers/theracingapi_transformer.py`**: Replaced `parse_obstacle`/`parse_code` with direct mapping from the API's `type` field (`"Flat"`, `"Hurdle"`, `"Chase"`, `"NH Flat"`). Added `# TODO: Improve in horsetalk`.

2. **`src/transformers/parsers.py`** (still used by `rapid_horseracing_transformer.py`, which lacks a `type` field):
   - `parse_code`: `"nh" in title.lower()` → `re.search(r"\bnhf?\b", title, re.IGNORECASE)` — word boundary check, also matches "NHF" (National Hunt Flat)
   - `parse_obstacle`: `\bCHASE\b`, `\bHURDLE\b` → end-anchored with optional trailing parentheticals `(?:\s*\([^)]*\))*\s*$` — "Chase" in the middle of a title (e.g. "Hullabaloos Chase Handicap") no longer matches

---

## ~~Formdata race fields leaking into runner subdocuments~~ FIXED

### Symptom

`transform_run` was writing race-level fields (`date`, `race_type`, `win_prize`, `course`, `number_of_runners`, `distance`, `going`) into each runner's `$set` in MongoDB. These fields belong on the race document, not on individual runners.

### Root Cause

`src/transformers/formdata_transformer.py:389-425` — `transform_run` starts from `run.model_dump()` which includes all `FormdataRun` fields. The `.cutout()` at line 423 only removed `position`, `time_rating`, `form_rating`, `jockey` — the race-level fields passed through untouched into the runner dict.

### Fix

Added `date`, `race_type`, `win_prize`, `course`, `number_of_runners`, `distance`, `going` to the `cutout` call. Runner dict now only contains: `allowance`, `beaten_distance`, `finishing_position`, `going_assessment` (popped to race root by processor), `headgear`, `lbs_carried`, `official_position`, `ratings`.

---

## Missing racecard dates — fill gaps via RapidAPI + formdata fallback

### Problem

TheRacingApi has no racecard data for 144 dates (2023–2026). These gaps mean no race documents in the DB → formdata can't find races → "No race found" for horses like Potters Marmite.

RapidAPI can fetch those dates' racecards (even post-switch, since RapidAPI isn't date-gated), but the sequential extractor won't reach them for months.

### Plan (5 phases)

**Phase 1 — Fetch missing racecards via RapidAPI**

New flow `fetch_missing_racecards` that:
- Reads missing dates from TheRacingApi's Spaces dir (`identify_missing_dates` already writes them)
- Cross-checks against existing RapidAPI racecards in Spaces (skips done)
- Fetches up to 50 racecards per run (respecting `day=50, minute=10` limits)
- Stores in `{RAPID_SPACES_DIR}racecards/`
- Run 3× to cover all ~144 dates. No result fetching during this time.

**Phase 2 — Load skeleton races into DB**

Run `load_rapid_horseracing_entries(source="racecards")`:
- Reads racecards from Spaces
- Creates `RapidRecord` for each race (horses defaults to `[]`)
- `transform_to_entries` → skeleton races with `runners=[]`

**Phase 3 — Conditional fallback in `result_line_processor.py`**

When `runners.horse` query returns None:
1. Query races at `racecourse_id` on `run.date` (no horse filter)
2. Map `race_type` → code ("FLAT"→"Flat", else "National Hunt")
3. Convert `run.distance` (float miles) → furlongs: `round(distance * 8)`
4. Convert each race's `distance_description` → furlongs
5. Filter to races matching both code and distance
6. If **exactly 1** match: `$addToSet {"horse": horse["_id"]}` → runners, proceed with normal update
7. If **0** matches: log warning, skip
8. If **>1** matches: log warning with race IDs/titles, skip

**Phase 4 — CLI tool**

New `cli.py` (or similar) with:

| Command | Action |
|---|---|
| `create-horse --name "Potters Marmite" --country GB --year 2019` | Insert horse doc |
| `add-horse-to-race --race-id <id> --horse-id <id>` | `$addToSet` horse into race's runners |
| `list-races --course "Lucksin" --date 2025-09-28` | Find races by course+date, show IDs & titles |

This manually resolves the >1-match cases that Phase 3 logged.

**Phase 5 — Run formdata loader**

After Phases 1–4, run `load_formdata()`:
- Unique distance+code matches auto-populate via Phase 3
- Division cases listed in logs → use CLI to add horse to correct race
- Second formdata run finds the race and writes result data

### Race division edge case

Multiple divisions of the same handicap (same course, date, distance, code) cannot be resolved automatically from formdata alone. These are logged and skipped — the CLI makes them a 2-minute manual fix.

### Duplicate runner concern

When full RapidAPI results arrive later, `runner_processor.flush_races` does a blind `$push`. If formdata already added a skeleton runner, the real runner duplicates it. A future fix should make `flush_races` skip horses already in the runners array.

---

## Formdata RR jockey names not parsed before person_processor

### Symptom

Jockey docs in the DB with `first: "JMitchell"` (concatenated initial+lastname as first name, empty last name) and `references.rr` pointing to the wrong jockey (e.g. `rr: "MsSJohns'e"` on a doc with `first: "MsHDoyle"`).

### Root Cause — two bugs interacting

**Bug 1 — `adjust_rr_name` result discarded**

`transform_run()` at `src/transformers/formdata_transformer.py:394` applied `adjust_rr_name` to the jockey (which calls `normal()` from peak-utility — splits `"JSmith"` → `"J Smith"`), but then cut the jockey field out of the returned dict at line 428. The caller `_apply_result_to_race()` in `result_line_processor.py:29` used the raw `run.jockey` instead, sending unspaced names like `"JMitchell"` to `person_processor`.

**Bug 2 — Loose match logic when `last=""`**

`person_processor` uses `HumanName(name)` to parse jockey names. Without spaces (e.g. `"JMitchell"`), HumanName puts the entire string in `first` with `last=""`. The DB search `{"last": ""}` returned all people with empty last names, and the first-letter fallback match (`first[0] == possibility["first"][0] and title == possibility["title"]`) was broad enough to match **unrelated** jockeys — e.g. `"MsSJohns'e"` matched a doc for `"MsHDoyle"` because both start with `"M"` and have `title=""`.

### Fix — three changes

| Change | File | Lines |
|---|---|---|
| Removed `"jockey"` from `cutout` — adjusted name stays in transform result | `src/transformers/formdata_transformer.py` | 424→428 |
| Pop adjusted jockey from `result` (like `going_assessment`), send to person processor | `src/processors/formdata_processors/result_line_processor.py` | 19–20 |
| Tightened match logic: first-letter fallback requires *both* sides to have a non-empty `last` | `src/processors/person_processor.py` | 88–94 |

### Residual issue — DuplicateKeyError in person_processor

Even with properly spaced names, a race condition can still occur: two concurrent subflows both search for `{"last": "Mitchell"}` → neither finds the person → both try `insert_one` → one hits the unique index `(last, first, middle)`. The fix fetches the existing doc by those key fields, sets `found_id`, and removes the bare `continue` so the runner-link update isn't silently dropped.

| Change | File |
|---|---|
| On `DuplicateKeyError`, query by `(last, first, middle)`, set `found_id`, update references | `src/processors/person_processor.py` |

---

## ~~Formdata RR jockey names not parsed before person_processor~~ FIXED

### What was fixed

- `transform_run`: jockey no longer cut out from result
- `result_line_processor`: adjusted jockey popped from result and used for `PreMongoPerson`
- `person_processor`: match logic requires both sides to have non-empty `last` for first-letter fallback
- `person_processor`: `DuplicateKeyError` handler now fetches existing doc, sets `found_id`, and falls through to `person_updates` instead of `continue`
