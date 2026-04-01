from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
import unicodedata

import requests

DEFAULT_OUTPUT = Path("data/europe_top10_2025_2026.csv")


@dataclass(frozen=True)
class CompetitionConfig:
    name: str
    country: str
    code: str
    results_url: str | None
    fixtures_url: str | None


COMPETITIONS: list[CompetitionConfig] = [
    CompetitionConfig("Premier League", "England", "E0", "https://www.football-data.co.uk/mmz4281/2526/E0.csv", "https://fixturedownload.com/download/epl-2025-UTC.csv"),
    CompetitionConfig("La Liga", "Spain", "SP1", "https://www.football-data.co.uk/mmz4281/2526/SP1.csv", "https://fixturedownload.com/download/la-liga-2025-UTC.csv"),
    CompetitionConfig("Serie A", "Italy", "I1", "https://www.football-data.co.uk/mmz4281/2526/I1.csv", "https://fixturedownload.com/download/serie-a-2025-UTC.csv"),
    CompetitionConfig("Bundesliga", "Germany", "D1", "https://www.football-data.co.uk/mmz4281/2526/D1.csv", "https://fixturedownload.com/download/bundesliga-2025-UTC.csv"),
    CompetitionConfig("Ligue 1", "France", "F1", "https://www.football-data.co.uk/mmz4281/2526/F1.csv", "https://fixturedownload.com/download/ligue-1-2025-UTC.csv"),
    CompetitionConfig("Eredivisie", "Netherlands", "N1", "https://www.football-data.co.uk/mmz4281/2526/N1.csv", "https://fixturedownload.com/download/eredivisie-2025-UTC.csv"),
    CompetitionConfig("Primeira Liga", "Portugal", "P1", "https://www.football-data.co.uk/mmz4281/2526/P1.csv", "https://fixturedownload.com/download/primeira-liga-2025-UTC.csv"),
    CompetitionConfig("Belgian Pro League", "Belgium", "B1", "https://www.football-data.co.uk/mmz4281/2526/B1.csv", None),
    CompetitionConfig("Turkish Super Lig", "Turkey", "T1", "https://www.football-data.co.uk/mmz4281/2526/T1.csv", "https://fixturedownload.com/download/super-lig-2025-UTC.csv"),
    CompetitionConfig("Scottish Premiership", "Scotland", "SC0", "https://www.football-data.co.uk/mmz4281/2526/SC0.csv", None),
    CompetitionConfig("UEFA Champions League", "Europe", "UCL", None, "https://fixturedownload.com/download/champions-league-2025-UTC.csv"),
    CompetitionConfig("UEFA Europa League", "Europe", "UEL", None, "https://fixturedownload.com/download/europa-league-2025-UTC.csv"),
]

OUTPUT_HEADERS = [
    "Competition",
    "Country",
    "Code",
    "Date",
    "Time",
    "HomeTeam",
    "AwayTeam",
    "FTHG",
    "FTAG",
    "FTR",
    "Source",
]

TEAM_NAME_ALIASES = {
    "spurs": "tottenham",
    "man utd": "man united",
    # Turkish team names - keys are already diacritic-stripped (post-NFD)
    "genclerbirligi": "genclerbirligi",
    "fenerbahce": "fenerbahce",
    "goztepe": "goztepe",
    "goztep": "goztepe",
    "caykur rizespor": "rizespor",
    "eyupspor": "eyupspor",
    "fatih karagumruk": "karagumruk",
    "basaksehir": "istanbul basaksehir",
    "istanbul basaksehir": "istanbul basaksehir",
    "buyuksehyr": "istanbul basaksehir",
    "kasimpasa": "kasimpasa",
}


def fetch_text(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreMultiLeagueScraper/1.0)"},
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def normalize_team(name: str) -> str:
    raw = (name or "").strip().lower()
    normalized = "".join(
        char for char in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(char)
    )
    normalized = " ".join(normalized.split())
    return TEAM_NAME_ALIASES.get(normalized, normalized)


def parse_result_cell(result_value: str) -> tuple[str, str, str]:
    raw = (result_value or "").strip()
    if not raw:
        return "", "", ""

    parts = [part.strip() for part in raw.split("-")]
    if len(parts) != 2:
        return "", "", ""

    left, right = parts
    if not left.isdigit() or not right.isdigit():
        return "", "", ""

    home = int(left)
    away = int(right)
    if home > away:
        ftr = "H"
    elif home < away:
        ftr = "A"
    else:
        ftr = "D"
    return str(home), str(away), ftr


def split_fixture_datetime(value: str) -> tuple[str, str]:
    raw = (value or "").strip()
    if " " in raw:
        d, t = raw.split(" ", 1)
        return d.strip(), t.strip()
    return raw, ""


def as_date(value: str) -> datetime:
    return datetime.strptime((value or "").strip(), "%d/%m/%Y")


def row_datetime(row: dict[str, str]) -> datetime:
    date_raw = (row.get("Date") or "").strip()
    time_raw = (row.get("Time") or "00:00").strip() or "00:00"
    try:
        return datetime.strptime(f"{date_raw} {time_raw}", "%d/%m/%Y %H:%M")
    except ValueError:
        try:
            return as_date(date_raw)
        except ValueError:
            return datetime.max


def is_better_row(current: dict[str, str], candidate: dict[str, str]) -> bool:
    current_played = bool((current.get("FTHG") or "").strip() and (current.get("FTAG") or "").strip())
    candidate_played = bool((candidate.get("FTHG") or "").strip() and (candidate.get("FTAG") or "").strip())
    if candidate_played != current_played:
        return candidate_played

    current_priority = 2 if (current.get("Source") or "").strip() == "football-data" else 1
    candidate_priority = 2 if (candidate.get("Source") or "").strip() == "football-data" else 1
    if candidate_priority != current_priority:
        return candidate_priority > current_priority

    return len((candidate.get("HomeTeam") or "").strip()) + len((candidate.get("AwayTeam") or "").strip()) > len((current.get("HomeTeam") or "").strip()) + len((current.get("AwayTeam") or "").strip())


def is_single_pair_season_competition(comp: CompetitionConfig) -> bool:
    return comp.code == "T1"


def load_results_rows(comp: CompetitionConfig, timeout: int) -> list[dict[str, str]]:
    if not comp.results_url:
        return []

    text = fetch_text(comp.results_url, timeout)
    reader = csv.DictReader(StringIO(text))
    rows = []
    for row in reader:
        date_raw = (row.get("Date") or "").strip()
        home = (row.get("HomeTeam") or "").strip()
        away = (row.get("AwayTeam") or "").strip()
        if not date_raw or not home or not away:
            continue

        rows.append(
            {
                "Competition": comp.name,
                "Country": comp.country,
                "Code": comp.code,
                "Date": date_raw,
                "Time": (row.get("Time") or "").strip(),
                "HomeTeam": home,
                "AwayTeam": away,
                "FTHG": (row.get("FTHG") or "").strip(),
                "FTAG": (row.get("FTAG") or "").strip(),
                "FTR": (row.get("FTR") or "").strip(),
                "Source": "football-data",
            }
        )
    return rows


def merge_fixture_rows(comp: CompetitionConfig, current_rows: list[dict[str, str]], timeout: int) -> tuple[list[dict[str, str]], int]:
    if not comp.fixtures_url:
        return current_rows, 0

    text = fetch_text(comp.fixtures_url, timeout)
    reader = csv.DictReader(StringIO(text))
    fixture_rows = list(reader)

    existing_by_key = {}
    for index, row in enumerate(current_rows):
        existing_by_key[(normalize_team(row["HomeTeam"]), normalize_team(row["AwayTeam"]))] = index

    added = 0
    for row in fixture_rows:
        date_raw, time_raw = split_fixture_datetime(row.get("Date") or "")
        home = (row.get("Home Team") or "").strip()
        away = (row.get("Away Team") or "").strip()
        if not date_raw or not home or not away:
            continue

        key = (normalize_team(home), normalize_team(away))
        fthg, ftag, ftr = parse_result_cell(row.get("Result") or "")
        candidate_row = {
            "Competition": comp.name,
            "Country": comp.country,
            "Code": comp.code,
            "Date": date_raw,
            "Time": time_raw,
            "HomeTeam": home,
            "AwayTeam": away,
            "FTHG": fthg,
            "FTAG": ftag,
            "FTR": ftr,
            "Source": "fixturedownload",
        }

        existing_index = existing_by_key.get(key)
        if existing_index is None:
            current_rows.append(candidate_row)
            existing_by_key[key] = len(current_rows) - 1
            added += 1
            continue

        existing_row = current_rows[existing_index]
        if is_single_pair_season_competition(comp):
            if is_better_row(existing_row, candidate_row):
                current_rows[existing_index] = candidate_row
            continue

        if abs((row_datetime(candidate_row).date() - row_datetime(existing_row).date()).days) <= 2:
            if is_better_row(existing_row, candidate_row):
                current_rows[existing_index] = candidate_row
            continue

        current_rows.append(candidate_row)
        added += 1

    return current_rows, added


def sort_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    def key(row: dict[str, str]):
        date_raw = (row.get("Date") or "").strip()
        time_raw = (row.get("Time") or "00:00").strip() or "00:00"
        try:
            dt = datetime.strptime(f"{date_raw} {time_raw}", "%d/%m/%Y %H:%M")
        except ValueError:
            try:
                dt = as_date(date_raw)
            except ValueError:
                dt = datetime.max
        return (dt, row.get("Competition") or "", row.get("HomeTeam") or "")

    return sorted(rows, key=key)


def write_output(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADERS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in OUTPUT_HEADERS})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape top 10 European leagues + UCL/UEL (results + upcoming fixtures) into one CSV."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    all_rows: list[dict[str, str]] = []
    total_added_upcoming = 0

    for comp in COMPETITIONS:
        comp_rows = load_results_rows(comp, args.timeout)
        comp_rows, added = merge_fixture_rows(comp, comp_rows, args.timeout)
        total_added_upcoming += added
        all_rows.extend(comp_rows)

    all_rows = sort_rows(all_rows)
    write_output(all_rows, args.output)

    print(f"Saved multi-league CSV to: {args.output.resolve()}")
    print(f"Competitions: {len(COMPETITIONS)}")
    print(f"Rows: {len(all_rows)}")
    print(f"Upcoming fixtures added: {total_added_upcoming}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
