from __future__ import annotations

import argparse
import csv
from datetime import datetime
from io import StringIO
from pathlib import Path

import requests

SOURCE_URL = "https://www.football-data.co.uk/mmz4281/2526/E0.csv"
FIXTURES_URL = "https://fixturedownload.com/download/epl-2025-UTC.csv"
DEFAULT_OUTPUT = Path("data/epl_2025_2026.csv")


def fetch_csv_text(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreEPLScraper/1.0)"},
    )
    response.raise_for_status()
    return response.text


def validate_csv(csv_text: str) -> tuple[list[str], int]:
    reader = csv.reader(StringIO(csv_text))
    rows = list(reader)
    if not rows:
        raise ValueError("Downloaded CSV is empty.")

    headers = rows[0]
    required_columns = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"}
    missing = required_columns.difference(headers)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Downloaded CSV is missing expected columns: {missing_list}")

    return headers, len(rows) - 1


def parse_csv_rows(csv_text: str) -> tuple[list[str], list[dict[str, str]]]:
    reader = csv.DictReader(StringIO(csv_text))
    headers = reader.fieldnames or []
    rows = list(reader)
    if not headers:
        raise ValueError("Downloaded CSV has no header row.")
    return headers, rows


def parse_fixtures_rows(csv_text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(StringIO(csv_text))
    required = {"Date", "Home Team", "Away Team", "Result"}
    headers = set(reader.fieldnames or [])
    missing = required.difference(headers)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Fixtures CSV is missing expected columns: {missing_list}")
    return list(reader)


def normalize_team(name: str) -> str:
    aliases = {
        "spurs": "tottenham",
        "man utd": "man united",
    }
    key = (name or "").strip().lower()
    return aliases.get(key, key)


def split_fixture_datetime(value: str) -> tuple[str, str]:
    raw = (value or "").strip()
    if " " in raw:
        date_part, time_part = raw.split(" ", 1)
        return date_part.strip(), time_part.strip()
    return raw, ""


def merge_upcoming_fixtures(
    result_headers: list[str],
    result_rows: list[dict[str, str]],
    fixture_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], int]:
    existing_keys = set()
    for row in result_rows:
        key = (
            (row.get("Date") or "").strip(),
            normalize_team(row.get("HomeTeam") or ""),
            normalize_team(row.get("AwayTeam") or ""),
        )
        existing_keys.add(key)

    division_value = next((row.get("Div") for row in result_rows if row.get("Div")), "E0")
    added = 0

    for fixture in fixture_rows:
        result_value = (fixture.get("Result") or "").strip()
        # Only add upcoming fixtures; played matches already come from football-data source.
        if result_value:
            continue

        date_part, time_part = split_fixture_datetime(fixture.get("Date") or "")
        home = (fixture.get("Home Team") or "").strip()
        away = (fixture.get("Away Team") or "").strip()
        key = (date_part, normalize_team(home), normalize_team(away))
        if not date_part or not home or not away or key in existing_keys:
            continue

        new_row = {header: "" for header in result_headers}
        if "Div" in new_row:
            new_row["Div"] = division_value
        if "Date" in new_row:
            new_row["Date"] = date_part
        if "Time" in new_row:
            new_row["Time"] = time_part
        if "HomeTeam" in new_row:
            new_row["HomeTeam"] = home
        if "AwayTeam" in new_row:
            new_row["AwayTeam"] = away

        result_rows.append(new_row)
        existing_keys.add(key)
        added += 1

    return result_rows, added


def render_csv_text(headers: list[str], rows: list[dict[str, str]]) -> str:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=headers, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()

    def sort_key(row: dict[str, str]):
        date_raw = (row.get("Date") or "").strip()
        time_raw = (row.get("Time") or "00:00").strip() or "00:00"
        try:
            dt = datetime.strptime(f"{date_raw} {time_raw}", "%d/%m/%Y %H:%M")
        except ValueError:
            try:
                dt = datetime.strptime(date_raw, "%d/%m/%Y")
            except ValueError:
                dt = datetime.max

        return (dt, (row.get("HomeTeam") or ""), (row.get("AwayTeam") or ""))

    for row in sorted(rows, key=sort_key):
        writer.writerow({header: row.get(header, "") for header in headers})
    return output.getvalue()


def save_csv(csv_text: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(csv_text, encoding="utf-8", newline="")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download/scrape EPL 2025/26 results CSV and save it locally."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--url",
        default=SOURCE_URL,
        help=f"Source URL (default: {SOURCE_URL})",
    )
    parser.add_argument(
        "--fixtures-url",
        default=FIXTURES_URL,
        help=f"Upcoming fixtures source URL (default: {FIXTURES_URL})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results_csv_text = fetch_csv_text(args.url, args.timeout)
    headers, _ = validate_csv(results_csv_text)
    _, result_rows = parse_csv_rows(results_csv_text)

    fixtures_csv_text = fetch_csv_text(args.fixtures_url, args.timeout)
    fixture_rows = parse_fixtures_rows(fixtures_csv_text)

    merged_rows, added_upcoming = merge_upcoming_fixtures(headers, result_rows, fixture_rows)
    final_csv_text = render_csv_text(headers, merged_rows)
    save_csv(final_csv_text, args.output)

    print(f"Saved EPL 2025/26 CSV to: {args.output.resolve()}")
    print(f"Rows (matches): {len(merged_rows)}")
    print(f"Upcoming fixtures added: {added_upcoming}")
    print(f"Columns: {len(headers)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
