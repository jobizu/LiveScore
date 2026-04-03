from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

SOURCE_URL = (
    "https://prod-cdn-public-api.lsmedia1.com/v1/api/app/competition/65/fixtures-w/0"
    "?locale=en&countryCode=GB&limit=200"
)
DEFAULT_OUTPUT = Path("data/epl_livescore_fixtures_2025_2026.csv")
EAT_OFFSET_HOURS = 3
OUTPUT_HEADERS = ["EventId", "Date", "Time", "Home", "Away", "Status", "HomeScore", "AwayScore"]


def fetch_fixtures(url: str, timeout: int) -> list[dict]:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreEPLCsvScraper/1.0)"},
    )
    response.raise_for_status()
    payload = response.json()

    stages = payload.get("Stages") or []
    if not stages:
        return []

    return stages[0].get("Events") or []


def parse_event_datetime(esd: str) -> datetime | None:
    value = (str(esd or "")).strip()
    if len(value) < 12:
        return None
    try:
        return datetime.strptime(value[:12], "%Y%m%d%H%M")
    except ValueError:
        return None


def map_events(events: list[dict], days_ahead: int) -> list[dict[str, str]]:
    now_eat = datetime.now(timezone.utc) + timedelta(hours=EAT_OFFSET_HOURS)
    today = now_eat.date()
    end_date = today + timedelta(days=days_ahead)

    rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for event in events:
        event_id = (event.get("Eid") or "").strip()
        if event_id and event_id in seen_ids:
            continue

        event_dt_utc = parse_event_datetime(event.get("Esd"))
        if event_dt_utc is None:
            continue

        event_dt_eat = event_dt_utc + timedelta(hours=EAT_OFFSET_HOURS)

        event_date = event_dt_eat.date()
        if event_date < today or event_date > end_date:
            continue

        home = ((event.get("T1") or [{}])[0].get("Nm") or "").strip()
        away = ((event.get("T2") or [{}])[0].get("Nm") or "").strip()
        if not home or not away:
            continue

        status = (event.get("Eps") or "").strip() or "NS"
        home_score = str(event.get("Tr1") or "").strip()
        away_score = str(event.get("Tr2") or "").strip()

        rows.append(
            {
                "EventId": event_id,
                "Date": event_dt_eat.strftime("%d/%m/%Y"),
                "Time": event_dt_eat.strftime("%H:%M"),
                "Home": home,
                "Away": away,
                "Status": status,
                "HomeScore": home_score,
                "AwayScore": away_score,
            }
        )

        if event_id:
            seen_ids.add(event_id)

    rows.sort(key=lambda r: (r["Date"], r["Time"], r["Home"], r["Away"]))
    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape LiveScore Premier League fixtures and store date/time/fixtures in CSV."
    )
    parser.add_argument("--url", default=SOURCE_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=62,
        help="Number of days ahead to include upcoming fixtures (default: 62).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print(f"Fetching LiveScore EPL fixtures from {args.url}...")
    events = fetch_fixtures(args.url, args.timeout)

    print("Transforming fixtures into CSV rows...")
    rows = map_events(events, args.days_ahead)

    print(f"Saving {len(rows)} rows to {args.output}...")
    write_csv(rows, args.output)

    print(f"Saved LiveScore EPL fixtures CSV to: {args.output.resolve()}, Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
