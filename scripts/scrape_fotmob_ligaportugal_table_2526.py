from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup

SOURCE_URL = "https://www.fotmob.com/leagues/61/table/liga-portugal"
DEFAULT_OUTPUT = Path("data/ligaportugal_fotmob_table_2025_2026.csv")
OUTPUT_HEADERS = [
    "Position",
    "Team",
    "TeamId",
    "Played",
    "Wins",
    "Draws",
    "Losses",
    "GoalsFor",
    "GoalsAgainst",
    "GoalDifference",
    "Points",
    "LogoUrl",
]


def fetch_html(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreFotMobScraper/1.0)"},
    )
    response.raise_for_status()
    return response.text


def parse_next_data(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if script is None or not script.string:
        raise ValueError("Could not locate __NEXT_DATA__ payload on FotMob page.")
    return json.loads(script.string)


def build_logo_url(team_id: int) -> str:
    return f"https://images.fotmob.com/image_resources/logo/teamlogo/{team_id}.png"


def parse_table_rows(next_data: dict) -> list[dict[str, str]]:
    page_props = next_data.get("props", {}).get("pageProps", {})
    table_sections = page_props.get("table") or []
    if not table_sections:
        raise ValueError("No table section found in FotMob payload.")

    data = table_sections[0].get("data", {})
    standings = (data.get("table") or {}).get("all") or []
    if not standings:
        raise ValueError("No Liga Portugal standings rows found in FotMob payload.")

    rows: list[dict[str, str]] = []
    for item in standings:
        scores_str = (item.get("scoresStr") or "-").strip()
        goals_for, goals_against = "", ""
        if "-" in scores_str:
            left, right = scores_str.split("-", 1)
            goals_for, goals_against = left.strip(), right.strip()

        team_id = item.get("id")
        rows.append(
            {
                "Position": str(item.get("idx") or ""),
                "Team": str(item.get("name") or "").strip(),
                "TeamId": str(team_id or ""),
                "Played": str(item.get("played") or ""),
                "Wins": str(item.get("wins") or ""),
                "Draws": str(item.get("draws") or ""),
                "Losses": str(item.get("losses") or ""),
                "GoalsFor": goals_for,
                "GoalsAgainst": goals_against,
                "GoalDifference": str(item.get("goalConDiff") or ""),
                "Points": str(item.get("pts") or ""),
                "LogoUrl": build_logo_url(int(team_id)) if team_id else "",
            }
        )

    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_HEADERS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape FotMob Liga Portugal table and team logo URLs into CSV."
    )
    parser.add_argument("--url", default=SOURCE_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    html = fetch_html(args.url, args.timeout)
    next_data = parse_next_data(html)
    rows = parse_table_rows(next_data)
    write_csv(rows, args.output)
    print(f"Saved FotMob Liga Portugal table CSV to: {args.output.resolve()}")
    print(f"Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())