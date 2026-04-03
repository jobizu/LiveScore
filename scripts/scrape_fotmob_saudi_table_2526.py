from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

import requests

# Saudi Pro League FotMob ID - verify at https://www.fotmob.com/leagues/{ID}
LEAGUE_ID = 536
FOTMOB_URL = f"https://www.fotmob.com/leagues/{LEAGUE_ID}"
DEFAULT_OUTPUT = Path("data/saudi_fotmob_table_2025_2026.csv")
OUTPUT_HEADERS = ["Team", "TeamId", "LogoUrl"]


def fetch_league_page(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreFotMobScraper/1.0)"},
    )
    response.raise_for_status()
    return response.text


def extract_standings_from_json(html: str) -> list[dict[str, str]]:
    # Extract __NEXT_DATA__ JSON from page
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
    if not match:
        raise ValueError("Could not find __NEXT_DATA__ JSON in page.")

    json_str = match.group(1)
    data = json.loads(json_str)

    # Navigate the data structure for FotMob league page
    try:
        page_props = data["props"]["pageProps"]
        table_list = page_props.get("table", [])
        if not table_list or len(table_list) == 0:
            raise ValueError("Table list is empty")
        
        data_item = table_list[0].get("data", {})
        table_dict = data_item.get("table", {})
        all_teams = table_dict.get("all", [])
    except (KeyError, TypeError, IndexError) as e:
        raise ValueError(f"Could not navigate JSON structure: {e}")

    if not isinstance(all_teams, list):
        raise ValueError("Teams data is not a list.")

    rows = []
    for team in all_teams:
        team_name = team.get("name", "").strip()
        team_id = team.get("id", "")

        if team_name and team_id:
            team_id_str = str(team_id)
            rows.append(
                {
                    "Team": team_name,
                    "TeamId": team_id_str,
                    "LogoUrl": f"https://images.fotmob.com/image_resources/logo/teamlogo/{team_id_str}.png",
                }
            )

    return rows


def save_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape FotMob Saudi Pro League table for logo URLs."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output CSV file path (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--league-id",
        type=int,
        default=LEAGUE_ID,
        help=f"FotMob league ID (default: {LEAGUE_ID}). Verify at https://www.fotmob.com/leagues/{{ID}}",
    )
    args = parser.parse_args()

    league_url = f"https://www.fotmob.com/leagues/{args.league_id}"
    print(f"Fetching FotMob Saudi Pro League standings from {league_url}...")
    html = fetch_league_page(league_url, args.timeout)

    print("Parsing standings table...")
    rows = extract_standings_from_json(html)

    print(f"Saving {len(rows)} teams to {args.output}...")
    save_csv(rows, args.output)

    print(f"Saved FotMob Saudi Pro League table CSV to: {args.output.resolve()}, Rows: {len(rows)}")


if __name__ == "__main__":
    main()
