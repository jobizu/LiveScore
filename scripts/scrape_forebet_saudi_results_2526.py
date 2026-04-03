from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime
from pathlib import Path

import cloudscraper
from bs4 import BeautifulSoup

SOURCE_URL = "https://www.forebet.com/en/football-tips-and-predictions-for-saudi-arabia/professional-league/results"
DEFAULT_OUTPUT = Path("data/saudi_forebet_results_2025_2026.csv")
OUTPUT_HEADERS = ["Date", "Result"]


def fetch_html(url: str, timeout: int) -> str:
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    response = scraper.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreForebetScraper/1.0)"},
    )
    response.raise_for_status()
    return response.text


def normalize_date(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    return datetime.strptime(value, "%d.%m.%Y").strftime("%d/%m/%Y")


def parse_rows(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="resultsTable")
    if table is None:
        raise ValueError("Could not find Forebet Saudi Pro League results table.")

    rows: list[dict[str, str]] = []
    current_date = ""

    for row in table.find_all("tr"):
        date_cell = row.find("td", class_="dateCell")
        if date_cell is not None:
            current_date = normalize_date(date_cell.get_text(strip=True))
            continue

        cells = row.find_all("td")
        if len(cells) >= 4:
            # Forebet results table structure:
            # cells[0]: "HH:MM" (time)
            # cells[1]: "Home Team Name"
            # cells[2]: "Score (e.g., 0 - 1)"
            # cells[3+]: "Away Team Name" and other columns
            
            home_team = cells[1].get_text(strip=True)
            score_cell = cells[2].get_text(strip=True)
            away_team = cells[3].get_text(strip=True)

            # Only add valid results (must have date, teams, and score)
            if current_date and home_team and away_team and score_cell:
                result = f"{home_team} {score_cell} {away_team}".strip()
                rows.append({"Date": current_date, "Result": result})

    return rows


def save_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Forebet Saudi Pro League results."
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
    args = parser.parse_args()

    print(f"Fetching Forebet Saudi Pro League results from {SOURCE_URL}...")
    html = fetch_html(SOURCE_URL, args.timeout)

    print("Parsing results table...")
    rows = parse_rows(html)

    print(f"Saving {len(rows)} rows to {args.output}...")
    save_csv(rows, args.output)

    print(f"Saved Forebet Saudi Pro League results CSV to: {args.output.resolve()}, Rows: {len(rows)}")


if __name__ == "__main__":
    main()
