from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

import cloudscraper
from bs4 import BeautifulSoup

SOURCE_URL = "https://www.forebet.com/en/football-tips-and-predictions-for-germany/bundesliga/results"
DEFAULT_OUTPUT = Path("data/bundesliga_forebet_results_2025_2026.csv")
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
        raise ValueError("Could not find Forebet Bundesliga results table.")

    rows: list[dict[str, str]] = []
    current_date = ""

    for row in table.find_all("tr"):
        date_cell = row.find("td", class_="dateCell")
        if date_cell is not None:
            current_date = normalize_date(date_cell.get_text(strip=True))
            continue

        cells = row.find_all("td")
        if len(cells) != 4 or not current_date:
            continue

        home = " ".join(cells[1].get_text(" ", strip=True).split())
        score = " ".join(cells[2].get_text(" ", strip=True).split())
        away = " ".join(cells[3].get_text(" ", strip=True).split())

        if not home or not away or " - " not in score:
            continue

        rows.append({"Date": current_date, "Result": f"{home} {score} {away}"})

    if not rows:
        raise ValueError("No Bundesliga result rows were parsed from Forebet.")

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
        description="Scrape Forebet Bundesliga 2025/26 results into a minimal CSV."
    )
    parser.add_argument("--url", default=SOURCE_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    html = fetch_html(args.url, args.timeout)
    rows = parse_rows(html)
    write_csv(rows, args.output)
    print(f"Saved Forebet Bundesliga results CSV to: {args.output.resolve()}")
    print(f"Rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())