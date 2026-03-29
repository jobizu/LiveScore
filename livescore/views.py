import csv
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path

import requests
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render


def _friendly_api_error(response):
    if response.status_code == 403:
        return "API-Football rejected the configured key for fixtures access. Update API_FOOTBALL_KEY or enable fixtures access on your API-Football plan."

    try:
        payload = response.json()
    except ValueError:
        return f"API request failed with status {response.status_code}."

    errors = payload.get("errors") or {}
    if isinstance(errors, dict) and errors:
        details = "; ".join(f"{key}: {value}" for key, value in errors.items())
        return f"API request failed: {details}"

    return f"API request failed with status {response.status_code}."


def _api_football_key():
    key = settings.API_FOOTBALL_KEY
    if key:
        return key

    # Fallback for local dev when key is present in .env.example.
    env_example = Path(settings.BASE_DIR) / ".env.example"
    if env_example.exists():
        for line in env_example.read_text().splitlines():
            if line.startswith("API_FOOTBALL_KEY="):
                return line.split("=", 1)[1].strip()
    return ""


def _map_fixtures(rows):
    matches = []
    for row in rows:
        fixture = row.get("fixture", {})
        league  = row.get("league", {})
        teams   = row.get("teams", {})
        goals   = row.get("goals", {})
        status  = fixture.get("status", {})

        short   = status.get("short") or "NS"
        elapsed = status.get("elapsed")
        is_live = short in {"1H", "2H", "HT", "ET", "BT", "P", "LIVE"}

        matches.append({
            "league":      league.get("name", "League"),
            "league_logo": league.get("logo", ""),
            "country":     league.get("country", ""),
            "status":      "LIVE" if is_live else short,
            "minute":      f"{elapsed}'" if is_live and elapsed is not None else "",
            "home":        (teams.get("home") or {}).get("name", "Home"),
            "home_logo":   (teams.get("home") or {}).get("logo", ""),
            "away":        (teams.get("away") or {}).get("name", "Away"),
            "away_logo":   (teams.get("away") or {}).get("logo", ""),
            "home_score":  goals.get("home", "-") if goals.get("home") is not None else "-",
            "away_score":  goals.get("away", "-") if goals.get("away") is not None else "-",
            "kickoff":     str(fixture.get("date", "")).replace("T", " ")[:16] or "TBD",
            "link":        "https://www.api-football.com/",
        })
    return matches


def _extract_leagues(rows):
    seen = OrderedDict()
    for row in rows:
        league = row.get("league", {})
        lid    = league.get("id")
        if lid is None:
            continue
        if lid not in seen:
            seen[lid] = {
                "id":      lid,
                "name":    league.get("name", "Unknown"),
                "country": league.get("country", ""),
                "logo":    league.get("logo", ""),
                "count":   0,
            }
        seen[lid]["count"] += 1
    return list(seen.values())


def _ensure_friendlies(leagues):
    """Always include friendlies entries in the sidebar list."""
    normalized = {str((league.get("name") or "")).lower(): league for league in leagues}

    def upsert(name, country):
        # Reuse an existing friendlies league if present, otherwise add a placeholder entry.
        existing = None
        for key, league in normalized.items():
            if "friendl" in key and name.split()[0].lower() in key:
                existing = league
                break

        if existing:
            existing["name"] = name
            if not existing.get("country"):
                existing["country"] = country
            return existing

        entry = {
            "id": f"friendly-{name.lower().replace(' ', '-')}",
            "name": name,
            "country": country,
            "logo": "",
            "count": 0,
        }
        leagues.append(entry)
        return entry

    club = upsert("Club Friendlies", "World")
    intl = upsert("International Friendlies", "World")

    # Keep both friendlies pinned to the top for visibility.
    remaining = [league for league in leagues if league is not club and league is not intl]
    return [club, intl] + remaining


def _selected_date_from_request(request):
    raw_date = (request.GET.get("date") or "").strip()
    if not raw_date:
        return date.today(), False

    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date(), True
    except ValueError:
        return date.today(), False


def _fetch_fixtures_for_date(selected_date):
    matches = []
    leagues = []
    api_error = ""
    api_access_error = False
    selected_date_label = "Today" if selected_date == date.today() else f"{selected_date.day} {selected_date.strftime('%b %Y')}"

    csv_path = Path(settings.BASE_DIR) / "data" / "epl_2025_2026.csv"
    target_date = selected_date.strftime("%d/%m/%Y")

    if not csv_path.exists():
        api_error = f"EPL CSV file not found: {csv_path}. Run scripts/scrape_epl_2526.py first."
    else:
        try:
            with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                required = {"Date", "Time", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}
                if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                    missing = required.difference(set(reader.fieldnames or []))
                    missing_list = ", ".join(sorted(missing))
                    api_error = f"EPL CSV is missing required columns: {missing_list}."
                else:
                    for row in reader:
                        row_date = (row.get("Date") or "").strip()
                        if row_date != target_date:
                            continue

                        home_goals_raw = (row.get("FTHG") or "").strip()
                        away_goals_raw = (row.get("FTAG") or "").strip()
                        played = home_goals_raw != "" and away_goals_raw != ""

                        matches.append(
                            {
                                "league": "Premier League",
                                "league_logo": "",
                                "country": "England",
                                "status": "FT" if played else "NS",
                                "minute": "",
                                "home": (row.get("HomeTeam") or "Home").strip(),
                                "home_logo": "",
                                "away": (row.get("AwayTeam") or "Away").strip(),
                                "away_logo": "",
                                "home_score": home_goals_raw if played else "-",
                                "away_score": away_goals_raw if played else "-",
                                "kickoff": f"{row_date} {(row.get('Time') or '').strip()}".strip(),
                                "link": "#",
                            }
                        )
        except OSError as exc:
            api_error = f"Unable to read EPL CSV: {exc}"

    leagues = [
        {
            "id": "E0",
            "name": "Premier League",
            "country": "England",
            "logo": "",
            "count": len(matches),
        }
    ]

    if not matches and not api_error:
        api_error = f"No Premier League fixtures scheduled for {selected_date_label.lower()}."

    return {
        "matches": matches,
        "leagues": leagues,
        "api_error": api_error,
        "api_access_error": api_access_error,
        "selected_date_label": selected_date_label,
    }


def fixtures_api(request):
    selected_date, has_explicit_date = _selected_date_from_request(request)
    data = _fetch_fixtures_for_date(selected_date)

    if request.GET.get("date") and not has_explicit_date:
        data["api_error"] = "Invalid date format. Use ?date=YYYY-MM-DD."

    return JsonResponse(
        {
            "date": selected_date.isoformat(),
            "date_label": data["selected_date_label"],
            "api_error": data["api_error"],
            "api_access_error": data["api_access_error"],
            "league_count": len(data["leagues"]),
            "match_count": len(data["matches"]),
            "leagues": data["leagues"],
            "matches": data["matches"],
        }
    )


def home(request):
    selected_date, has_explicit_date = _selected_date_from_request(request)
    data = _fetch_fixtures_for_date(selected_date)

    if request.GET.get("date") and not has_explicit_date:
        data["api_error"] = "Invalid date format. Showing today's fixtures instead."

    return render(
        request,
        "home.html",
        {
            "leagues": data["leagues"],
            "matches": data["matches"],
            "api_error": data["api_error"],
            "api_access_error": data["api_access_error"],
            "selected_date": selected_date.isoformat(),
            "selected_date_label": data["selected_date_label"],
        },
    )


def csv_view(request):
    csv_path = Path(r"C:\Users\Administrator\Downloads\I1.csv")
    headers = []
    rows = []
    error = ""
    max_rows = 300
    team_name = "Juventus"

    def _new_bucket():
        return {"played": 0, "wins": 0, "draws": 0, "losses": 0, "gf": 0, "ga": 0}

    team_summary = {
        "overall": _new_bucket(),
        "home": _new_bucket(),
        "away": _new_bucket(),
    }
    home_results = []
    away_results = []
    overall_results = []

    def _to_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _apply_result(bucket, gf, ga):
        bucket["played"] += 1
        bucket["gf"] += gf
        bucket["ga"] += ga
        if gf > ga:
            bucket["wins"] += 1
            return "W"
        if gf < ga:
            bucket["losses"] += 1
            return "L"
        bucket["draws"] += 1
        return "D"

    if not csv_path.exists():
        error = f"CSV file not found: {csv_path}"
    else:
        try:
            with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as csv_file:
                reader = csv.reader(csv_file)
                all_rows = list(reader)

            if all_rows:
                headers = all_rows[0]
                data_rows = all_rows[1:]
                rows = data_rows[:max_rows]

                # Build Juventus-specific home/away/overall result summaries.
                idx = {name: i for i, name in enumerate(headers)}
                required = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}
                if required.issubset(idx):
                    for raw_row in data_rows:
                        padded_row = raw_row + [""] * (len(headers) - len(raw_row))
                        home_team = padded_row[idx["HomeTeam"]]
                        away_team = padded_row[idx["AwayTeam"]]

                        if home_team != team_name and away_team != team_name:
                            continue

                        home_goals = _to_int(padded_row[idx["FTHG"]])
                        away_goals = _to_int(padded_row[idx["FTAG"]])
                        date_value = padded_row[idx["Date"]]
                        score_value = f"{home_goals}-{away_goals}"

                        if home_team == team_name:
                            team_goals = home_goals
                            opp_goals = away_goals
                            side = "home"
                        else:
                            team_goals = away_goals
                            opp_goals = home_goals
                            side = "away"

                        result_letter = _apply_result(team_summary[side], team_goals, opp_goals)
                        _apply_result(team_summary["overall"], team_goals, opp_goals)

                        match_row = {
                            "date": date_value,
                            "home": home_team,
                            "away": away_team,
                            "score": score_value,
                            "result": result_letter,
                        }

                        overall_results.append(match_row)
                        if side == "home":
                            home_results.append(match_row)
                        else:
                            away_results.append(match_row)
            else:
                error = "CSV file is empty."
        except OSError as exc:
            error = f"Unable to read CSV file: {exc}"

    # Normalize row lengths to avoid broken table layout on uneven CSV lines.
    col_count = max([len(headers)] + [len(row) for row in rows], default=0)
    if col_count and len(headers) < col_count:
        headers = headers + [f"Column {i}" for i in range(len(headers) + 1, col_count + 1)]

    normalized_rows = [
        row + [""] * (col_count - len(row))
        for row in rows
    ]

    return render(
        request,
        "csv_view.html",
        {
            "csv_path": str(csv_path),
            "headers": headers,
            "rows": normalized_rows,
            "shown_rows": len(normalized_rows),
            "max_rows": max_rows,
            "error": error,
            "team_name": team_name,
            "team_summary": team_summary,
            "home_results": home_results[:30],
            "away_results": away_results[:30],
            "overall_results": overall_results[:40],
        },
    )
