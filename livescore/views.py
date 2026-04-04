import csv
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
import unicodedata

import requests
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render


COUNTRY_FLAG_CODES = {
    "England": "gb-eng",
    "Spain": "es",
    "Italy": "it",
    "Germany": "de",
    "France": "fr",
    "Netherlands": "nl",
    "Portugal": "pt",
    "Belgium": "be",
    "Turkey": "tr",
    "Scotland": "gb-sct",
    "Europe": "eu",
    "World": "un",
}

TEAM_NAME_ALIASES = {
    "spurs": "tottenham",
    "man utd": "manchester united",
    "man united": "manchester united",
    "man city": "manchester city",
    "newcastle": "newcastle united",
    "leeds": "leeds united",
    "nott'm forest": "nottingham forest",
    "espanol": "espanyol",
    "rcd espanyol de barcelona": "espanyol",
    "celta": "celta vigo",
    "rc celta de vigo": "celta vigo",
    "tottenham hotspur": "tottenham",
    "wolverhampton wanderers": "wolverhampton",
    "wolves": "wolverhampton",
    "brighton and hove albion": "brighton",
    "brighton & hove albion": "brighton",
    "brighton hove albion": "brighton",
    "west ham united": "west ham",
    "afc bournemouth": "bournemouth",
    "athletic club": "athletic bilbao",
    "mallorca": "real mallorca",
    # Bundesliga name alignment (Forebet vs FotMob)
    "bayern munchen": "bayern munich",
    "borussia monchengladbach": "borussia m'gladbach",
    "fc heidenheim": "heidenheim",
    "1. fc koln": "koln",
    "mainz 05": "mainz",
    "st. pauli": "st pauli",
    "vfb stuttgart": "stuttgart",
    # Serie A name alignment (Forebet vs FotMob)
    "internazionale": "inter",
    "parma calcio 1913": "parma",
    "hellas verona": "verona",
    "lecce": "us lecce",
    "cremonese": "us cremonese",
    "como": "calcio como",
    "sassuolo": "us sassuolo",
    "pisa": "ac pisa",
    "cagliari": "cagliari calcio",
    # Ligue 1 name alignment (Forebet vs FotMob)
    "paris saint-germain": "psg",
    "lyon": "olympique lyonnais",
    "nice": "ogc nice",
    "metz": "fc metz",
    "strasbourg": "strasbourg alsace",
    "lens": "rc lens",
    "rennes": "stade rennais",
    "nantes": "fc nantes",
    "angers": "angers sco",
    "lille": "losc",
    "marseille": "olympique marseille",
    # Eredivisie name alignment (Forebet vs FotMob)
    "go ahead eagles": "go ahead eagles",
    "n.e.c.": "nec nijmegen",
    "nec": "nec nijmegen",
    "nec nijmegen": "nijmegen",
    "sparta rotterdam": "sparta",
    "fortuna sittard": "fortuna",
    "feyenoord": "feyenoord rotterdam",
    "fc groningen": "groningen",
    "sc heerenveen": "heerenveen",
    "fc twente": "twente",
    "fc utrecht": "utrecht",
    "fc volendam": "volendam",
    "telstar": "sc telstar",
    # Liga Portugal name alignment (Forebet vs FotMob)
    "sporting cp": "sporting",
    "fc porto": "porto",
    "vitoria guimaraes": "vitoria",
    "vitoria de guimaraes": "vitoria",
    "braga": "sporting braga",
    "sl benfica": "benfica",
    "avs futebol sad": "avs",
    "alverca": "fc alverca",
    "arouca": "fc arouca",
    "famalicao": "fc famalicao",
    "estoril": "gd estoril",
    "nacional": "nacional madeira",
    "tondela": "cd tondela",
    "estrela amadora": "estrela da amadora",
    "moreirense": "moreirense fc",
    "boavista": "boavista fc",
    # Super Lig name alignment (Forebet vs FotMob)
    "gaziantep fk": "gaziantep",
    "goztepe": "goztepe izmir",
    "kasımpasa": "kasımpasa sk",
    "kasimpasa": "kasımpasa sk",
        # Saudi Pro League name alignment (Forebet vs FotMob)
        "al akhdood": "al akhdoud",
        "al fateh (ksa)": "al fateh fc",
        "al feiha": "al-fayha",
        "al hazm": "al hazem",
        "al khaleej club": "al khaleej",
        "al kholood club": "al kholood",
        "al najma (ksa)": "al najma",
        "al nassr": "al nassr fc",
        "al riyadh sc": "al riyadh",
        "damak": "damac fc",
        "ettifaq fc": "al-ettifaq",
        "ittihad jeddah": "al ittihad",
        "qadisiya khubar": "al qadsiah",
        "shabab riyadh": "al shabab",
        "taawoun": "al-taawoun",
    # Turkish team names - keys are already diacritic-stripped (post-NFD)
    "genclerbirligi": "genclerbirligi",
    "fenerbahce": "fenerbahce",
    "goztepe": "goztepe izmir",
    "goztep": "goztepe izmir",
    "caykur rizespor": "rizespor",
    "eyupspor": "eyupspor",
    "fatih karagumruk": "karagumruk",
    "basaksehir": "istanbul basaksehir",
    "istanbul basaksehir": "istanbul basaksehir",
    "buyuksehyr": "istanbul basaksehir",
    "kasimpasa": "kasimpasa",
}


# Maps football-data ASCII-garbled Turkish team names to proper display forms.
# Keys are lowercase as they appear in the CSV (football-data source).
DISPLAY_NAME_OVERRIDES = {
    "afc bournemouth": "Bournemouth",
    "besiktas": "Beşiktaş",
    "buyuksehyr": "Istanbul Başakşehir",
    "istanbul basaksehir": "Istanbul Başakşehir",
    "eyupspor": "Eyüpspor",
    "fenerbahce": "Fenerbahçe",
    "genclerbirligi": "Gençlerbirligi",
    "goztep": "Göztepe",
    "karagumruk": "Fatih Karagümrük",
    "kasimpasa": "Kasımpaşa",
    "rizespor": "Çaykur Rizespor",
}


def _display_team_name(name):
    """Return the proper display form for a team name, fixing football-data garbled names."""
    raw = (name or "").strip()
    return DISPLAY_NAME_OVERRIDES.get(raw.lower(), raw)


def _format_kickoff_eat_from_csv(date_raw, time_raw):
    """Convert a CSV UTC kickoff date/time pair to an EAT display string."""
    date_value = (date_raw or "").strip()
    time_value = (time_raw or "").strip() or "00:00"
    if not date_value:
        return "TBD"
    try:
        utc_dt = datetime.strptime(f"{date_value} {time_value}", "%d/%m/%Y %H:%M").replace(tzinfo=timezone.utc)
        eat_dt = utc_dt.astimezone(timezone(timedelta(hours=3)))
        return eat_dt.strftime("%d/%m/%Y %H:%M")
    except ValueError:
        return f"{date_value} {time_value}".strip()


def _format_kickoff_eat_from_iso(iso_raw):
    """Convert an ISO datetime string to an EAT display string."""
    value = (iso_raw or "").strip()
    if not value:
        return "TBD"
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        eat_dt = parsed.astimezone(timezone(timedelta(hours=3)))
        return eat_dt.strftime("%d/%m/%Y %H:%M")
    except ValueError:
        return value.replace("T", " ")[:16]


def _country_flag_url(country_name):
    code = COUNTRY_FLAG_CODES.get((country_name or "").strip())
    if not code:
        return ""
    return f"https://flagcdn.com/w40/{code}.png"


def _normalize_team_name(name):
    raw = (name or "").strip().lower()
    if not raw:
        return ""

    normalized = "".join(
        char for char in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(char)
    )
    normalized = " ".join(normalized.split())
    return TEAM_NAME_ALIASES.get(normalized, normalized)


def _fixture_identity(row):
    return (
        (row.get("Competition") or "").strip().casefold(),
        _normalize_team_name(row.get("HomeTeam") or ""),
        _normalize_team_name(row.get("AwayTeam") or ""),
    )


def _fixture_sort_datetime(row):
    date_raw = (row.get("Date") or "").strip()
    time_raw = (row.get("Time") or "00:00").strip() or "00:00"
    try:
        return datetime.strptime(f"{date_raw} {time_raw}", "%d/%m/%Y %H:%M")
    except ValueError:
        try:
            return datetime.strptime(date_raw, "%d/%m/%Y")
        except ValueError:
            return datetime.max


def _fixture_source_priority(row):
    return 2 if (row.get("Source") or "").strip() == "football-data" else 1


def _is_single_pair_season_competition(row):
    competition = (row.get("Competition") or "").strip()
    code = (row.get("Code") or "").strip()
    return competition == "Turkish Super Lig" or code == "T1"


def _prefer_fixture_row(current, candidate):
    current_played = _match_played(current)
    candidate_played = _match_played(candidate)

    # For Turkish Super Lig: fixturedownload has correct local dates and proper
    # diacritic-bearing team names, so always prefer it over football-data regardless
    # of which row has scores.
    if _is_single_pair_season_competition(current) and _is_single_pair_season_competition(candidate):
        current_src = (current.get("Source") or "").strip()
        candidate_src = (candidate.get("Source") or "").strip()
        if current_src != candidate_src:
            if current_src == "fixturedownload":
                return False  # keep current (fixturedownload)
            if candidate_src == "fixturedownload":
                return True   # upgrade to fixturedownload

    # For all other competitions: if one row has scores and the other doesn't, prefer the one with scores
    if candidate_played and not current_played:
        return True
    if not candidate_played and current_played:
        return False

    current_priority = _fixture_source_priority(current)
    candidate_priority = _fixture_source_priority(candidate)
    if candidate_priority != current_priority:
        return candidate_priority > current_priority

    return len((candidate.get("HomeTeam") or "").strip()) + len((candidate.get("AwayTeam") or "").strip()) > len((current.get("HomeTeam") or "").strip()) + len((current.get("AwayTeam") or "").strip())


def _dedupe_fixture_rows(rows):
    deduped = []
    grouped_indexes = {}

    for row in sorted(rows, key=_fixture_sort_datetime):
        fixture_id = _fixture_identity(row)
        fixture_dt = _fixture_sort_datetime(row)
        existing_index = grouped_indexes.get(fixture_id)
        if existing_index is None:
            grouped_indexes[fixture_id] = len(deduped)
            deduped.append(row)
            continue

        existing_row = deduped[existing_index]
        existing_dt = _fixture_sort_datetime(existing_row)
        if _is_single_pair_season_competition(row) and _is_single_pair_season_competition(existing_row):
            # Only merge when dates are within 4 days (handles UTC vs UTC+3 shifts).
            # Larger gaps mean a rescheduled / genuinely different matchday — treat as separate.
            if existing_dt != datetime.max and fixture_dt != datetime.max:
                day_diff = abs((fixture_dt.date() - existing_dt.date()).days)
                if day_diff <= 4:
                    if _prefer_fixture_row(existing_row, row):
                        deduped[existing_index] = row
                    continue
            # Dates too far apart — append as a new row
            grouped_indexes[fixture_id] = len(deduped)
            deduped.append(row)
            continue

        if existing_dt != datetime.max and fixture_dt != datetime.max:
            if abs((fixture_dt.date() - existing_dt.date()).days) <= 2:
                if _prefer_fixture_row(existing_row, row):
                    deduped[existing_index] = row
                continue

        grouped_indexes[fixture_id] = len(deduped)
        deduped.append(row)

    return deduped


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
        league = row.get("league", {})
        teams = row.get("teams", {})
        goals = row.get("goals", {})
        status = fixture.get("status", {})

        short = status.get("short") or "NS"
        elapsed = status.get("elapsed")
        is_live = short in {"1H", "2H", "HT", "ET", "BT", "P", "LIVE"}

        matches.append(
            {
                "league": league.get("name", "League"),
                "league_logo": league.get("logo", ""),
                "country": league.get("country", ""),
                "status": "LIVE" if is_live else short,
                "minute": f"{elapsed}'" if is_live and elapsed is not None else "",
                "home": (teams.get("home") or {}).get("name", "Home"),
                "home_logo": (teams.get("home") or {}).get("logo", ""),
                "away": (teams.get("away") or {}).get("name", "Away"),
                "away_logo": (teams.get("away") or {}).get("logo", ""),
                "home_score": goals.get("home", "-") if goals.get("home") is not None else "-",
                "away_score": goals.get("away", "-") if goals.get("away") is not None else "-",
                "kickoff": _format_kickoff_eat_from_iso(fixture.get("date", "")),
                "link": "https://www.api-football.com/",
            }
        )
    return matches


def _extract_leagues(rows):
    seen = OrderedDict()
    for row in rows:
        league = row.get("league", {})
        lid = league.get("id")
        if lid is None:
            continue
        if lid not in seen:
            seen[lid] = {
                "id": lid,
                "name": league.get("name", "Unknown"),
                "country": league.get("country", ""),
                "logo": league.get("logo", ""),
                "count": 0,
            }
        seen[lid]["count"] += 1
    return list(seen.values())


def _selected_date_from_request(request):
    raw_date = (request.GET.get("date") or "").strip()
    if not raw_date:
        return date.today(), False

    try:
        return datetime.strptime(raw_date, "%Y-%m-%d").date(), True
    except ValueError:
        return date.today(), False


def _group_matches_by_league(matches):
    grouped = OrderedDict()
    for match in matches:
        league_name = match.get("league") or "League"
        if league_name not in grouped:
            country_name = match.get("country", "")
            grouped[league_name] = {
                "league": league_name,
                "country": country_name,
                "flag_url": _country_flag_url(country_name),
                "matches": [],
            }
        grouped[league_name]["matches"].append(match)
    return list(grouped.values())


def _fetch_fixtures_for_date(selected_date):
    matches = []
    api_error = ""
    api_access_error = False
    selected_date_label = "Today" if selected_date == date.today() else f"{selected_date.day} {selected_date.strftime('%b %Y')}"
    seen_fixture_keys = set()

    csv_path = Path(settings.BASE_DIR) / "data" / "europe_top10_2025_2026.csv"
    target_date = selected_date.strftime("%d/%m/%Y")

    competitions_order = [
        ("Premier League", "England", "E0"),
        ("La Liga", "Spain", "SP1"),
        ("Serie A", "Italy", "I1"),
        ("Bundesliga", "Germany", "D1"),
        ("Ligue 1", "France", "F1"),
        ("Eredivisie", "Netherlands", "N1"),
        ("Primeira Liga", "Portugal", "P1"),
        ("Belgian Pro League", "Belgium", "B1"),
        ("Turkish Super Lig", "Turkey", "T1"),
        ("Scottish Premiership", "Scotland", "SC0"),
        ("UEFA Champions League", "Europe", "UCL"),
        ("UEFA Europa League", "Europe", "UEL"),
    ]
    league_counts = {name: 0 for name, _, _ in competitions_order}

    if not csv_path.exists():
        api_error = f"Europe CSV file not found: {csv_path}. Run scripts/scrape_europe_top10_2526.py first."
    else:
        try:
            rows = _load_europe_csv_rows()
            required = {"Competition", "Country", "Date", "Time", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}
            if not rows:
                rows = []
            csv_headers = set(rows[0].keys()) if rows else required
            if not required.issubset(csv_headers):
                missing = required.difference(csv_headers)
                api_error = f"Europe CSV is missing required columns: {', '.join(sorted(missing))}."
            else:
                for row in rows:
                    row_date = (row.get("Date") or "").strip()
                    if row_date != target_date:
                        continue

                    fixture_key = (
                        ((row.get("Competition") or "").strip().casefold()),
                        row_date,
                        _normalize_team_name(row.get("HomeTeam") or ""),
                        _normalize_team_name(row.get("AwayTeam") or ""),
                    )
                    if fixture_key in seen_fixture_keys:
                        continue
                    seen_fixture_keys.add(fixture_key)

                    competition_name = (row.get("Competition") or "").strip() or "League"
                    country_name = (row.get("Country") or "").strip()
                    if competition_name in league_counts:
                        league_counts[competition_name] += 1
                    else:
                        league_counts[competition_name] = 1

                    home_goals_raw = (row.get("FTHG") or "").strip()
                    away_goals_raw = (row.get("FTAG") or "").strip()
                    played = home_goals_raw != "" and away_goals_raw != ""

                    matches.append(
                        {
                            "league": competition_name,
                            "league_logo": "",
                            "country": country_name,
                            "status": "FT" if played else "NS",
                            "minute": "",
                            "home": _display_team_name((row.get("HomeTeam") or "Home").strip()),
                            "home_logo": "",
                            "away": _display_team_name((row.get("AwayTeam") or "Away").strip()),
                            "away_logo": "",
                            "home_score": home_goals_raw if played else "-",
                            "away_score": away_goals_raw if played else "-",
                            "kickoff": _format_kickoff_eat_from_csv(row_date, (row.get("Time") or "").strip()),
                            "link": "#",
                        }
                    )
        except OSError as exc:
            api_error = f"Unable to read Europe CSV: {exc}"

    leagues = [
        {"id": code, "name": name, "country": country, "logo": "", "count": league_counts.get(name, 0)}
        for name, country, code in competitions_order
    ]

    if not matches and not api_error:
        api_error = f"No fixtures scheduled for {selected_date_label.lower()} across the selected competitions."

    return {
        "matches": matches,
        "match_groups": _group_matches_by_league(matches),
        "leagues": leagues,
        "api_error": api_error,
        "api_access_error": api_access_error,
        "selected_date_label": selected_date_label,
    }


def _parse_csv_date(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y").date()
    except ValueError:
        return None


def _parse_forebet_result(value):
    raw = (value or "").strip()
    if not raw:
        return None

    match = re.match(r"^(?P<home>.+?)\s+(?P<home_score>\d+)\s+-\s+(?P<away_score>\d+)\s+(?P<away>.+)$", raw)
    if not match:
        return None

    return {
        "home": _display_team_name(match.group("home")),
        "away": _display_team_name(match.group("away")),
        "home_score": match.group("home_score"),
        "away_score": match.group("away_score"),
    }


def _read_forebet_results_for_date(csv_path, target_date):
    if not csv_path.exists():
        return None, f"Results file not found: {csv_path.name}."

    matches = []
    try:
        with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            for row in csv.DictReader(f):
                if (row.get("Date") or "").strip() != target_date:
                    continue
                parsed_result = _parse_forebet_result(row.get("Result") or "")
                if not parsed_result:
                    continue

                matches.append(
                    {
                        "home": parsed_result["home"],
                        "away": parsed_result["away"],
                        "status": "FT",
                        "home_score": parsed_result["home_score"],
                        "away_score": parsed_result["away_score"],
                        "kickoff": "",
                    }
                )
    except OSError as exc:
        return None, f"Could not read results CSV: {exc}"

    return matches, ""


def _read_livescore_fixtures_for_date(csv_path, target_date):
    if not csv_path.exists():
        return None, f"Fixtures file not found: {csv_path.name}."

    matches = []
    seen_ids = set()
    finished_markers = {"FT", "AET", "PEN", "AP", "FULL TIME", "ENDED"}
    not_finished_markers = {"NS", "POSTP.", "PST", "CANC.", "ABN.", "TBD"}
    excluded_markers = {"POSTP.", "PST", "CANC.", "ABN."}

    try:
        with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            for row in csv.DictReader(f):
                if (row.get("Date") or "").strip() != target_date:
                    continue

                fixture_id = (row.get("EventId") or "").strip()
                if fixture_id and fixture_id in seen_ids:
                    continue
                if fixture_id:
                    seen_ids.add(fixture_id)

                home = _display_team_name((row.get("Home") or "").strip())
                away = _display_team_name((row.get("Away") or "").strip())
                if not home or not away:
                    continue

                status = (row.get("Status") or "").strip() or "NS"
                kickoff = (row.get("Time") or "").strip()
                home_score = (row.get("HomeScore") or "").strip()
                away_score = (row.get("AwayScore") or "").strip()

                status_upper = status.upper()
                
                # Skip postponed, rescheduled, cancelled, or abandoned matches
                if status_upper in excluded_markers:
                    continue

                has_scores = home_score != "" and away_score != ""
                is_finished = status_upper in finished_markers or (
                    has_scores and status_upper not in not_finished_markers
                )

                if is_finished:
                    status = "FT"
                    kickoff = ""
                elif not kickoff:
                    kickoff = "TBD"

                matches.append(
                    {
                        "home": home,
                        "away": away,
                        "status": status,
                        "home_score": home_score if home_score else "-",
                        "away_score": away_score if away_score else "-",
                        "kickoff": kickoff,
                    }
                )
    except OSError as exc:
        return None, f"Could not read fixtures CSV: {exc}"

    return matches, ""


def _fetch_livescore_epl_events(timeout=20):
    endpoint = "https://prod-cdn-public-api.lsmedia1.com/v1/api/app/competition/65/fixtures-w/0?locale=en&countryCode=GB&limit=200"
    try:
        response = requests.get(
            endpoint,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; LiveScoreEPLFixtures/1.0)"},
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        return None, f"Could not fetch LiveScore EPL fixtures: {exc}"
    except ValueError as exc:
        return None, f"Could not parse LiveScore EPL fixtures response: {exc}"

    stages = payload.get("Stages") or []
    if not stages:
        return [], ""

    events = stages[0].get("Events") or []
    return events, ""


def _map_livescore_events_for_date(events, selected_date):
    selected_key = selected_date.strftime("%Y%m%d")
    window_end = selected_date + timedelta(days=62)
    parsed_events = []

    for event in events:
        start_raw = str(event.get("Esd") or "").strip()
        if len(start_raw) < 12:
            continue

        try:
            event_dt = datetime.strptime(start_raw[:12], "%Y%m%d%H%M")
        except ValueError:
            continue

        home = ((event.get("T1") or [{}])[0].get("Nm") or "").strip()
        away = ((event.get("T2") or [{}])[0].get("Nm") or "").strip()
        if not home or not away:
            continue

        status_text = (event.get("Eps") or "").strip()
        if status_text in {"", "NS"}:
            status = "NS"
            kickoff = event_dt.strftime("%H:%M")
        elif status_text == "FT":
            status = "FT"
            kickoff = ""
        else:
            status = "NS"
            kickoff = status_text

        parsed_events.append(
            {
            "event_dt": event_dt,
                "event_date": event_dt.date(),
                "home": _display_team_name(home),
                "away": _display_team_name(away),
                "status": status,
                "home_score": "-",
                "away_score": "-",
                "kickoff": kickoff,
            }
        )

    exact_day = [m for m in parsed_events if m["event_date"].strftime("%Y%m%d") == selected_key]
    if exact_day:
        exact_day.sort(key=lambda m: m["event_dt"])
        return [{k: v for k, v in m.items() if k not in {"event_date", "event_dt"}} for m in exact_day]

    upcoming = [m for m in parsed_events if selected_date <= m["event_date"] <= window_end]
    upcoming.sort(key=lambda m: m["event_dt"])
    return [{k: v for k, v in m.items() if k not in {"event_date", "event_dt"}} for m in upcoming]


def _load_league_logo_map(csv_filename):
    csv_path = Path(settings.BASE_DIR) / "data" / csv_filename
    if not csv_path.exists():
        return {}

    logo_map = {}
    try:
        with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            for row in csv.DictReader(f):
                team_name = (row.get("Team") or "").strip()
                logo_url = (row.get("LogoUrl") or "").strip()
                if not team_name or not logo_url:
                    continue
                logo_map[_normalize_team_name(team_name)] = logo_url
    except OSError:
        return {}

    return logo_map


def _load_europe_csv_rows():
    csv_path = Path(settings.BASE_DIR) / "data" / "europe_top10_2025_2026.csv"
    if not csv_path.exists():
        return []
    try:
        with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as csv_file:
            return _dedupe_fixture_rows(list(csv.DictReader(csv_file)))
    except OSError:
        return []


def _match_played(row):
    return (row.get("FTHG") or "").strip() != "" and (row.get("FTAG") or "").strip() != ""


def _build_team_form(rows, team_name, cutoff_date, limit=5, venue=None):
    if not team_name:
        return []

    results = []
    seen_matches = set()
    team_key = _normalize_team_name(team_name)
    sorted_rows = sorted(rows, key=lambda r: (_parse_csv_date(r.get("Date")) or date.min, (r.get("Time") or "")))
    for row in reversed(sorted_rows):
        row_date = _parse_csv_date(row.get("Date"))
        if not row_date or row_date > cutoff_date or not _match_played(row):
            continue

        home = (row.get("HomeTeam") or "").strip()
        away = (row.get("AwayTeam") or "").strip()
        home_key = _normalize_team_name(home)
        away_key = _normalize_team_name(away)
        if home_key != team_key and away_key != team_key:
            continue

        home_goals = int((row.get("FTHG") or "0").strip() or "0")
        away_goals = int((row.get("FTAG") or "0").strip() or "0")
        is_home = home_key == team_key
        team_goals = home_goals if is_home else away_goals
        opp_goals = away_goals if is_home else home_goals

        # Filter by venue if specified
        match_venue = "H" if is_home else "A"
        if venue and match_venue != venue:
            continue

        match_key = (row_date.isoformat(), match_venue, team_goals, opp_goals)
        if match_key in seen_matches:
            continue
        seen_matches.add(match_key)

        row_dt = _fixture_sort_datetime(row)

        if team_goals > opp_goals:
            result = "W"
        elif team_goals < opp_goals:
            result = "L"
        else:
            result = "D"

        results.append(
            {
                "_sort_dt": row_dt,
                "date": row_date.strftime("%d %b %Y"),
                "date_short": row_date.strftime("%d/%m"),
                "date_year": row_date.strftime("%Y"),
                "opponent": away if is_home else home,
                "venue": match_venue,
                "score": f"{team_goals}-{opp_goals}",
                "result": result,
                "team_goals": team_goals,
                "opp_goals": opp_goals,
                "btts": team_goals > 0 and opp_goals > 0,
                "over_25": (team_goals + opp_goals) > 2,
            }
        )

    results.sort(key=lambda item: item.get("_sort_dt") or datetime.min, reverse=True)
    trimmed = results[:limit]
    for item in trimmed:
        item.pop("_sort_dt", None)
    return trimmed


def _build_h2h(rows, home_team, away_team, cutoff_date, limit=8):
    history = []
    sorted_rows = sorted(rows, key=lambda r: (_parse_csv_date(r.get("Date")) or date.min, (r.get("Time") or "")))
    for row in reversed(sorted_rows):
        row_date = _parse_csv_date(row.get("Date"))
        if not row_date or row_date > cutoff_date or not _match_played(row):
            continue

        home = (row.get("HomeTeam") or "").strip()
        away = (row.get("AwayTeam") or "").strip()
        pair_matches = (home == home_team and away == away_team) or (home == away_team and away == home_team)
        if not pair_matches:
            continue

        home_goals = int((row.get("FTHG") or "0").strip() or "0")
        away_goals = int((row.get("FTAG") or "0").strip() or "0")
        history.append(
            {
                "date": row_date.strftime("%d %b %Y"),
                "competition": (row.get("Competition") or "League").strip(),
                "home": home,
                "away": away,
                "score": f"{home_goals}-{away_goals}",
            }
        )
        if len(history) >= limit:
            break

    return history


def _build_league_table(rows, league_name, cutoff_date):
    if not league_name:
        return []

    table = {}

    def ensure(name):
        if name not in table:
            table[name] = {
                "team": name,
                "played": 0,
                "wins": 0,
                "draws": 0,
                "losses": 0,
                "gf": 0,
                "ga": 0,
                "gd": 0,
                "pts": 0,
            }
        return table[name]

    for row in rows:
        if (row.get("Competition") or "").strip() != league_name:
            continue
        row_date = _parse_csv_date(row.get("Date"))
        if not row_date or row_date > cutoff_date or not _match_played(row):
            continue

        home = (row.get("HomeTeam") or "").strip()
        away = (row.get("AwayTeam") or "").strip()
        if not home or not away:
            continue

        home_goals = int((row.get("FTHG") or "0").strip() or "0")
        away_goals = int((row.get("FTAG") or "0").strip() or "0")

        home_row = ensure(home)
        away_row = ensure(away)

        home_row["played"] += 1
        away_row["played"] += 1
        home_row["gf"] += home_goals
        home_row["ga"] += away_goals
        away_row["gf"] += away_goals
        away_row["ga"] += home_goals

        if home_goals > away_goals:
            home_row["wins"] += 1
            home_row["pts"] += 3
            away_row["losses"] += 1
        elif home_goals < away_goals:
            away_row["wins"] += 1
            away_row["pts"] += 3
            home_row["losses"] += 1
        else:
            home_row["draws"] += 1
            away_row["draws"] += 1
            home_row["pts"] += 1
            away_row["pts"] += 1

    table_rows = list(table.values())
    for item in table_rows:
        item["gd"] = item["gf"] - item["ga"]

    table_rows.sort(key=lambda x: (-x["pts"], -x["gd"], -x["gf"], x["team"]))
    for index, item in enumerate(table_rows, start=1):
        item["rank"] = index

    return table_rows


def _resolve_europe_league_name(raw_league_name):
    value = (raw_league_name or "").strip()
    if not value:
        return ""

    normalized = _normalize_team_name(value)
    mapping = {
        "premier league": "Premier League",
        "la liga": "La Liga",
        "bundesliga": "Bundesliga",
        "serie a": "Serie A",
        "ligue 1": "Ligue 1",
        "eredivisie": "Eredivisie",
        "liga portugal": "Primeira Liga",
        "primeira liga": "Primeira Liga",
        "super lig": "Turkish Super Lig",
        "turkish super lig": "Turkish Super Lig",
        "saudi pro league": "Saudi Pro League",
    }
    return mapping.get(normalized, value)


def _resolve_team_name_from_rows(rows, requested_name):
    requested = (requested_name or "").strip()
    if not requested:
        return ""

    requested_key = _normalize_team_name(requested)
    for row in rows:
        home = (row.get("HomeTeam") or "").strip()
        away = (row.get("AwayTeam") or "").strip()
        if home and _normalize_team_name(home) == requested_key:
            return home
        if away and _normalize_team_name(away) == requested_key:
            return away
    return requested


def _load_logo_map_for_league(league_name):
    table_file_by_league = {
        "Premier League": "epl_fotmob_table_2025_2026.csv",
        "La Liga": "laliga_fotmob_table_2025_2026.csv",
        "Bundesliga": "bundesliga_fotmob_table_2025_2026.csv",
        "Serie A": "seriea_fotmob_table_2025_2026.csv",
        "Ligue 1": "ligue1_fotmob_table_2025_2026.csv",
        "Eredivisie": "eredivisie_fotmob_table_2025_2026.csv",
        "Primeira Liga": "ligaportugal_fotmob_table_2025_2026.csv",
        "Turkish Super Lig": "superlig_fotmob_table_2025_2026.csv",
        "Saudi Pro League": "saudi_fotmob_table_2025_2026.csv",
    }

    filename = table_file_by_league.get((league_name or "").strip())
    if not filename:
        return {}
    return _load_league_logo_map(filename)


def _format_last_five_display_score(item):
    venue = (item.get("venue") or "").strip().upper()
    team_goals = item.get("team_goals", 0)
    opp_goals = item.get("opp_goals", 0)
    if venue == "A":
        return f"{opp_goals}:{team_goals}"
    return f"{team_goals}:{opp_goals}"


def _serialize_last_five_with_logos(rows, team_name, cutoff_date, logo_map):
    team_form = _build_team_form(rows, team_name, cutoff_date, limit=5)
    team_logo = logo_map.get(_normalize_team_name(team_name), "")

    serialized = []
    for item in team_form:
        opponent = item.get("opponent") or ""
        opponent_logo = logo_map.get(_normalize_team_name(opponent), "")
        serialized.append(
            {
                "score": _format_last_five_display_score(item),
                "result": item.get("result", ""),
                "venue": item.get("venue", ""),
                "team_logo": team_logo,
                "opponent_logo": opponent_logo,
            }
        )

    return serialized


def _serialize_last_five_with_logos_by_venue(rows, team_name, cutoff_date, logo_map, venue):
    team_form = _build_team_form(rows, team_name, cutoff_date, limit=5, venue=venue)
    team_logo = logo_map.get(_normalize_team_name(team_name), "")

    serialized = []
    for item in team_form:
        opponent = item.get("opponent") or ""
        opponent_logo = logo_map.get(_normalize_team_name(opponent), "")
        serialized.append(
            {
                "score": _format_last_five_display_score(item),
                "result": item.get("result", ""),
                "venue": item.get("venue", ""),
                "team_logo": team_logo,
                "opponent_logo": opponent_logo,
            }
        )

    return serialized


def match_last_five_api(request):
    selected_date, has_explicit_date = _selected_date_from_request(request)
    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    raw_home = (request.GET.get("home") or "").strip()
    raw_away = (request.GET.get("away") or "").strip()
    raw_league = (request.GET.get("league") or "").strip()
    if not raw_home or not raw_away:
        return JsonResponse({"error": "Missing required query parameters: home and away."}, status=400)

    rows = _load_europe_csv_rows()
    if not rows:
        return JsonResponse(
            {
                "home": _display_team_name(raw_home),
                "away": _display_team_name(raw_away),
                "home_last_five": [],
                "away_last_five": [],
                "home_home_last_five": [],
                "away_away_last_five": [],
            }
        )

    resolved_league = _resolve_europe_league_name(raw_league)
    league_rows = rows
    if resolved_league:
        filtered_rows = [row for row in rows if (row.get("Competition") or "").strip() == resolved_league]
        if filtered_rows:
            league_rows = filtered_rows

    resolved_home = _resolve_team_name_from_rows(league_rows, raw_home)
    resolved_away = _resolve_team_name_from_rows(league_rows, raw_away)
    logo_map = _load_logo_map_for_league(resolved_league)

    league_display = resolved_league
    if resolved_league == "Premier League":
        league_display = "EPL"

    return JsonResponse(
        {
            "home": _display_team_name(resolved_home),
            "away": _display_team_name(resolved_away),
            "league": league_display,
            "home_last_five": _serialize_last_five_with_logos(league_rows, resolved_home, selected_date, logo_map),
            "away_last_five": _serialize_last_five_with_logos(league_rows, resolved_away, selected_date, logo_map),
            "home_home_last_five": _serialize_last_five_with_logos_by_venue(league_rows, resolved_home, selected_date, logo_map, "H"),
            "away_away_last_five": _serialize_last_five_with_logos_by_venue(league_rows, resolved_away, selected_date, logo_map, "A"),
        }
    )


def epl_fixtures_api(request):
    """Return Premier League matches for a given date, merging forebet results and livescore fixtures."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    logo_map = _load_league_logo_map("epl_fotmob_table_2025_2026.csv")

    # Load completed results from forebet CSV first.
    results_csv = Path(settings.BASE_DIR) / "data" / "epl_forebet_results_2025_2026.csv"
    forebet_matches, _ = _read_forebet_results_for_date(results_csv, target_date)
    forebet_matches = forebet_matches or []

    # Load fixtures (upcoming and any completed livescore entries) from livescore CSV.
    fixtures_csv = Path(settings.BASE_DIR) / "data" / "epl_livescore_fixtures_2025_2026.csv"
    livescore_matches, error = _read_livescore_fixtures_for_date(fixtures_csv, target_date)
    if livescore_matches is None:
        livescore_matches = []

    # Merge: forebet results take priority; livescore provides upcoming (NS) matches not in forebet.
    forebet_keys = {
        (_normalize_team_name(m.get("home") or ""), _normalize_team_name(m.get("away") or ""))
        for m in forebet_matches
    }
    extra_fixtures = [
        m for m in livescore_matches
        if m.get("status") != "FT"
        and (_normalize_team_name(m.get("home") or ""), _normalize_team_name(m.get("away") or ""))
        not in forebet_keys
    ]
    matches = forebet_matches + extra_fixtures

    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def saudi_results_api(request):
    """Return Saudi Pro League results from the Forebet Saudi results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "saudi_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Saudi Pro League results: {error}"}, status=500)

    logo_map = _load_league_logo_map("saudi_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def laliga_results_api(request):
    """Return La Liga results from the Forebet La Liga results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "laliga_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet La Liga results: {error}"}, status=500)

    logo_map = _load_league_logo_map("laliga_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def bundesliga_results_api(request):
    """Return Bundesliga results from the Forebet Bundesliga results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "bundesliga_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Bundesliga results: {error}"}, status=500)

    logo_map = _load_league_logo_map("bundesliga_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def seriea_results_api(request):
    """Return Serie A results from the Forebet Serie A results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "seriea_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Serie A results: {error}"}, status=500)

    logo_map = _load_league_logo_map("seriea_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def ligue1_results_api(request):
    """Return Ligue 1 results from the Forebet Ligue 1 results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "ligue1_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Ligue 1 results: {error}"}, status=500)

    logo_map = _load_league_logo_map("ligue1_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def eredivisie_results_api(request):
    """Return Eredivisie results from the Forebet Eredivisie results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "eredivisie_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Eredivisie results: {error}"}, status=500)

    logo_map = _load_league_logo_map("eredivisie_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def ligaportugal_results_api(request):
    """Return Liga Portugal results from the Forebet Liga Portugal results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "ligaportugal_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Liga Portugal results: {error}"}, status=500)

    logo_map = _load_league_logo_map("ligaportugal_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


def superlig_results_api(request):
    """Return Turkiye Super Lig results from the Forebet Super Lig results CSV for a given date."""
    selected_date, has_explicit_date = _selected_date_from_request(request)

    if request.GET.get("date") and not has_explicit_date:
        return JsonResponse({"error": "Invalid date format. Use ?date=YYYY-MM-DD."}, status=400)

    target_date = selected_date.strftime("%d/%m/%Y")
    csv_path = Path(settings.BASE_DIR) / "data" / "superlig_forebet_results_2025_2026.csv"
    matches, error = _read_forebet_results_for_date(csv_path, target_date)
    if matches is None:
        return JsonResponse({"error": f"Could not read Forebet Super Lig results: {error}"}, status=500)

    logo_map = _load_league_logo_map("superlig_fotmob_table_2025_2026.csv")
    for match in matches:
        home_key = _normalize_team_name(match.get("home") or "")
        away_key = _normalize_team_name(match.get("away") or "")
        match["home_logo"] = logo_map.get(home_key, "")
        match["away_logo"] = logo_map.get(away_key, "")

    return JsonResponse({"date": selected_date.isoformat(), "matches": matches})


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
            "match_groups": data["match_groups"],
            "api_error": data["api_error"],
            "api_access_error": data["api_access_error"],
            "selected_date": selected_date.isoformat(),
            "selected_date_label": data["selected_date_label"],
        },
    )


def csv_view(request):
    csv_path = Path(settings.BASE_DIR) / "data" / "europe_top10_2025_2026.csv"
    headers = []
    rows = []
    error = ""
    max_rows = 300

    selected_team = (request.GET.get("team") or "").strip()
    all_teams_mode = not selected_team or selected_team.lower() == "all"
    team_name = "All Teams" if all_teams_mode else selected_team

    def _new_bucket():
        return {"played": 0, "wins": 0, "draws": 0, "losses": 0, "gf": 0, "ga": 0}

    team_summary = {"overall": _new_bucket(), "home": _new_bucket(), "away": _new_bucket()}
    home_results = []
    away_results = []
    overall_results = []
    available_teams = set()

    def _to_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _result_letter(gf, ga):
        if gf > ga:
            return "W"
        if gf < ga:
            return "L"
        return "D"

    def _apply_stats(bucket, gf, ga):
        bucket["played"] += 1
        bucket["gf"] += gf
        bucket["ga"] += ga
        if gf > ga:
            bucket["wins"] += 1
        elif gf < ga:
            bucket["losses"] += 1
        else:
            bucket["draws"] += 1

    if not csv_path.exists():
        error = f"CSV file not found: {csv_path}"
    else:
        try:
            with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as csv_file:
                reader = csv.DictReader(csv_file)
                headers = list(reader.fieldnames or [])
                required = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}

                if not headers:
                    error = "CSV file is empty."
                elif not required.issubset(set(headers)):
                    error = "CSV is missing required columns: Date, HomeTeam, AwayTeam, FTHG, FTAG"
                else:
                    for raw in reader:
                        row_values = [raw.get(header, "") for header in headers]
                        if len(rows) < max_rows:
                            rows.append(row_values)

                        home_team = (raw.get("HomeTeam") or "").strip()
                        away_team = (raw.get("AwayTeam") or "").strip()
                        if home_team:
                            available_teams.add(home_team)
                        if away_team:
                            available_teams.add(away_team)

                        if not home_team or not away_team or not _match_played(raw):
                            continue

                        home_goals = _to_int(raw.get("FTHG"))
                        away_goals = _to_int(raw.get("FTAG"))
                        score_value = f"{home_goals}-{away_goals}"
                        date_value = raw.get("Date") or ""

                        home_entry = {
                            "date": date_value,
                            "home": home_team,
                            "away": away_team,
                            "score": score_value,
                            "result": _result_letter(home_goals, away_goals),
                            "team": home_team,
                        }
                        away_entry = {
                            "date": date_value,
                            "home": home_team,
                            "away": away_team,
                            "score": score_value,
                            "result": _result_letter(away_goals, home_goals),
                            "team": away_team,
                        }

                        if all_teams_mode:
                            home_results.append(home_entry)
                            away_results.append(away_entry)
                            overall_results.append(home_entry)
                            overall_results.append(away_entry)
                            _apply_stats(team_summary["home"], home_goals, away_goals)
                            _apply_stats(team_summary["away"], away_goals, home_goals)
                            _apply_stats(team_summary["overall"], home_goals, away_goals)
                            _apply_stats(team_summary["overall"], away_goals, home_goals)
                        else:
                            if home_team == selected_team:
                                home_results.append(home_entry)
                                overall_results.append(home_entry)
                                _apply_stats(team_summary["home"], home_goals, away_goals)
                                _apply_stats(team_summary["overall"], home_goals, away_goals)
                            elif away_team == selected_team:
                                away_results.append(away_entry)
                                overall_results.append(away_entry)
                                _apply_stats(team_summary["away"], away_goals, home_goals)
                                _apply_stats(team_summary["overall"], away_goals, home_goals)
        except OSError as exc:
            error = f"Unable to read CSV file: {exc}"

    col_count = max([len(headers)] + [len(row) for row in rows], default=0)
    if col_count and len(headers) < col_count:
        headers = headers + [f"Column {i}" for i in range(len(headers) + 1, col_count + 1)]

    normalized_rows = [row + [""] * (col_count - len(row)) for row in rows]

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
            "selected_team": selected_team,
            "available_teams": sorted(available_teams),
            "team_summary": team_summary,
            "home_results": home_results[:60],
            "away_results": away_results[:60],
            "overall_results": overall_results[:80],
        },
    )
