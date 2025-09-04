# scripts/scrape_defenders.py
"""
Scrape Bundesliga defender transfer rumours from Transfermarkt (detailed view).

- Robust to nested <td> in player cell (uses recursive=False)
- Maps columns by reading table headers (EN/DE), with sensible fallbacks
- Extracts clubs from <a href*="/verein/"> reliably
- Prefers external source links; falls back to internal normalized URLs
- Filters to defender positions using expanded English/German keywords
- Outputs JSON and an HTML summary table

Env vars:
  COMPETITION (default "L1")
  SEASON_ID  (default "2025")
"""

import os
import re
import json
import datetime
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.transfermarkt.com"
COMPETITION = os.getenv("COMPETITION", "L1")
SEASON_ID = os.getenv("SEASON_ID", "2025")
URL = f"{BASE_URL}/bundesliga/geruechte/wettbewerb/{COMPETITION}/saison_id/{SEASON_ID}/plus/1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DefenderRumoursBot/1.2",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9,de;q=0.7",
    "Referer": BASE_URL + "/",
    "Connection": "keep-alive",
}
TIMEOUT = 30

# Expanded defender keywords (case-insensitive).
DEFENDER_KEYWORDS = {
    # English
    "defender", "centre-back", "center-back", "centre back", "center back",
    "left-back", "left back", "right-back", "right back",
    "wing-back", "wingback", "full-back", "fullback",
    # German (common forms)
    "innenverteidiger", "rechter verteidiger", "linker verteidiger",
    "außenverteidiger", "aussenverteidiger", "verteidiger",
}

# Header synonyms to map columns robustly (EN + DE)
HEADER_ALIASES = {
    "player": {"player", "spieler"},
    "current_club": {"current club", "aktueller verein", "verein"},
    "interested_club": {"interested club", "interessent", "interessenten"},
    "source": {"source", "quelle"},
    "probability": {"probability", "wahrscheinlichkeit"},
}


def fetch_with_retries(url: str, max_retries: int = 3, backoff: float = 1.5) -> requests.Response:
    """GET with retry adapter + manual backoff."""
    s = requests.Session()
    # Attach retry adapter if available
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry  # type: ignore
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
    except Exception:
        pass

    last_exc = None
    for i in range(1, max_retries + 1):
        try:
            resp = s.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if i < max_retries:
                time.sleep(backoff * i)
    # If we reach here, raise last error
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown error performing GET")


def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def text(el) -> str:
    return norm(el.get_text(" ", strip=True)) if el else ""


def is_defender(position_text: str) -> bool:
    p = (position_text or "").lower()
    return any(k in p for k in DEFENDER_KEYWORDS)


def parse_probability(s: Optional[str]) -> Optional[int]:
    m = re.search(r"(\d{1,3})\s*%", s or "")
    if m:
        val = int(m.group(1))
        if 0 <= val <= 100:
            return val
    return None


def map_columns(table: BeautifulSoup) -> Dict[str, int]:
    """
    Inspect the table header and map known columns to indices.
    If headers are missing or icon-only, fall back to typical indices.
    """
    mapping: Dict[str, int] = {}
    thead = table.find("thead")
    if thead:
        headers = [text(th).lower() for th in thead.select("tr th")]
        for idx, h in enumerate(headers):
            for key, aliases in HEADER_ALIASES.items():
                if any(alias in h for alias in aliases):
                    mapping[key] = idx

    # Ensure required minimum mapping with sensible defaults
    mapping.setdefault("player", 0)
    mapping.setdefault("current_club", 2)
    mapping.setdefault("interested_club", 3)
    mapping.setdefault("source", 4)
    mapping.setdefault("probability", 5)
    return mapping


def extract_position_from_player_cell(player_cell) -> str:
    """
    Try to extract a position string from within the player cell (detailed layout).
    """
    # Look into common sub-elements first
    for sel in ["table.inline-table td", "small", "span", ".position"]:
        for el in player_cell.select(sel):
            t = text(el)
            if t and any(k in t.lower() for k in ("back", "verteidiger", "defender")):
                return t
    # Fallback: regex scan of the whole cell
    t_all = text(player_cell)
    m = re.search(
        r"(?:[A-Za-zÄÖÜäöüß\- ]*(?:back|verteidiger|defender)[A-Za-zÄÖÜäöüß\- ]*)",
        t_all, flags=re.IGNORECASE
    )
    return m.group(0) if m else ""


def first_club_from_cell(cell) -> str:
    a = cell.select_one('a[href*="/startseite/verein/"], a[href*="/verein/"]')
    return text(a) if a else text(cell)


def interested_clubs_from_cell(cell) -> str:
    names = [text(a) for a in cell.select('a[href*="/startseite/verein/"], a[href*="/verein/"]')]
    if names:
        # Return the first club (schema expects single string)
        return names[0]
    return text(cell)


def source_href_from_cell(cell) -> str:
    # Prefer external sources; else fall back to internal link (normalized with urljoin)
    for a in cell.select('a[href]'):
        href = a.get("href") or ""
        if href.startswith("http") and "transfermarkt." not in href:
            return href
    a = cell.select_one("a[href]")
    if a:
        raw = a.get("href") or ""
        return urljoin(URL, raw) if raw else ""
    return ""


def render_html(items: List[dict]) -> str:
    def esc(s: str) -> str:
        return (
            s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;")
        )

    rows = []
    for it in items:
        prob_disp = "" if it["probability"] is None else f'{it["probability"]}%'
        link_html = "" if not it["source_link"] else f'<a href="{esc(it["source_link"])}">Source</a>'
        rows.append(
            "<tr>"
            f"<td>{esc(it['player'])}</td>"
            f"<td>{esc(it['position'])}</td>"
            f"<td>{esc(it['current_club'])}</td>"
            f"<td>{esc(it['interested_club'])}</td>"
            f"<td style='text-align:center'>{esc(prob_disp)}</td>"
            f"<td>{link_html}</td>"
            "</tr>"
        )
    table = (
        "<table border='1' cellspacing='0' cellpadding='6' "
        "style='border-collapse:collapse;font-family:Segoe UI,Arial,Helvetica,sans-serif;font-size:14px;'>"
        "<thead style='background:#f3f4f6'>"
        "<tr><th>Player</th><th>Position</th><th>Current</th><th>Interested</th><th>Prob</th><th>Link</th></tr>"
        "</thead>"
        "<tbody>" + "\n".join(rows) + "</tbody></table>"
    )
    title = "Bundesliga Defender Rumours"
    return f"<html><body><h3 style='font-family:Segoe UI,Arial,Helvetica,sans-serif'>{title}</h3>{table}</body></html>"


def main():
    resp = fetch_with_retries(URL)
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    table = soup.select_one("table.items")
    rows = table.select("tbody > tr") if table else []
    parsed_rows: List[dict] = []
    items: List[dict] = []

    col_idx = map_columns(table) if table else {}

    for tr in rows:
        # CRITICAL: only top-level td's; ignore nested inline-table cells
        tds = tr.find_all("td", recursive=False)
        if not tds:
            continue

        # Player
        p_idx = col_idx.get("player", 0)
        player_cell = tds[p_idx] if len(tds) > p_idx else None
        if not player_cell:
            continue
        player_link = player_cell.select_one("a[href*='/profil/spieler/']")
        player = text(player_link) if player_link else text(player_cell)

        # Position heuristics from the player cell
        pos_txt = extract_position_from_player_cell(player_cell)

        # Row-level text for fallbacks
        row_txt = " ".join(text(td) for td in tds)
        if not pos_txt and is_defender(row_txt):
            m = re.search(
                r"(?:[A-Za-zÄÖÜäöüß\- ]*(?:back|verteidiger|defender)[A-Za-zÄÖÜäöüß\- ]*)",
                row_txt, flags=re.IGNORECASE
            )
            pos_txt = m.group(0) if m else ""

        # Clubs
        cur_idx = col_idx.get("current_club", 2)
        int_idx = col_idx.get("interested_club", 3)
        current_club = first_club_from_cell(tds[cur_idx]) if len(tds) > cur_idx else ""
        interested = interested_clubs_from_cell(tds[int_idx]) if len(tds) > int_idx else ""

        # Source link
        src_idx = col_idx.get("source", 4)
        source_cell = tds[src_idx] if len(tds) > src_idx else None
        source_link = source_href_from_cell(source_cell) if source_cell else ""

        # Probability (prefer cell, fallback to scanning entire row)
        pr_idx = col_idx.get("probability", 5)
        prob_txt_cell = text(tds[pr_idx]) if len(tds) > pr_idx else ""
        prob = parse_probability(prob_txt_cell) or parse_probability(row_txt)

        parsed_rows.append({
            "player": player,
            "player_cell_text": text(player_cell),
            "row_text": row_txt,
            "position_raw": pos_txt,
            "current_club": current_club,
            "interested_club": interested,
            "probability_raw": prob_txt_cell,
            "source_link": source_link,
            "probability": prob,
        })

    # Filter to defenders after parsing rows
    for r in parsed_rows:
        if is_defender(r.get("position_raw") or r.get("player_cell_text") or r.get("row_text")):
            items.append({
                "player": r["player"],
                "position": r["position_raw"] or "Defender",
                "current_club": r["current_club"],
                "interested_club": r["interested_club"],
                "probability": r["probability"],
                "source_link": r["source_link"],
            })

    # Sort: known probs first (desc), then name
    items.sort(key=lambda x: (x["probability"] is None, -(x["probability"] or 0), x["player"]))

    os.makedirs("out", exist_ok=True)

    # Debug artifacts if nothing parsed (helps tune selectors)
    if not rows or not items:
        with open("out/debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        with open("out/rows.json", "w", encoding="utf-8") as f:
            json.dump({
                "url": URL,
                "total_rows": len(rows),
                "parsed_rows": parsed_rows[:50],  # cap for size
                "defender_items_count": len(items),
                "column_index_map": col_idx,
            }, f, ensure_ascii=False, indent=2)

    # JSON output
    with open("out/defender_rumours.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "source": URL,
            "items": items
        }, f, ensure_ascii=False, indent=2)

    # HTML output (for email)
    with open("out/defender_rumours.html", "w", encoding="utf-8") as f:
        f.write(render_html(items))


if __name__ == "__main__":
    main()
