# scripts/scrape_defenders.py
import os, re, json, datetime, time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.transfermarkt.com"
COMPETITION = os.getenv("COMPETITION", "L1")
SEASON_ID = os.getenv("SEASON_ID", "2025")
# Use DETAILED layout for more consistent DOM
URL = f"{BASE_URL}/bundesliga/geruechte/wettbewerb/{COMPETITION}/saison_id/{SEASON_ID}/plus/1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DefenderRumoursBot/1.1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9,de;q=0.7",
    "Referer": BASE_URL + "/",
    "Connection": "keep-alive",
}
TIMEOUT = 30

DEFENDER_KEYWORDS = {
    # English
    "defender", "centre-back", "center-back", "left-back", "right-back", "wing-back",
    # German common forms
    "innenverteidiger", "rechter verteidiger", "linker verteidiger",
    "außenverteidiger", "aussenverteidiger", "verteidiger",
}

def fetch_with_retries(url: str, max_retries: int = 3, backoff: float = 2.0) -> requests.Response:
    last_exc = None
    s = requests.Session()
    for i in range(1, max_retries + 1):
        try:
            resp = s.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if i < max_retries:
                time.sleep(backoff * i)
    raise last_exc

def text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""

def is_defender(position_text: str) -> bool:
    p = (position_text or "").lower()
    return any(k in p for k in DEFENDER_KEYWORDS)

def parse_probability(s: str):
    m = re.search(r"(\d{1,3})\s*%", s or "")
    if m:
        val = int(m.group(1))
        if 0 <= val <= 100:
            return val
    return None

def main():
    resp = fetch_with_retries(URL)
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    table = soup.select_one("table.items")
    rows = table.select("tbody > tr") if table else []
    parsed_rows = []
    items = []

    for tr in rows:
        tds = tr.find_all("td")
        # Rumours table should have enough cols; guard lightly
        if len(tds) < 5:
            continue

        player_cell = tds[0]
        player_link = player_cell.select_one("a[href*='/profil/spieler/']")
        player = text(player_link) if player_link else text(player_cell)

        # Try to get a position string from the player cell first
        player_cell_txt = text(player_cell)
        pos_txt = ""
        # Detailed view commonly places position on the second line of the inline-table or within small/span
        for s in player_cell.select("table.inline-table tr td, span, small, .position"):
            t = text(s)
            if t and any(k in t.lower() for k in ("back", "verteidiger", "defender")):
                pos_txt = t
                break
        # Fallback: scan the whole player cell text
        if not pos_txt and is_defender(player_cell_txt):
            pos_txt = re.search(
                r"(?:[A-Za-zÄÖÜäöüß\- ]*(?:back|verteidiger|defender)[A-Za-zÄÖÜäöüß\- ]*)",
                player_cell_txt, flags=re.IGNORECASE
            )
            pos_txt = pos_txt.group(0) if pos_txt else ""

        # If still nothing, try entire row text (some layouts place pos elsewhere)
        row_txt = " ".join(text(td) for td in tds)
        if not pos_txt and is_defender(row_txt):
            pos_txt = re.search(
                r"(?:[A-Za-zÄÖÜäöüß\- ]*(?:back|verteidiger|defender)[A-Za-zÄÖÜäöüß\- ]*)",
                row_txt, flags=re.IGNORECASE
            )
            pos_txt = pos_txt.group(0) if pos_txt else ""

        current_club = text(tds[2]) if len(tds) > 2 else ""
        interested = text(tds[3]) if len(tds) > 3 else ""

        source_cell = tds[4] if len(tds) > 4 else None
        source_link_el = source_cell.select_one("a") if source_cell else None
        raw_href = source_link_el["href"] if (source_link_el and source_link_el.has_attr("href")) else ""
        source_link = urljoin(URL, raw_href) if raw_href else ""

        prob_txt = text(tds[5]) if len(tds) > 5 else ""
        prob = parse_probability(prob_txt)

        parsed_rows.append({
            "player": player,
            "player_cell_text": player_cell_txt,
            "row_text": row_txt,
            "position_raw": pos_txt,
            "current_club": current_club,
            "interested_club": interested,
            "probability_raw": prob_txt,
            "source_link": source_link
        })

    # Filter to defenders now that we've parsed every row
    for r in parsed_rows:
        if is_defender(r.get("position_raw") or r.get("player_cell_text") or r.get("row_text")):
            items.append({
                "player": r["player"],
                "position": r["position_raw"] or "Defender",
                "current_club": r["current_club"],
                "interested_club": r["interested_club"],
                "probability": parse_probability(r["probability_raw"]),
                "source_link": r["source_link"],
            })

    # Sort: known probs first (desc), then name
    items.sort(key=lambda x: (x["probability"] is None, -(x["probability"] or 0), x["player"]))

    os.makedirs("out", exist_ok=True)

    # If nothing parsed, drop debug to inspect what HTML we actually got on the runner
    if not rows or not items:
        with open("out/debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        with open("out/rows.json", "w", encoding="utf-8") as f:
            json.dump({
                "url": URL,
                "total_rows": len(rows),
                "parsed_rows": parsed_rows[:50],  # cap for size
                "defender_items_count": len(items),
            }, f, ensure_ascii=False, indent=2)

    with open("out/defender_rumours.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "source": URL,
            "items": items
        }, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
