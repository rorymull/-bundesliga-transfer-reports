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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DefenderRumoursBot/1.3",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9,de;q=0.7",
    "Referer": BASE_URL + "/",
    "Connection": "keep-alive",
}
TIMEOUT = 30

DEFENDER_KEYWORDS = {
    "defender", "centre-back", "center-back", "centre back", "center back",
    "left-back", "left back", "right-back", "right back",
    "wing-back", "wingback", "full-back", "fullback",
    "innenverteidiger", "rechter verteidiger", "linker verteidiger",
    "außenverteidiger", "aussenverteidiger", "verteidiger",
}

def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def text(el) -> str:
    return norm(el.get_text(" ", strip=True)) if el else ""

def is_defender(position_text: str) -> bool:
    p = (position_text or "").lower()
    return any(k in p for k in DEFENDER_KEYWORDS)

def extract_position_from_cell(cell) -> str:
    for sel in ["table.inline-table td", "small", "span", ".position"]:
        for el in cell.select(sel):
            t = text(el)
            if t and any(k in t.lower() for k in DEFENDER_KEYWORDS):
                return t
    return ""

def extract_additional_info(player_cell) -> Dict[str, str]:
    info = {
        "age": "",
        "nationality": "",
        "contract_expiry": "",
        "market_value": "",
        "transfer_type": "",
        "rumour_date": "",
        "profile_link": ""
    }
    # Age
    age_el = player_cell.select_one("td.zentriert.alter")
    if age_el:
        info["age"] = text(age_el)
    # Nationality
    nat_imgs = player_cell.select("img.flaggenrahmen")
    nationalities = [img.get("title", "") for img in nat_imgs if img.get("title")]
    info["nationality"] = ", ".join(nationalities)
    # Contract expiry and market value
    smalls = player_cell.select("small")
    for sm in smalls:
        t = text(sm)
        if "Contract expires" in t:
            info["contract_expiry"] = t.replace("Contract expires:", "").strip()
        elif "Market value:" in t:
            info["market_value"] = t.replace("Market value:", "").strip()
    # Transfer type
    transfer_type_el = player_cell.find("span", class_="transfer-type")
    if transfer_type_el:
        info["transfer_type"] = text(transfer_type_el)
    # Rumour date
    date_el = player_cell.find("span", class_="datum")
    if date_el:
        info["rumour_date"] = text(date_el)
    # Profile link
    link_el = player_cell.select_one("a[href*='/profil/spieler/']")
    if link_el:
        info["profile_link"] = urljoin(BASE_URL, link_el.get("href", ""))
    return info

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
        link_html = f'<a href="{esc(it["profile_link"])}">Profile</a>' if it["profile_link"] else ""
        rows.append(
            "<tr>"
            f"<td>{esc(it['player'])}</td>"
            f"<td>{esc(it['position'])}</td>"
            f"<td>{esc(it['age'])}</td>"
            f"<td>{esc(it['nationality'])}</td>"
            f"<td>{esc(it['contract_expiry'])}</td>"
            f"<td>{esc(it['market_value'])}</td>"
            f"<td>{esc(it['transfer_type'])}</td>"
            f"<td>{esc(it['rumour_date'])}</td>"
            f"<td>{esc(it['current_club'])}</td>"
            f"<td>{esc(it['interested_club'])}</td>"
            f"<td>{link_html}</td>"
            "</tr>"
        )
    table = (
        "<table border='1' cellspacing='0' cellpadding='6' "
        "style='border-collapse:collapse;font-family:Segoe UI,Arial,Helvetica,sans-serif;font-size:14px;'>"
        "<thead style='background:#f3f4f6'>"
        "<tr><th>Player</th><th>Position</th><th>Age</th><th>Nationality</th><th>Contract Expiry</th>"
        "<th>Market Value</th><th>Transfer Type</th><th>Rumour Date</th><th>Current Club</th>"
        "<th>Interested Club</th><th>Profile</th></tr>"
        "</thead>"
        "<tbody>" + "\n".join(rows) + "</tbody></table>"
    )
    title = "Bundesliga Defender Rumours"
    return f"<html><body><h3 style='font-family:Segoe UI,Arial,Helvetica,sans-serif'>{title}</h3>{table}</body></html>"

def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.items")
    rows = table.select("tbody > tr") if table else []
    items: List[dict] = []

    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if not tds:
            continue
        player_cell = tds[0]
        player_link = player_cell.select_one("a[href*='/profil/spieler/']")
        player = text(player_link) if player_link else text(player_cell)
        position = extract_position_from_cell(player_cell)
        if not is_defender(position):
            continue
        current_club = text(tds[2]) if len(tds) > 2 else ""
        interested_club = text(tds[3]) if len(tds) > 3 else ""
        info = extract_additional_info(player_cell)
        items.append({
            "player": player,
            "position": position or "Defender",
            "age": info["age"],
            "nationality": info["nationality"],
            "contract_expiry": info["contract_expiry"],
            "market_value": info["market_value"],
            "transfer_type": info["transfer_type"],
            "rumour_date": info["rumour_date"],
            "profile_link": info["profile_link"],
            "current_club": current_club,
            "interested_club": interested_club
        })

    os.makedirs("out", exist_ok=True)
    with open("out/defender_rumours.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "source": URL,
            "items": items
        }, f, ensure_ascii=False, indent=2)

    with open("out/defender_rumours.html", "w", encoding="utf-8") as f:
        f.write(render_html(items))

main()
