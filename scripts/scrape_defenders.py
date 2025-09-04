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

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def text(el) -> str:
    return norm(el.get_text(" ", strip=True)) if el else ""

def is_defender(position_text: str) -> bool:
    p = (position_text or "").lower()
    return any(k in p for k in DEFENDER_KEYWORDS)

def fetch_with_retries(url: str, max_retries: int = 3, backoff: float = 1.5) -> requests.Response:
    s = requests.Session()
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
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown error performing GET")

def extract_player_details(profile_url: str) -> dict:
    try:
        resp = fetch_with_retries(profile_url)
        soup = BeautifulSoup(resp.text, "lxml")

        info_box = soup.select_one("div.data-header__details")
        age = ""
        nationality = ""
        contract_expiry = ""
        market_value = ""
        transfer_type = ""
        rumour_date = ""

        if info_box:
            age_match = re.search(r'Age:\s*(\d+)', info_box.text)
            if age_match:
                age = age_match.group(1)

        nation_el = soup.select_one("span.data-header__label:contains('Citizenship') + span")
        if nation_el:
            nationality = text(nation_el)

        contract_el = soup.find("span", string=re.compile("Contract expires"))
        if contract_el and contract_el.find_next("span"):
            contract_expiry = text(contract_el.find_next("span"))

        mv_el = soup.select_one("div.data-header__market-value-wrapper")
        if mv_el:
            market_value = text(mv_el)

        # Rumour date and transfer type may be in the rumours table
        rumour_table = soup.select_one("div.box")
        if rumour_table:
            table_text = rumour_table.get_text(" ", strip=True)
            date_match = re.search(r"Rumour date:\s*(\d{2}/\d{2}/\d{4})", table_text)
            if date_match:
                rumour_date = date_match.group(1)
            type_match = re.search(r"Transfer type:\s*(\w+)", table_text)
            if type_match:
                transfer_type = type_match.group(1)

        return {
            "age": age,
            "nationality": nationality,
            "contract_expiry": contract_expiry,
            "market_value": market_value,
            "transfer_type": transfer_type,
            "rumour_date": rumour_date
        }
    except Exception:
        return {
            "age": "", "nationality": "", "contract_expiry": "",
            "market_value": "", "transfer_type": "", "rumour_date": ""
        }

def main():
    resp = fetch_with_retries(URL)
    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.select_one("table.items")
    rows = table.select("tbody > tr") if table else []
    items = []

    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if not tds or len(tds) < 5:
            continue

        player_cell = tds[0]
        player_link = player_cell.select_one("a[href*='/profil/spieler/']")
        player_name = text(player_link)
        profile_link = urljoin(BASE_URL, player_link['href']) if player_link else ""

        position = ""
        for el in player_cell.select("table.inline-table td, small, span"):
            t = text(el)
            if is_defender(t):
                position = t
                break

        if not is_defender(position):
            continue

        current_club = text(tds[2])
        interested_club = text(tds[3])

        details = extract_player_details(profile_link)

        items.append({
            "player": player_name,
            "position": position or "Defender",
            "age": details["age"],
            "nationality": details["nationality"],
            "contract_expiry": details["contract_expiry"],
            "market_value": details["market_value"],
            "transfer_type": details["transfer_type"],
            "rumour_date": details["rumour_date"],
            "profile_link": profile_link,
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

main()
