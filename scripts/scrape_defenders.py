# scripts/scrape_defenders.py
# Produces out/defender_rumours.json from Transfermarkt Bundesliga Rumours (2025), defenders only.
import re, json, os, datetime, requests
from bs4 import BeautifulSoup

URL = "https://www.transfermarkt.com/bundesliga/geruechte/wettbewerb/L1/saison_id/2025"
DEF_PAT = re.compile(r"(centre[- ]?back|center[- ]?back|left[- ]?back|right[- ]?back|wing[- ]?back|defender|"
                     r"innenverteidiger|linker verteidiger|rechter verteidiger|außenverteidiger|verteidiger)",
                     re.IGNORECASE)

def text(el):
    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""

resp = requests.get(URL, headers={"User-Agent":"Mozilla/5.0"})
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "lxml")

items = []
table = soup.select_one("table.items")
if table:
    for tr in table.select("tbody > tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        player_cell = tds[0]
        link = player_cell.select_one("a[href*='/profil/spieler/']")
        player = text(link) if link else text(player_cell)

        # Find a position string near the player name
        pos_txt = ""
        for s in player_cell.select("table.inline-table tr td:nth-child(2), span, small"):
            t = text(s)
            if any(k in t.lower() for k in ["back", "verteidiger"]):
                pos_txt = t; break

        # Defender-only
        if not DEF_PAT.search(pos_txt or ""):
            continue

        current_club = text(tds[2]) if len(tds) > 2 else ""
        interested = text(tds[3]) if len(tds) > 3 else ""
        source_cell = tds[4] if len(tds) > 4 else None
        source_link_el = source_cell.select_one("a") if source_cell else None
        source_link = source_link_el["href"] if (source_link_el and source_link_el.has_attr("href")) else ""
        prob_txt = text(tds[5]) if len(tds) > 5 else ""
        m = re.search(r"(\d+)\s*%", prob_txt)
        prob = int(m.group(1)) if m else None

        items.append({
            "player": player,
            "position": pos_txt or "Defender",
            "current_club": current_club,
            "interested_club": interested or "",
            "probability": prob,
            "source_link": source_link
        })

# Sort by probability desc, then name
items.sort(key=lambda x: (x["probability"] if x["probability"] is not None else -1, x["player"]), reverse=True)

os.makedirs("out", exist_ok=True)
with open("out/defender_rumours.json", "w", encoding="utf-8") as f:
    json.dump({"generated_utc": datetime.datetime.utcnow().isoformat()+"Z", "items": items},
              f, ensure_ascii=False, indent=2)