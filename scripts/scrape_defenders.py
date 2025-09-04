import os
import re
import json
import datetime as dt
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.transfermarkt.com"
COMPETITION = os.getenv("COMPETITION", "L1")
SEASON_ID = os.getenv("SEASON_ID", "2025")
URL = f"{BASE_URL}/bundesliga/geruechte/wettbewerb/{COMPETITION}/saison_id/{SEASON_ID}/plus/1"

# Toggle profile lookups (age, nationality, contract expiry, market value)
# Use 0 to skip profiles (faster, fewer requests)
FETCH_PROFILES = os.getenv("FETCH_PROFILES", "1") == "1"
PROFILE_SLEEP_S = float(os.getenv("PROFILE_SLEEP_S", "1.2"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
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
    # German and other common languages on TM
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

def fetch_with_retries(url: str, max_retries: int = 3, backoff: float = 1.6) -> requests.Response:
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

def parse_prob_from_style(style: str) -> Optional[int]:
    # e.g. 'width:80%' or 'width: 40 %'
    if not style:
        return None
    m = re.search(r"width\s*:\s*(\d{1,3})\s*%", style)
    if not m:
        return None
    try:
        val = int(m.group(1))
        return max(0, min(100, val))
    except:
        return None

def extract_player_details(profile_url: str) -> dict:
    """Best-effort extraction from player profile page with graceful fallbacks."""
    try:
        resp = fetch_with_retries(profile_url)
        soup = BeautifulSoup(resp.text, "lxml")

        # AGE: derive from birthdate (data-zeit is unix ts of DOB)
        age = ""
        dob_span = soup.select_one("span[data-zeit]")
        if dob_span and dob_span.get("data-zeit", "").isdigit():
            try:
                birth_ts = int(dob_span["data-zeit"])
                now_ts = int(dt.datetime.utcnow().timestamp())
                years = int((now_ts - birth_ts) // (365.2425 * 24 * 3600))
                if 14 <= years <= 50:  # sanity bounds
                    age = str(years)
            except:
                pass

        # NATIONALITY
        nationality = ""
        nat_imgs = soup.select("img.flaggenrahmen[title]")
        if nat_imgs:
            nationality = ", ".join(img.get("title", "") for img in nat_imgs if img.get("title"))

        # CONTRACT EXPIRY (EN locale)
        contract_expiry = ""
        # common place: label + sibling
        lab = soup.find(lambda tag: tag.name in ("span", "div") and "Contract expires" in text(tag))
        if lab:
            nx = lab.find_next(["span", "div"])
            if nx:
                contract_expiry = text(nx)
        if not contract_expiry:
            # alternative locations
            for sel in [
                "div.data-header__info-box span:contains('Contract expires')",
                "li:contains('Contract expires')",
            ]:
                el = soup.select_one(sel)
                if el:
                    contract_expiry = text(el)
                    break

        # MARKET VALUE (first currency value in header)
        market_value = ""
        mv_wrap = soup.select_one("div.data-header__market-value-wrapper")
        if mv_wrap:
            # Typically like: "€20.00m Last update: ..."
            val = re.search(r"([€£$]\s?[0-9\.,]+[mMkK]?)", text(mv_wrap))
            if val:
                market_value = val.group(1)

        return {
            "age": age,
            "nationality": nationality,
            "contract_expiry": contract_expiry,
            "market_value": market_value,
        }
    except Exception:
        return {
            "age": "",
            "nationality": "",
            "contract_expiry": "",
            "market_value": "",
        }

def extract_rumour_row(tr) -> Optional[dict]:
    """Parse a single rumours table row."""
    tds = tr.find_all("td", recursive=False)
    if not tds:
        return None

    # Player cell
    player_cell = tds[0]
    player_link = player_cell.select_one("a[href*='/profil/spieler/']")
    player_name = text(player_link)
    profile_href = player_link.get("href") if player_link else ""
    profile_url = urljoin(BASE_URL, profile_href) if profile_href else ""

    # Position (within inline-table / small tags)
    position = ""
    for el in player_cell.select("table.inline-table td, small, span"):
        t = text(el)
        if t and is_defender(t):
            position = t
            break

    # Defender filter
    if not is_defender(position):
        return None

    # Current club + logo
    current_club = ""
    current_club_logo = ""
    # typically first club link/img in row
    curr_club_a = tr.select_one("a.vereinprofil_tooltip")  # first occurrence near left
    if curr_club_a:
        current_club = text(curr_club_a)
        logo = curr_club_a.select_one("img.tiny_wappen")
        if logo and logo.get("src"):
            current_club_logo = urljoin(BASE_URL, logo["src"])

    # Interested club + logo (often a later a.vereinprofil_tooltip in the row)
    interested_club = ""
    interested_club_logo = ""
    club_links = tr.select("a.vereinprofil_tooltip")
    if len(club_links) >= 2:
        interested_club = text(club_links[-1])
        logo = club_links[-1].select_one("img.tiny_wappen")
        if logo and logo.get("src"):
            interested_club_logo = urljoin(BASE_URL, logo["src"])

    # Rumour date (often in the rightmost cell as <abbr title="2025-09-04">04/09/25</abbr>)
    rumour_date = ""
    date_abbr = tr.select_one("td:last-child abbr[title], td:last-child span[title]")
    if date_abbr and date_abbr.get("title"):
        rumour_date = date_abbr["title"]
    else:
        # fallback to visible last cell text
        rumour_date = text(tr.select_one("td:last-child"))

    # Probability (best effort, based on style width%)
    probability = None
    prob_bar = tr.select_one("[style*='width'][class*='bar'], [style*='width'][class*='wahrscheinlichkeit']")
    if prob_bar:
        probability = parse_prob_from_style(prob_bar.get("style", ""))

    # Source (best effort)
    source_name, source_link = "", ""
    source_a = tr.find("a", href=lambda h: h and "/news/" in h or "/geruechte/" in h)
    if source_a:
        source_name = text(source_a)
        source_link = urljoin(BASE_URL, source_a.get("href", ""))

    # Transfer type heuristic: read any tooltip titles/images suggesting Loan/Free/Return
    transfer_type = ""
    tips = tr.select("[title]")
    tip_text = " ".join((tip.get("title") or "") for tip in tips).lower()
    if "loan" in tip_text:
        transfer_type = "Loan"
    elif "return from loan" in tip_text or "end of loan" in tip_text:
        transfer_type = "Return/End of loan"
    elif "free transfer" in tip_text or "without fee" in tip_text:
        transfer_type = "Free"
    elif "transfer" in tip_text or "wechsel" in tip_text:
        transfer_type = "Transfer"
    # otherwise leave empty; may be refined by detail page if needed

    return {
        "player": player_name,
        "position": position or "Defender",
        "profile_link": profile_url,
        "current_club": current_club,
        "current_club_logo": current_club_logo,
        "interested_club": interested_club,
        "interested_club_logo": interested_club_logo,
        "rumour_date": rumour_date,
        "probability": probability,
        "source_name": source_name,
        "source_link": source_link,
        "transfer_type": transfer_type,
    }

def build_email_html(items: List[dict], source_url: str) -> str:
    """Inline-styled HTML email with alternating black/grey rows and club logos."""
    css = """
      body { font-family: Segoe UI, Arial, sans-serif; background:#0b0b0b; color:#eaeaea; }
      .wrap { max-width: 960px; margin: 0 auto; }
      h2 { color:#ffffff; }
      table { border-collapse: collapse; width: 100%; }
      th, td { padding: 8px 10px; vertical-align: middle; }
      th { background:#000; color:#fff; text-align:left; position: sticky; top: 0; }
      tr:nth-child(odd)  { background:#111; color:#f0f0f0; }
      tr:nth-child(even) { background:#2b2b2b; color:#f0f0f0; }
      a { color:#9bd5ff; text-decoration: none; }
      .club { display:flex; align-items:center; gap:8px; }
      .club img { height:18px; width:18px; object-fit:contain; border-radius: 2px; background:#fff; }
      .tag { background:#444; padding:2px 6px; border-radius:10px; font-size:12px; color:#ddd; }
      .meta { font-size:12px; color:#bbb; }
    """.strip()

    header = f"""
    <div class="wrap">
      <h2>Bundesliga Defender Rumours – {dt.datetime.utcnow().strftime('%Y-%m-%d')} (UTC)</h2>
      <div class="meta">Source: <a href="{source_url}">{source_url}</a></div>
      <table role="table" aria-label="Bundesliga Defender Rumours">
        <thead>
          <tr>
            <th>Player</th>
            <th>Position</th>
            <th>Current Club</th>
            <th>Interested Club</th>
            <th>Type</th>
            <th>Probability</th>
            <th>Age</th>
            <th>Nationality</th>
            <th>Contract</th>
            <th>Market Value</th>
            <th>Rumour Date</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
    """.strip()

    rows = []
    for it in items:
        prob = f"{it.get('probability')}%" if it.get('probability') is not None else ""
        src_html = (f'<a href="{it["source_link"]}">{it["source_name"] or "Link"}</a>'
                    if it.get("source_link") else (it.get("source_name") or ""))
        rows.append(f"""
          <tr>
            <td><a href="{it.get('profile_link','')}"><strong>{it.get('player','')}</strong></a></td>
            <td>{it.get('position','')}</td>
            <td>
              <div class="club">
                {'<img src="'+it['current_club_logo']+'" alt="">' if it.get('current_club_logo') else ''}
                <span>{it.get('current_club','')}</span>
              </div>
            </td>
            <td>
              <div class="club">
                {'<img src="'+it['interested_club_logo']+'" alt="">' if it.get('interested_club_logo') else ''}
                <span>{it.get('interested_club','')}</span>
              </div>
            </td>
            <td><span class="tag">{it.get('transfer_type','')}</span></td>
            <td>{prob}</td>
            <td>{it.get('age','')}</td>
            <td>{it.get('nationality','')}</td>
            <td>{it.get('contract_expiry','')}</td>
            <td>{it.get('market_value','')}</td>
            <td>{it.get('rumour_date','')}</td>
            <td>{src_html}</td>
          </tr>
        """.strip())

    footer = """
        </tbody>
      </table>
    </div>
    """.strip()

    return f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>{header}\n" + "\n".join(rows) + f"\n{footer}</body></html>"

def main():
    resp = fetch_with_retries(URL)
    soup = BeautifulSoup(resp.text, "lxml")

    table = soup.select_one("table.items")
    rows = table.select("tbody > tr") if table else []

    items: List[dict] = []
    for tr in rows:
        item = extract_rumour_row(tr)
        if not item:
            continue

        if FETCH_PROFILES and item.get("profile_link"):
            details = extract_player_details(item["profile_link"])
            item.update(details)
            time.sleep(PROFILE_SLEEP_S)  # politeness
        else:
            # Defaults when skipping profiles
            item.update({
                "age": "",
                "nationality": "",
                "contract_expiry": "",
                "market_value": "",
            })

        items.append(item)

    # Output
    os.makedirs("out", exist_ok=True)
    payload = {
        "generated_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "source": URL,
        "competition": COMPETITION,
        "season": SEASON_ID,
        "count": len(items),
        "items": items,
    }
    with open("out/defender_rumours.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    html = build_email_html(items, URL)
    with open("out/defender_rumours.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote out/defender_rumours.json ({len(items)} items)")
    print(f"Wrote out/defender_rumours.html")

if __name__ == "__main__":
    main()
