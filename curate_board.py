# curate_board.py
import os, base64, pathlib, time, requests
from typing import Optional, List, Dict
from dotenv import load_dotenv
from dateutil import parser as dtparse

API_BASE = "https://api.pinterest.com/v5"
UA = "fashion-poc/0.1 (+contact: you@example.com)"

def load_env():
    here = pathlib.Path(__file__).resolve().parent
    env_path = here / ".env"
    if env_path.exists(): load_dotenv(env_path)

def env(name, required=False, default=None):
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip()==""):
        raise RuntimeError(f"Missing env var: {name}")
    return v

def headers(tok): return {"Authorization": f"Bearer {tok}", "Accept":"application/json", "User-Agent": UA}

def refresh_access_token(app_id, app_secret, refresh_token):
    r = requests.post(f"{API_BASE}/oauth/token",
        auth=(app_id, app_secret),
        headers={"Content-Type":"application/x-www-form-urlencoded","User-Agent":UA},
        data={"grant_type":"refresh_token","refresh_token":refresh_token}, timeout=30)
    r.raise_for_status(); return r.json()["access_token"]

def list_boards(token) -> List[Dict]:
    out, bm = [], None
    while True:
        params = {"page_size":50}
        if bm: params["bookmark"]=bm
        r = requests.get(f"{API_BASE}/boards", headers=headers(token), params=params, timeout=30)
        r.raise_for_status()
        j = r.json(); items = j.get("items",[])
        out.extend(items); bm = j.get("bookmark")
        if not bm or not items: break
        time.sleep(0.2)
    return out

def resolve_board_id(token, board_name_or_id: str) -> Optional[str]:
    if board_name_or_id.isdigit(): return board_name_or_id
    for b in list_boards(token):
        if (b.get("name","").strip().lower() == board_name_or_id.strip().lower()):
            return str(b.get("id"))
    return None

def list_pins_on_board(token, board_id) -> List[Dict]:
    out, bm = [], None
    while True:
        params = {"page_size":100}
        if bm: params["bookmark"]=bm
        r = requests.get(f"{API_BASE}/boards/{board_id}/pins", headers=headers(token), params=params, timeout=30)
        r.raise_for_status()
        j = r.json(); items = j.get("items",[])
        out.extend(items); bm = j.get("bookmark")
        if not bm or not items: break
        time.sleep(0.2)
    return out

def search_user_pins(token, query: str) -> List[Dict]:
    # Searches YOUR saved pins only, not global Pinterest
    out, bm = [], None
    while True:
        params = {"query": query, "page_size":50}
        if bm: params["bookmark"]=bm
        r = requests.get(f"{API_BASE}/search/pins", headers=headers(token), params=params, timeout=30)
        r.raise_for_status()
        j = r.json(); items = j.get("items",[])
        out.extend(items); bm = j.get("bookmark")
        if not bm or not items: break
        time.sleep(0.2)
    return out

def extract_image_url(pin: Dict) -> Optional[str]:
    media = pin.get("media") or {}
    if media.get("media_type","").lower()=="video": return None
    images = media.get("images") or {}
    for k in ("original","orig","xlarge","large","1200x"):
        if k in images and "url" in images[k]: return images[k]["url"]
    if "image_url" in pin: return pin["image_url"]
    return None

def create_pin_from_url(token, board_id, img_url, title="", description="", link=""):
    payload = {"board_id": board_id, "title": title, "description": description,
               "link": link, "media_source": {"source_type":"image_url","url": img_url}}
    r = requests.post(f"{API_BASE}/pins", headers=headers(token), json=payload, timeout=60)
    r.raise_for_status(); return r.json()

if __name__=="__main__":
    load_env()
    APP_ID     = env("PINTEREST_APP_ID", required=True)
    APP_SECRET = env("PINTEREST_APP_SECRET", required=True)
    REFRESH    = env("PINTEREST_REFRESH_TOKEN", required=True)

    TARGET_BOARD_ID = env("PINTEREST_BOARD_ID", default="")
    TARGET_BOARD_NAME = env("PINTEREST_BOARD_NAME", default="")

    # Either keywords to search YOUR saved pins, or external image URLs to add
    QUERY   = env("PIN_QUERY", default="")          # e.g. "menswear, streetwear, outfit"
    URL_CSV = env("PIN_IMAGE_URLS", default="")     # comma-separated external URLs

    # 1) Mint fresh access token
    token = refresh_access_token(APP_ID, APP_SECRET, REFRESH)

    # 2) Resolve board id
    if not TARGET_BOARD_ID:
        if not TARGET_BOARD_NAME:
            raise RuntimeError("Set PINTEREST_BOARD_ID or PINTEREST_BOARD_NAME in .env")
        TARGET_BOARD_ID = resolve_board_id(token, TARGET_BOARD_NAME)
        if not TARGET_BOARD_ID:
            raise RuntimeError(f'Board "{TARGET_BOARD_NAME}" not found.')

    print(f"Using board_id={TARGET_BOARD_ID}")

    # 3) If you want to copy from your **saved pins** by keywords
    if QUERY.strip():
        terms = " ".join(t.strip() for t in QUERY.split(",") if t.strip())
        print("Searching your saved pins for:", terms)
        pins = search_user_pins(token, terms)
        added = 0
        for pin in pins:
            img = extract_image_url(pin)
            if not img: continue
            # Preserve original link if present (keeps attribution)
            link = pin.get("link") or pin.get("pin_url") or ""
            title = pin.get("title") or pin.get("grid_title") or ""
            description = pin.get("description") or pin.get("note") or ""
            try:
                resp = create_pin_from_url(token, TARGET_BOARD_ID, img, title=title, description=description, link=link)
                print("Added pin:", resp.get("id"))
                added += 1
                time.sleep(0.1)
            except requests.HTTPError as e:
                print("Skip:", e.response.text)
        print(f"Copied {added} pins from search to your board.")

    # 4) If you want to add external image URLs directly
    if URL_CSV.strip():
        for u in [s.strip() for s in URL_CSV.split(",") if s.strip()]:
            try:
                resp = create_pin_from_url(token, TARGET_BOARD_ID, u)
                print("Pinned external:", resp.get("id"), u)
                time.sleep(0.1)
            except requests.HTTPError as e:
                print("Skip:", e.response.text)
