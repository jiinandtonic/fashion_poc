#!/usr/bin/env python3
# ingest_pinterest_v5.py
import os, sys, csv, time, argparse, pathlib, re
from typing import Dict, List, Optional, Tuple

import requests
from dateutil import parser as dtparse
from dotenv import load_dotenv
from tqdm import tqdm

API_BASE = "https://api.pinterest.com/v5"
UA = "fashion-poc/0.1 (+PoC; contact: you@example.com)"

# ---------------- Env helpers ----------------
def env(name: str, required: bool = False, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

def load_env():
    # Loads .env from repo root or script folder
    here = pathlib.Path(__file__).resolve().parent
    for candidate in [here / ".env", here.parent / ".env"]:
        if candidate.exists():
            load_dotenv(candidate, override=False)
            break

# ---------------- Auth ----------------
def headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": UA}

def refresh_access_token(app_id: str, app_secret: str, refresh_token: str) -> str:
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    r = requests.post(
        f"{API_BASE}/oauth/token",
        auth=(app_id, app_secret),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=data,
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Refresh failed ({r.status_code}): {r.text}")
    j = r.json()
    tok = j.get("access_token")
    if not tok:
        raise RuntimeError(f"No access_token in refresh response: {j}")
    return tok

# ---------------- Boards ----------------
def list_boards(token: str, privacy: Optional[str] = None, page_size: int = 50) -> List[Dict]:
    url = f"{API_BASE}/boards"
    boards, bookmark = [], None
    while True:
        params = {"page_size": page_size}
        if bookmark: params["bookmark"] = bookmark
        if privacy:  params["privacy"] = privacy  # PUBLIC | PROTECTED | SECRET
        r = requests.get(url, headers=headers(token), params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        items = j.get("items", [])
        boards.extend(items)
        bookmark = j.get("bookmark")
        if not bookmark or not items:
            break
        time.sleep(0.2)
    return boards

def score_relevance(board: Dict) -> float:
    """Simple menswear heuristics on name/description."""
    txt = f"{board.get('name','')} {board.get('description','')}".lower()
    pats = [
        r"\bmen\b", r"\bmens\b", r"\bmenswear\b", r"\bstreetwear\b",
        r"\boutfit\b", r"\boutfits\b", r"\bstyle\b", r"\bfashion\b",
        r"\bwardrobe\b", r"\bformal\b", r"\bcasual\b"
    ]
    s = sum(1.0 for p in pats if re.search(p, txt))
    # small boost for public/protected (often more curated)
    if (board.get("privacy") or "").upper() != "SECRET":
        s += 0.2
    return s

def pick_relevant_boards(boards: List[Dict], top_k: int = 3) -> List[Dict]:
    scored = sorted(boards, key=score_relevance, reverse=True)
    return [b for b in scored[:top_k] if score_relevance(b) > 0]

def find_boards_by_names_or_ids(boards: List[Dict], names_or_ids: List[str]) -> List[Dict]:
    wanted_ids = set(s.strip() for s in names_or_ids)
    wanted_names = set(s.strip().lower() for s in names_or_ids)
    sel = []
    for b in boards:
        bid = str(b.get("id") or "")
        name = (b.get("name") or "").strip().lower()
        if bid in wanted_ids or name in wanted_names:
            sel.append(b)
    return sel

# ---------------- Pins ----------------
def list_pins_on_board(token: str, board_id: str, page_size: int = 100) -> List[Dict]:
    url = f"{API_BASE}/boards/{board_id}/pins"
    pins, bookmark = [], None
    while True:
        params = {"page_size": page_size}
        if bookmark: params["bookmark"] = bookmark
        r = requests.get(url, headers=headers(token), params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        items = j.get("items", [])
        pins.extend(items)
        bookmark = j.get("bookmark")
        if not bookmark or not items:
            break
        time.sleep(0.2)
    return pins

def extract_image_url(pin: Dict) -> Optional[str]:
    media = pin.get("media") or {}
    if media.get("media_type", "").lower() == "video":
        return None
    images = media.get("images") or {}
    for key in ("original", "orig", "xlarge", "large", "1200x"):
        if key in images and "url" in images[key]:
            return images[key]["url"]
    if "image_url" in pin:
        return pin["image_url"]
    return None

# ---------------- I/O ----------------
def sanitize_filename(s: str) -> str:
    return "".join(c for c in s if c.isalnum() or c in ("-", "_", ".")).strip()

def download_image(url: str, out_dir: pathlib.Path, pin_id: str) -> Optional[pathlib.Path]:
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        fp = out_dir / f"{sanitize_filename(pin_id) or str(int(time.time()*1000))}.jpg"
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        resp.raise_for_status()
        with open(fp, "wb") as f: f.write(resp.content)
        return fp
    except Exception as e:
        print(f"[download-skip] {pin_id}: {e}", file=sys.stderr)
        return None

def search_user_pins(token: str, query: str, page_size: int = 50):
    """Search the token user's saved pins for keywords (NOT global Pinterest)."""
    url = f"{API_BASE}/search/pins"
    pins, bookmark = [], None
    while True:
        params = {"query": query, "page_size": page_size}
        if bookmark:
            params["bookmark"] = bookmark
        r = requests.get(url, headers=headers(token), params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        items = j.get("items", [])
        pins.extend(items)
        bookmark = j.get("bookmark")
        if not bookmark or not items:
            break
        time.sleep(0.2)
    return pins

def write_meta(rows: List[Dict], meta_csv: pathlib.Path):
    meta_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not meta_csv.exists()
    with open(meta_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "platform","local_path","pin_id","board_id","board_name",
                "url","link","title","description","created_at"
            ],
        )
        if write_header: w.writeheader()
        w.writerows(rows)

# ---------------- Main ----------------
def main():
    load_env()

    ap = argparse.ArgumentParser(description="Pinterest v5 ingestor (images) using refresh-token auth + relevant board selection")
    ap.add_argument("--limit-per-board", type=int, default=int(env("PINTEREST_LIMIT_PER_BOARD", default="300")),
                    help="Max pins per board to download")
    ap.add_argument("--out-images", default=env("PINTEREST_OUT_IMAGES", default="data/pinterest_images"),
                    help="Folder for images")
    ap.add_argument("--out-meta", default=env("PINTEREST_OUT_META", default="data/pinterest_meta.csv"),
                    help="CSV for metadata")
    ap.add_argument("--privacy", default=env("PINTEREST_BOARD_PRIVACY", default=None),
                    choices=[None, "PUBLIC", "PROTECTED", "SECRET"], help="Filter boards by privacy")
    ap.add_argument("--boards", nargs="+", default=None,
                    help="Board names or IDs (overrides PINTEREST_BOARDS env).")
    ap.add_argument("--pick-relevant", action="store_true", default=env("PINTEREST_PICK_RELEVANT", default="true").lower() == "true",
                    help="Auto-pick top menswear-relevant boards if none specified.")
    ap.add_argument("--query", default=None, help='Comma-separated search terms scoped to YOUR saved pins (e.g. "menswear, streetwear").')
    args = ap.parse_args()

    # Required OAuth creds from .env
    APP_ID     = env("PINTEREST_APP_ID", required=True)
    APP_SECRET = env("PINTEREST_APP_SECRET", required=True)
    REFRESH    = env("PINTEREST_REFRESH_TOKEN", required=True)

    # 1) Mint a fresh access token
    token = refresh_access_token(APP_ID, APP_SECRET, REFRESH)

    # 2) Boards
    boards = list_boards(token, privacy=args.privacy)
    if not boards:
        sys.exit("No boards available for this account/token.")

    # Resolve board selection
    selected = []
    env_boards = env("PINTEREST_BOARDS", default=None)
    if args.boards:
        selected = find_boards_by_names_or_ids(boards, args.boards)
    elif env_boards:
        selected = find_boards_by_names_or_ids(boards, [s.strip() for s in env_boards.split(",") if s.strip()])

    if not selected and args.pick_relevant:
        selected = pick_relevant_boards(boards, top_k=3)

    # If still none, fall back to all (warn: may be noisy)
    if not selected:
        print("No boards specified or auto-selected; ingesting from ALL boards.")
        selected = boards

    print("Selected boards:")
    for b in selected:
        print(f" - {b.get('id')} :: {b.get('name')} :: privacy={b.get('privacy')}")

    out_dir = pathlib.Path(args.out_images)
    meta_csv = pathlib.Path(args.out_meta)
    all_rows: List[Dict] = []

    if args.query:
        q = " ".join(s.strip() for s in args.query.split(",") if s.strip())
        print(f"Searching your saved pins for: {q}")
        pins = search_user_pins(token, q)
        for pin in tqdm(pins, desc="Search results", unit="pin"):
            img_url = extract_image_url(pin)
            if not img_url:
                continue
            pin_id = str(pin.get("id") or "")
            fp = download_image(img_url, pathlib.Path(args.out_images), pin_id)
            if not fp:
                continue
            link = pin.get("link") or pin.get("pin_url") or ""
            title = pin.get("title") or pin.get("grid_title") or ""
            desc  = pin.get("description") or pin.get("note") or ""
            created = pin.get("created_at") or pin.get("created") or ""
            try:
                created = dtparse.parse(created).isoformat()
            except Exception:
                pass
            all_rows.append({
                "platform":"pinterest","local_path":str(fp),"pin_id":pin_id,
                "board_id":"", "board_name":"(search)", "url":img_url,
                "link":link, "title":title, "description":desc, "created_at":created
            })

    # 3) Ingest pins → download images → write CSV
    for b in selected:
        bid = str(b.get("id"))
        bname = b.get("name") or ""
        pins = list_pins_on_board(token, bid)
        if not pins:
            continue

        count = 0
        for pin in tqdm(pins, desc=f"Board {bname}", unit="pin"):
            if count >= args.limit_per_board:
                break
            img_url = extract_image_url(pin)
            if not img_url:
                continue

            pin_id = str(pin.get("id") or "")
            fp = download_image(img_url, out_dir, pin_id)
            if not fp:
                continue

            link = pin.get("link") or pin.get("pin_url") or ""
            title = pin.get("title") or pin.get("grid_title") or ""
            desc  = pin.get("description") or pin.get("note") or ""
            created = pin.get("created_at") or pin.get("created") or ""
            try:
                created = dtparse.parse(created).isoformat()
            except Exception:
                pass

            all_rows.append({
                "platform": "pinterest",
                "local_path": str(fp),
                "pin_id": pin_id,
                "board_id": bid,
                "board_name": bname,
                "url": img_url,
                "link": link,
                "title": title,
                "description": desc,
                "created_at": created,
            })
            count += 1
            time.sleep(0.05)  # gentle throttle

    if all_rows:
        write_meta(all_rows, meta_csv)
        print(f"Saved {len(all_rows)} images → {out_dir}")
        print(f"Appended metadata → {meta_csv}")
    else:
        print("No images saved; check board selection / scopes.")

if __name__ == "__main__":
    main()
