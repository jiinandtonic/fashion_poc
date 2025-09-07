import os
import sys
import csv
import time
import argparse
import pathlib
from typing import Dict, List, Optional

import requests
from dateutil import parser as dtparse
from tqdm import tqdm

API_BASE = "https://api.pinterest.com/v5"
UA = "fashion-poc/0.1 (+PoC; contact: your_email@example.com)"  # keep descriptive

def _hdr(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": UA}

def list_boards(token: str, privacy: Optional[str] = None, page_size: int = 50) -> List[Dict]:
    """
    GET /v5/boards — returns boards you own or collaborate on. Supports pagination via bookmark.
    Docs: developers.pinterest.com (List boards)
    """
    url = f"{API_BASE}/boards"
    boards = []
    bookmark = None
    while True:
        params = {"page_size": page_size}
        if bookmark:
            params["bookmark"] = bookmark
        if privacy:
            params["privacy"] = privacy  # PUBLIC | PROTECTED | SECRET
        r = requests.get(url, headers=_hdr(token), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        boards.extend(items)
        bookmark = data.get("bookmark")
        if not bookmark or not items:
            break
        time.sleep(0.2)  # polite pacing
    return boards

def list_pins_on_board(token: str, board_id: str, page_size: int = 100) -> List[Dict]:
    """
    GET /v5/boards/{board_id}/pins — paginate with 'bookmark'.
    Docs: developers.pinterest.com (List Pins on board)
    """
    url = f"{API_BASE}/boards/{board_id}/pins"
    pins = []
    bookmark = None
    while True:
        params = {"page_size": page_size}
        if bookmark:
            params["bookmark"] = bookmark
        r = requests.get(url, headers=_hdr(token), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        pins.extend(items)
        bookmark = data.get("bookmark")
        if not bookmark or not items:
            break
        time.sleep(0.2)
    return pins

def extract_image_url(pin: Dict) -> Optional[str]:
    """
    Pinterest v5 returns media metadata. We prefer original image URL if available.
    Typical path: pin['media']['images']['original']['url']
    Skip videos for PoC.
    """
    media = pin.get("media") or {}
    if media.get("media_type", "").lower() == "video":
        return None
    images = media.get("images") or {}
    # try original, then 1200x, etc.
    for key in ("original", "orig", "xlarge", "large", "1200x"):
        if key in images and "url" in images[key]:
            return images[key]["url"]
    # fallback: top-level image_url if present
    if "image_url" in pin:
        return pin["image_url"]
    return None

def sanitize_filename(s: str) -> str:
    return "".join(c for c in s if c.isalnum() or c in ("-", "_", ".")).strip()

def download_image(url: str, out_dir: pathlib.Path, pin_id: str) -> Optional[pathlib.Path]:
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        ext = ".jpg"
        base = sanitize_filename(pin_id) or str(int(time.time() * 1000))
        fp = out_dir / f"{base}{ext}"
        # Some URLs may have query params; Pinterest CDN usually okay with vanilla GET.
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        resp.raise_for_status()
        with open(fp, "wb") as f:
            f.write(resp.content)
        return fp
    except Exception as e:
        print(f"[download-skip] {pin_id}: {e}", file=sys.stderr)
        return None

def find_boards(boards: List[Dict], names_or_ids: List[str]) -> List[Dict]:
    """Resolve board selection by exact name or id (case-insensitive for name)."""
    wanted = set(n.strip().lower() for n in names_or_ids)
    sel = []
    for b in boards:
        bid = str(b.get("id") or "")
        name = str(b.get("name") or "")
        if bid in names_or_ids or name.lower() in wanted:
            sel.append(b)
    return sel

def write_meta(rows: List[Dict], meta_csv: pathlib.Path):
    meta_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not meta_csv.exists()
    with open(meta_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "platform", "local_path", "pin_id", "board_id", "board_name",
                "url", "link", "title", "description", "created_at"
            ],
        )
        if write_header:
            w.writeheader()
        w.writerows(rows)

def main():
    ap = argparse.ArgumentParser(description="Pinterest v5 ingestor (images only) for fashion PoC")
    ap.add_argument("--token", default=os.getenv("PINTEREST_ACCESS_TOKEN"), help="Pinterest access token (env PINTEREST_ACCESS_TOKEN if omitted)")
    ap.add_argument("--boards", nargs="+", default=None, help="Board names or IDs to ingest. If omitted, ingests ALL boards you own/collaborate on (public/protected/secret).")
    ap.add_argument("--privacy", default=None, choices=["PUBLIC","PROTECTED","SECRET"], help="Filter boards by privacy (optional).")
    ap.add_argument("--limit-per-board", type=int, default=300, help="Max pins to fetch per board (pagination stops after this many).")
    ap.add_argument("--out-images", default="data/pinterest_images", help="Folder to store downloaded images.")
    ap.add_argument("--out-meta", default="data/pinterest_meta.csv", help="CSV to append pin metadata.")
    args = ap.parse_args()

    if not args.token:
        sys.exit("Missing access token. Set --token or PINTEREST_ACCESS_TOKEN.")

    out_dir = pathlib.Path(args.out_images)
    meta_csv = pathlib.Path(args.out_meta)

    # 1) List boards
    boards = list_boards(args.token, privacy=args.privacy)
    if not boards:
        sys.exit("No boards found for this token/account.")

    if args.boards:
        boards = find_boards(boards, args.boards)
        if not boards:
            sys.exit("Board name/ID not found. Tip: run without --boards to print IDs & names.")

    # Show boards summary
    print("Selected boards:")
    for b in boards:
        print(f"- {b.get('id')} :: {b.get('name')} :: privacy={b.get('privacy')}")

    # 2) Iterate boards → pins → download images
    all_rows: List[Dict] = []
    for b in boards:
        bid = str(b.get("id"))
        bname = b.get("name") or ""
        pins = list_pins_on_board(args.token, bid)
        if not pins:
            continue

        # respect a rough per-board cap
        count = 0
        for pin in tqdm(pins, desc=f"Board {bname}", unit="pin"):
            if count >= args.limit_per_board:
                break
            img_url = extract_image_url(pin)
            if not img_url:
                continue  # skip videos / pins without image url

            pin_id = str(pin.get("id") or "")
            fp = download_image(img_url, out_dir, pin_id)
            if not fp:
                continue

            # Metadata normalization
            link = pin.get("link") or pin.get("pin_url") or ""
            title = pin.get("title") or pin.get("grid_title") or ""
            desc = pin.get("description") or pin.get("note") or ""
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
            time.sleep(0.05)  # light throttle for CDN

    if all_rows:
        write_meta(all_rows, meta_csv)
        print(f"Saved {len(all_rows)} images → {out_dir}")
        print(f"Appended metadata → {meta_csv}")
    else:
        print("No images saved. Check board selection and token scopes.")

if __name__ == "__main__":
    main()
