# ingest_reddit_public.py
import os, time, csv, argparse, requests
from datetime import datetime, timezone

HDRS = {"User-Agent": "fashion-poc/0.1 by yourusername"}  # keep this descriptive

def is_image(u):
    u = u.lower().split("?")[0]
    return u.endswith((".jpg",".jpeg",".png",".webp"))

def fetch_posts(sub, limit=100, sleep=0.5):
    url = f"https://www.reddit.com/r/{sub}/new.json?limit={limit}"
    r = requests.get(url, headers=HDRS, timeout=20)
    r.raise_for_status()
    data = r.json().get("data", {}).get("children", [])
    posts = []
    for c in data:
        d = c.get("data", {})
        img_url = d.get("url_overridden_by_dest") or d.get("url") or ""
        if not (img_url and is_image(img_url)):
            # fallback to preview (usually direct image)
            img_url = (
                d.get("preview", {})
                 .get("images", [{}])[0]
                 .get("source", {})
                 .get("url", "")
            ).replace("&amp;", "&")
        if img_url:
            posts.append({
                "id": d.get("id",""),
                "sub": sub,
                "url": img_url,
                "title": d.get("title",""),
                "ts": datetime.fromtimestamp(d.get("created_utc", 0), tz=timezone.utc).isoformat()
            })
    time.sleep(sleep)  # polite pause per sub
    return posts

def download(url, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    name = url.split("/")[-1].split("?")[0] or "img.jpg"
    # ensure extension
    if "." not in name: name += ".jpg"
    path = os.path.join(out_dir, f"{int(time.time()*1000)}_{name}")
    r = requests.get(url, headers=HDRS, timeout=30)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--subs", nargs="+", default=["malefashionadvice","streetwear"])
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--out", default="data/images")
    ap.add_argument("--meta", default="data/public_meta.csv")
    args = ap.parse_args()

    rows = []
    for sub in args.subs:
        try:
            posts = fetch_posts(sub, limit=args.limit)
            for p in posts:
                try:
                    lp = download(p["url"], args.out)
                    rows.append({
                        "local_path": lp, "id": p["id"], "sub": p["sub"],
                        "url": p["url"], "ts": p["ts"], "title": p["title"]
                    })
                    print(f"[OK] {sub} {p['id']} -> {lp}")
                    time.sleep(0.25)  # throttle to avoid 429s
                except Exception as e:
                    print(f"[skip-download] {p.get('id')} {e}")
        except Exception as e:
            print(f"[skip-sub] {sub} {e}")

    if rows:
        os.makedirs(os.path.dirname(args.meta), exist_ok=True)
        # append if file exists; else write header
        write_header = not os.path.exists(args.meta)
        with open(args.meta, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["local_path","id","sub","url","ts","title"])
            if write_header: w.writeheader()
            w.writerows(rows)
        print(f"Saved {len(rows)} rows → {args.meta}")

if __name__ == "__main__":
    main()
