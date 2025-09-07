# ingest_reddit.py
import os, time, requests
import praw
from urllib.parse import urlparse
from datetime import datetime, timezone

SUBS = ["malefashionadvice", "streetwear"]
SAVE_DIR = "data/images"
os.makedirs(SAVE_DIR, exist_ok=True)

def is_image(u): return any(u.lower().endswith(ext) for ext in [".jpg",".jpeg",".png",".webp"])

def fetch(limit=200):
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_SECRET"),
        user_agent="fashion-poc"
    )
    posts = []
    for sub in SUBS:
        for p in reddit.subreddit(sub).new(limit=limit):
            if p.url and is_image(p.url):
                posts.append(dict(
                    id=p.id, sub=sub, url=p.url,
                    title=p.title, ts=datetime.fromtimestamp(p.created_utc, tz=timezone.utc).isoformat()
                ))
    return posts

def download(url, out_dir=SAVE_DIR):
    fn = os.path.basename(urlparse(url).path)
    fp = os.path.join(out_dir, f"{int(time.time()*1000)}_{fn}")
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    with open(fp, "wb") as f: f.write(r.content)
    return fp

if __name__ == "__main__":
    for post in fetch():
        try:
            lp = download(post["url"])
            print(post["id"], lp)
        except Exception as e:
            print("skip", post["id"], e)
