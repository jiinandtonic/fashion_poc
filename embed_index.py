# embed_index.py
import os, glob, numpy as np, torch, clip, faiss
from PIL import Image
from sqlalchemy import create_engine, text
from datetime import datetime
import csv

META_CSV = "data/public_meta.csv"
meta_by_path = {}
if os.path.exists(META_CSV):
    with open(META_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            meta_by_path[r["local_path"]] = r

DB = "sqlite:///data/items.sqlite"
engine = create_engine(DB, future=True)
os.makedirs("data", exist_ok=True)

STYLES = ["streetwear","formal","business casual","vintage","minimalist","grunge"]

@torch.no_grad()
def load_model():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, preprocess = clip.load("ViT-B/32", device=device)
    txt = torch.cat([clip.tokenize(f"a photo of {s} menswear outfit") for s in STYLES]).to(device)
    return device, model, preprocess, txt

@torch.no_grad()
def embed_img(path, model, preprocess, device):
    img = preprocess(Image.open(path).convert("RGB")).unsqueeze(0).to(device)
    v = model.encode_image(img)
    v = v / v.norm(dim=-1, keepdim=True)
    return v.cpu().numpy().astype("float32")

@torch.no_grad()
def zero_shot_style(v, model, text_tokens, device):
    v_t = torch.from_numpy(v).to(device)
    t = model.encode_text(text_tokens)
    t = t / t.norm(dim=-1, keepdim=True)
    logits = (v_t @ t.T).softmax(dim=-1).cpu().numpy()[0]
    i = int(logits.argmax())
    return STYLES[i], float(logits[i])

def ensure_tables():
    with engine.begin() as con:
        con.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS items(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT, url TEXT, local_path TEXT,
          ts TEXT, style TEXT, prob REAL,
          emb BLOB
        )""")

def upsert_item(meta, style, prob, emb):
    with engine.begin() as con:
        con.execute(text("""
          INSERT INTO items(source,url,local_path,ts,style,prob,emb)
          VALUES(:source,:url,:local,:ts,:style,:prob,:emb)
        """), dict(source=meta.get("sub","reddit"),
                   url=meta.get("url",""),
                   local=meta["local_path"],
                   ts=meta.get("ts", datetime.utcnow().isoformat()),
                   style=style, prob=prob,
                   emb=emb.tobytes()))

def build_faiss():
    with engine.begin() as con:
        rows = con.execute(text("SELECT emb FROM items")).fetchall()
    if not rows: return
    X = np.vstack([np.frombuffer(r[0], dtype="float32").reshape(1,-1) for r in rows])
    # cosine → normalize, then inner product
    faiss.normalize_L2(X)
    index = faiss.IndexFlatIP(X.shape[1])
    index.add(X)
    faiss.write_index(index, "data/index.faiss")

if __name__ == "__main__":
    ensure_tables()
    device, model, preprocess, text_tokens = load_model()
    for p in glob.glob("data/images/*"):
        v = embed_img(p, model, preprocess, device)
        style, prob = zero_shot_style(v, model, text_tokens, device)
        meta = {"local_path": p, "ts": datetime.utcnow().isoformat(), "url": "", "sub":"reddit"}
        upsert_item(meta, style, prob, v)
        
        # ... inside your loop over images:
        meta = meta_by_path.get(p, {})
        upsert_item(
            {
            "sub": meta.get("sub","reddit"),
            "url": meta.get("url",""),
            "ts": meta.get("ts",""),
            "local_path": p
            },
            style, prob, v
        )
    build_faiss()
    print("Indexed.")
