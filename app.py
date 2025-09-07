# app.py
import gradio as gr, numpy as np, faiss, torch, clip
from PIL import Image
from sqlalchemy import create_engine, text
import pandas as pd

DB = "sqlite:///data/items.sqlite"
engine = create_engine(DB, future=True)

device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load("ViT-B/32", device=device)
index = faiss.read_index("data/index.faiss")

def embed(img: Image.Image):
    with torch.no_grad():
        x = preprocess(img.convert("RGB")).unsqueeze(0).to(device)
        v = model.encode_image(x)
        v = v / v.norm(dim=-1, keepdim=True)
    return v.cpu().numpy().astype("float32")

def recommend(user_img, styles, k=24):
    q = embed(user_img)
    faiss.normalize_L2(q)
    D,I = index.search(q, k*5)  # get a bigger pool
    with engine.begin() as con:
        items = pd.read_sql(text("SELECT rowid,* FROM items"), con)
        trends = pd.read_sql(text("SELECT * FROM trends"), con)
    # latest velocity per style for recency
    latest = trends.sort_values("day").groupby("style").tail(1).set_index("style")["velocity"].to_dict()
    chosen = []
    for idx, dist in zip(I[0], D[0]):
        row = items.iloc[idx]
        if styles and row["style"] not in styles: 
            continue
        trend_boost = max(0.0, latest.get(row["style"], 0.0))
        score = float(dist) + 0.25*trend_boost
        chosen.append((score, row["local_path"], row["style"]))
        if len(chosen) >= k: break
    chosen.sort(reverse=True)
    images = [c[1] for c in chosen]
    captions = [c[2] for c in chosen]
    return images, captions

demo = gr.Interface(
    fn=recommend,
    inputs=[
        gr.Image(type="pil", label="Upload your photo (full-body if possible)"),
        gr.CheckboxGroup(choices=["streetwear","formal","business casual","vintage","minimalist","grunge"], label="Filter by styles (optional)")
    ],
    outputs=[gr.Gallery(label="Recommendations").style(grid=4, height="auto"), gr.JSON(label="Styles")],
    title="Personalized Trend Recs (PoC)",
    description="Matches trending looks to you using CLIP similarity + trend velocity."
)

if __name__ == "__main__":
    demo.launch()
