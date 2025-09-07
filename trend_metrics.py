# trend_metrics.py
from sqlalchemy import create_engine, text
import pandas as pd

DB = "sqlite:///data/items.sqlite"
engine = create_engine(DB, future=True)

def calc_trends(span=5):
    with engine.begin() as con:
        df = pd.read_sql(text("SELECT style, ts FROM items"), con, parse_dates=["ts"])
    df["day"] = df["ts"].dt.date
    g = df.groupby(["style","day"]).size().reset_index(name="count")
    out = []
    for style, gdf in g.groupby("style"):
        gdf = gdf.sort_values("day")
        gdf["ema"] = gdf["count"].ewm(span=span).mean()
        gdf["velocity"] = gdf["ema"].diff().fillna(0)
        for _, r in gdf.iterrows():
            out.append((style, str(r["day"]), int(r["count"]), float(r["ema"]), float(r["velocity"])))
    with engine.begin() as con:
        con.exec_driver_sql("""CREATE TABLE IF NOT EXISTS trends(style TEXT, day TEXT, count INT, ema REAL, velocity REAL)""")
        con.exec_driver_sql("DELETE FROM trends")
        con.execute(text("INSERT INTO trends(style, day, count, ema, velocity) VALUES(:s,:d,:c,:e,:v)"),
                    [dict(s=s,d=d,c=c,e=e,v=v) for s,d,c,e,v in out])

if __name__ == "__main__":
    calc_trends()
