import psycopg2
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://fin:finpass@localhost/market")

conn = psycopg2.connect(
    host="localhost",
    database="market",
    user="fin",
    password="finpass"
)

df = pd.read_sql("""
    SELECT f.time_id, t.ts, f.close, f.ema_20, f.ema_50
    FROM features.candle_features f
    JOIN silver.dim_time t ON f.time_id = t.time_id
    ORDER BY t.ts
""", engine)

df["signal"] = (df["ema_20"] > df["ema_50"]).astype(int)
df["position"] = df["signal"].shift(1)
df["pnl"] = df["position"] * df["close"].diff()

df = df.dropna()

cur = conn.cursor()
for _, r in df.iterrows():
    cur.execute("""
        INSERT INTO strategies.backtest_results
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        int(r["time_id"]),
        "EMA_CROSS",
        int(r["position"]),
        float(r["close"]),
        float(r["close"]),
        float(r["pnl"])
    ))
conn.commit()
