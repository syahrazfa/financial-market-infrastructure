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
    SELECT f.symbol_id, f.time_id, t.ts, f.close
    FROM silver.fact_candles f
    JOIN silver.dim_time t ON f.time_id = t.time_id
    ORDER BY t.ts
""", engine)

df["ema_20"] = df["close"].ewm(span=20).mean()
df["ema_50"] = df["close"].ewm(span=50).mean()
df["volatility"] = df["close"].rolling(20).std()

delta = df["close"].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)
rs = gain.rolling(14).mean() / loss.rolling(14).mean()
df["rsi_14"] = 100 - (100 / (1 + rs))

df = df.dropna()

cur = conn.cursor()

for _, r in df.iterrows():
    cur.execute("""
        INSERT INTO features.candle_features
        (symbol_id, time_id, close, ema_20, ema_50, rsi_14, volatility)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        int(r["symbol_id"]),
        int(r["time_id"]),
        float(r["close"]),
        float(r["ema_20"]),
        float(r["ema_50"]),
        float(r["rsi_14"]),
        float(r["volatility"])
    ))

conn.commit()

