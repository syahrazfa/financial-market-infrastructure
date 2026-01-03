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
    ORDER BY t.ts DESC
    LIMIT 1
""", engine)

row = df.iloc[0]

if row["ema_20"] > row["ema_50"]:
    signal = "BUY"
else:
    signal = "SELL"

cur = conn.cursor()
cur.execute("""
    INSERT INTO signals.live_signals (time_id, signal, direction, price)
    VALUES (%s,%s,%s,%s)
""", (int(row["time_id"]), "EMA_CROSS", signal, float(row["close"])))

conn.commit()
