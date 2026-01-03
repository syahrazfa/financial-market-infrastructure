import boto3, psycopg2, json
from datetime import datetime

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="finadmin",
    aws_secret_access_key="finpassword"
)

conn = psycopg2.connect(
    host="localhost",
    database="market",
    user="fin",
    password="finpass"
)

def load():
    cursor = conn.cursor()
    objs = s3.list_objects_v2(Bucket="fin-raw-lake")["Contents"]
    for o in objs:
        if "candles" in o["Key"]:
            data = json.loads(s3.get_object(Bucket="fin-raw-lake", Key=o["Key"])["Body"].read())
            for row in data:
                ts = datetime.fromtimestamp(ts/1000, tz=datetime.UTC)
                cursor.execute("""
		INSERT INTO bronze.candles (symbol, open_time, open, high, low, close, volume)
		SELECT %s,%s,%s,%s,%s,%s,%s
			WHERE NOT EXISTS (
			SELECT 1 FROM bronze.candles
			WHERE symbol=%s AND open_time=%s
			)
			""", (
			    "BTCUSDT", ts, o, h, l, c, v,
			    "BTCUSDT", ts
			))

    conn.commit()

load()
