import boto3, json, datetime

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="finadmin",
    aws_secret_access_key="finpassword"
)

BUCKET="fin-raw-lake"

def upload_raw(exchange, market, dtype, symbol, data):
    now = datetime.datetime.utcnow()
    key = f"{exchange}/{market}/{dtype}/{now.year}/{now.month:02}/{now.day:02}/{symbol}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
