import requests
from uploader import upload_raw

def pull(symbol, interval, limit):
    url = f"https://data-api.binance.vision/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    SYMBOL = "BTCUSDT"
    INTERVAL = "1m"
    LIMIT = 1000

    raw_data = pull(SYMBOL, INTERVAL, LIMIT)

    upload_raw("binance", "spot", "candles", SYMBOL, raw_data)
