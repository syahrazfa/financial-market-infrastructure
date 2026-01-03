import time, requests, subprocess

while True:
    # Pull latest candle
    subprocess.run(["python", "ingestion/binance_spot_candles/main.py"])
    
    # Replay raw into bronze
    subprocess.run(["python", "lake/replay_candles.py"])
    
    # Refresh features
    subprocess.run(["python", "features/build_features.py"])
    
    # Generate live signal
    subprocess.run(["python", "signals/run_live_signals.py"])
    
    time.sleep(60)  # 1 minute heartbeat
