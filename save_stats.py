import sqlite3
import time
import random
from datetime import datetime
import schedule
import requests
import logging
logging.basicConfig(filename="log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")
BASE_URL = "https://api.kraken.com"


def get_stats(pair):
    url = f"{BASE_URL}/0/public/Ticker?pair={pair}"
    response = requests.get(url).json()
    # print(response['result'][pair])
    if response.get("error"):
        logging.error("Kraken API Error: %s", response["error"])
        return None
    try:
        ask = float(response['result'][pair]['a'][0])  # Get the current ask price
        bid = float(response['result'][pair]['b'][0])
        close = float(response['result'][pair]['c'][0])
        volume = float(response['result'][pair]['v'][1])
        VWAP = float(response['result'][pair]['p'][1])
        trades = float(response['result'][pair]['t'][1])
        low = float(response['result'][pair]['l'][1])
        high = float(response['result'][pair]['h'][1])
        open = float(response['result'][pair]['o'])

        return {
            "a": ask,  # Ask price
            "b": bid,  # Bid price
            "c": close,  # Close price
            "v": volume,  # Volume
            "p": VWAP,  # Average price
            "t": trades,  # Trades count
            "l": low,  # Low price
            "h": high,  # High price
            "o": open,  # Open price
        }
    except KeyError:
        logging.error("Price data for %s not found in response.", pair)
        return None


# Initialize database for a specific pair
def initialize_db(pair):
    db_name = f"{pair.replace('/', '_')}.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ask REAL,
        bid REAL,
        close REAL,
        volume REAL,
        avg_price REAL,
        trades_count INTEGER,
        low REAL,
        high REAL,
        open REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()
    return db_name

# Store stats in the pair-specific database
def store_stats(db_name, stats):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO stats (ask, bid, close, volume, avg_price, trades_count, low, high, open)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (stats["a"], stats["b"], stats["c"], stats["v"], stats["p"], stats["t"], stats["l"], stats["h"], stats["o"]))
    conn.commit()
    conn.close()

# Fetch and store stats for a specific pair
def fetch_and_store(pair):
    db_name = initialize_db(pair)  # Ensure DB is initialized
    stats = get_stats(pair)
    store_stats(db_name, stats)
    print(f"[{datetime.now()}] Stored stats for {pair}: {stats}")

# Schedule fetching for all pairs with a random delay
def schedule_fetching(pairs, interval=30):
    for pair in pairs:
        random_offset = random.uniform(0, 5)  # Add a random delay of 0-5 seconds

        def fetch_with_delay(pair=pair, offset=random_offset):
            time.sleep(offset)  # Wait for the random offset
            fetch_and_store(pair=pair)

        schedule.every(interval).seconds.do(fetch_with_delay)
    print(f"Scheduled stats fetching every {interval} seconds with random offsets for pairs: {', '.join(pairs)}")

# Run scheduled jobs
def run_scheduler():
    while True:
        schedule.run_pending()
        random_sleep = random.uniform(5, 15)  # Sleep for 5 to 15 seconds
        time.sleep(random_sleep)

# Query stored data for analysis
def query_data(pair, start_time=None, end_time=None):
    db_name = f"{pair.replace('/', '_')}.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    query = "SELECT * FROM stats WHERE 1=1"
    params = []
    if start_time:
        query += " AND timestamp >= ?"
        params.append(start_time)
    if end_time:
        query += " AND timestamp <= ?"
        params.append(end_time)
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    conn.close()
    return rows


# Main
if __name__ == "__main__":
    print("Starting")
    # Define trading pairs and intervals
    trading_pairs = ["XXBTZUSD", "XETHZUSD", "XDGUSD", "SHIBUSD", "WIFUSD", "DOTUSD", "XXMRZUSD", "SUIUSD", "UNIUSD",
                     "POPCATUSD", "MNGOUSD", "PEPEUSD", "FARMUSD", "POLUSD", "SOLUSD", "ADAUSD", "XXLMZUSD",
                     "XXRPZUSD", "SAMOUSD", "RAYUSD"]

    interval_seconds = 60

    # Schedule fetching
    schedule_fetching(trading_pairs, interval=interval_seconds)

    # Run scheduler
    try:
        run_scheduler()
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
