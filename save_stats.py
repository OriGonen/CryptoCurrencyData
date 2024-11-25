import sqlite3
import time
import random
from datetime import datetime
import schedule
import requests
import logging
logging.basicConfig(filename="log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")
BASE_URL = "https://api.kraken.com"


def get_stats(pair, max_retries=5, initial_wait=10):
    """
    Fetch stats for the given trading pair from the Kraken API with retry logic.
    :param pair: Trading pair (e.g., "XXBTZUSD")
    :param max_retries: Maximum number of retries
    :param initial_wait: Initial wait time in seconds for exponential backoff
    :return: Dictionary of stats or None if retries are exhausted
    """
    retries = 0
    while retries < max_retries:
        try:
            url = f"{BASE_URL}/0/public/Ticker?pair={pair}"
            response = requests.get(url, timeout=10)  # Add timeout to avoid indefinite hangs
            response.raise_for_status()  # Raise exception for HTTP errors
            data = response.json()

            # Handle potential API errors
            if data.get("error"):
                logging.error("Kraken API Error for %s: %s", pair, data["error"])
                return None

            # Parse response data
            ask = float(data['result'][pair]['a'][0])  # Ask price
            bid = float(data['result'][pair]['b'][0])
            close = float(data['result'][pair]['c'][0])
            volume = float(data['result'][pair]['v'][1])
            VWAP = float(data['result'][pair]['p'][1])
            trades = int(data['result'][pair]['t'][1])
            low = float(data['result'][pair]['l'][1])
            high = float(data['result'][pair]['h'][1])
            open = float(data['result'][pair]['o'])

            return {
                "a": ask,
                "b": bid,
                "c": close,
                "v": volume,
                "p": VWAP,
                "t": trades,
                "l": low,
                "h": high,
                "o": open,
            }
        except (requests.exceptions.RequestException, KeyError) as e:
            logging.warning("Error fetching stats for %s: %s", pair, str(e))
            retries += 1
            wait_time = initial_wait * (2 ** retries)  # Exponential backoff
            logging.info("Retrying in %d seconds... (%d/%d)", wait_time, retries, max_retries)
            time.sleep(wait_time)

    logging.error("Max retries exceeded for %s. Skipping.", pair)
    return None


# Fetch and store stats for a specific pair
def fetch_and_store(pair):
    db_name = initialize_db(pair)  # Ensure DB is initialized
    stats = get_stats(pair)
    if stats:  # Only store stats if retrieval was successful
        store_stats(db_name, stats)
        print(f"[{datetime.now()}] Stored stats for {pair}: {stats}")
    else:
        print(f"[{datetime.now()}] Failed to fetch stats for {pair}. Check logs for details.")


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
