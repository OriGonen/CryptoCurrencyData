import sqlite3
import time
from datetime import datetime
import random
import schedule
import threading
import requests
import logging
logging.basicConfig(filename="log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")
BASE_URL = "https://api.kraken.com"


# Function to get the current price of the asset pair
def get_price(pair):
    url = f"{BASE_URL}/0/public/Ticker?pair={pair}"
    response = requests.get(url).json()
    # print(response['result'][pair])
    if response.get("error"):
        logging.error("Kraken API Error: %s", response["error"])
        return None
    try:
        price = float(response['result'][pair]['c'][0])  # Get the current ask price
        return price
    except KeyError:
        logging.error("Price data for %s not found in response.", pair)
        return None


# Initialize database for a specific pair
def initialize_db(pair):
    db_name = f"{pair.replace('/', '_')}.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        price REAL NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()
    return db_name


# Store price in the pair-specific database
def store_price(db_name, price):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO prices (price) VALUES (?)", (price,))
    conn.commit()
    conn.close()


# Fetch and store price for a specific pair
def fetch_and_store(pair):
    db_name = initialize_db(pair)  # Ensure DB is initialized
    price = get_price(pair)
    store_price(db_name, price)
    print(f"[{datetime.now()}] Stored {pair} price: {price}")


# Schedule fetching for all pairs with a random delay
def schedule_fetching(pairs, interval=30):
    for pair in pairs:
        random_offset = random.uniform(0, 5)  # Add a random delay of 0-5 seconds
        schedule.every(interval).seconds.do(fetch_and_store, pair=pair)
        schedule.every(interval).seconds.at(random_offset).do(fetch_and_store, pair=pair)
    print(f"Scheduled price fetching every {interval} seconds with random offsets for pairs: {', '.join(pairs)}")


# Run scheduled jobs
def run_scheduler():
    while True:
        schedule.run_pending()

        # random POSITIVE wait
        waittime = -1
        while waittime <= 0:
            waittime = 5 + random.uniform(0, 4) + random.gauss(0.5, 2)

        time.sleep(
            waittime
        )  # Sleep for a few seconds between checking jobs


# Query stored data for analysis
def query_data(pair, start_time=None, end_time=None):
    db_name = f"{pair.replace('/', '_')}.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    query = "SELECT * FROM prices WHERE 1=1"
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
    # Define the trading pairs and start fetching
    trading_pairs = ["XXBTZUSD", "XETHZUSD", "XDGUSD", "SHIBUSD", "WIFUSD", "DOTUSD", "XXMRZUSD", "SUIUSD", "UNIUSD",
                     "POPCATUSD", "MNGOUSD", "PEPEUSD", "FARMUSD", "POLUSD", "SOLUSD", "ADAUSD", "XXLMZUSD",
                     "XXRPZUSD", "SAMOUSD", "RAYUSD"]

    interval_seconds = 60
    # Schedule fetching
    schedule_fetching(trading_pairs, interval=interval_seconds)


    # Test the pairs are written correctly
    # for pair in trading_pairs:
    #     print(f"{pair}: {get_price(pair)}")
    # print("done")

    # Run scheduler
    try:
        run_scheduler()
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
