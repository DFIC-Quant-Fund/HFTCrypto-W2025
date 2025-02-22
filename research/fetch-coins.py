'''
Continously fetch new coins and store them in our MySQL DB,
as well as a JSON file containing all coins.

Coins in JSON are stored in format:
{
    "id:" int,
    "mint_address": VARCHAR,
    "creator_address": VARCHAR,
    "mint_date": VARCHAR,
    "coin_name"" VARCHAR,
    "coin_symbol:" VARCHAR,
    "
}
'''

import requests
import json
import os
import time
import mysql.connector
from mysql.connector import Error
from trades_db_utils import create_connection

from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


from dotenv import load_dotenv
import os
load_dotenv()

DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

# FETCH_COINS_URL = 'https://client-api-2-74b1891ee9f9.herokuapp.com/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false'
# FETCH_COINS_HEADERS = {
#     'Host': 'client-api-2-74b1891ee9f9.herokuapp.com',
#     'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
#     'sec-ch-ua-mobile': '?0',
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
#     'sec-ch-ua-platform': '"Windows"',
#     'Accept': '*/*',
#     'dnt': '1',
#     'Origin': 'https://pump.fun',
#     'Sec-Fetch-Site': 'cross-site',
#     'Sec-Fetch-Mode': 'cors',
#     'Sec-Fetch-Dest': 'empty',
#     'Referer': 'https://pump.fun/',
#     'Accept-Language': 'en-US,en;q=0.9',
# }

FETCH_COINS_URL = 'https://frontend-api-v3.pump.fun/coins?limit=5&offset=0&sort=created_timestamp&order=DESC&includeNsfw=false&complete=false'
FETCH_COINS_HEADERS = {
    'Host': 'frontend-api-v3.pump.fun',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

def  add_coin_to_database(connection, mint_address, creator_address, mint_date, coin_name, coin_symbol):
    """Insert a new coin into the coins table."""

    # sleep one second to avoid false duplicate creator with trade client
    time.sleep(1)

    query = "INSERT INTO coins (mint_address, creator_address, mint_date, coin_name, coin_symbol) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id"
    cursor = connection.cursor()
    cursor.execute(query, (mint_address, creator_address, mint_date, coin_name, coin_symbol))
    connection.commit()

def fetch_coins():
    coin_map = {}
    coin_map_file = 'coin_map.json'

    if os.path.exists(coin_map_file):
        with open(coin_map_file, 'r') as file:
            coin_map = json.load(file)

    connection = create_connection(DB_HOSTNAME, 'CoinTrades', DB_USER, DB_PASSWORD)

    while True:
        try:
            response = requests.get(FETCH_COINS_URL, headers=FETCH_COINS_HEADERS)
            coins = response.json()

            updated = False
            for coin in coins:
                print(coin)
                token_addr = coin["mint"]

                if token_addr not in coin_map:
                    creator_addr = coin["creator"]
                    mint_date = coin["created_timestamp"]
                    coin_name = coin["name"]
                    coin_symbol = coin["symbol"]
                    new_coin = {
                        "tokenAddr": token_addr,
                        "creatorAddr": creator_addr,
                        "createTime": mint_date,
                        "name": coin_name,
                        "symbol": coin_symbol,
                    }

                    coin_map[token_addr] = new_coin
                    updated = True
                    add_coin_to_database(connection, token_addr, creator_addr, mint_date, coin_name, coin_symbol)
                    print(f'Added new coin: {coin["name"]} {coin["mint"]}')

            # Save updated map to file if there were updates
            if updated:
                with open(coin_map_file, 'w') as file:
                    json.dump(coin_map, file)

        except Exception as e:
            print(f"Failed to fetch coins: {e}")

        # Wait 1 second before fetching coins again
        time.sleep(1)
fetch_coins()


