import requests
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List

HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': 'Windows',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}
BASE_URL = "https://www.zara.com/{iso2}/en/stores-locator/search"
KEYS = ["datatype", "kind", "id", "latitude", "longitude", "city", "type", "status", "country"]

data = pd.read_csv("data/worldcities.csv")
data = data.dropna(subset=['iso2'])
data = data[data['population'] >= 40000]
city_points = data[['lat', 'lng', 'iso2']]


def fetch_store_data(row: Dict[str, Any], headers: Dict[str, str], keys: List[str], base_url: str):
    params = {
        'lat': row['lat'],
        'lng': row['lng'],
        'isDonationOnly': 'false',
        'ajax': 'true'
    }

    try:
        iso2 = row['iso2'].lower()
        response = requests.get(base_url.format(iso2=iso2), headers=headers, params=params)
        if response.status_code == 200:
            store_data = response.json()
            return [{key: store.get(key, None) for key in keys} for store in store_data]
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    return []


city_list = city_points.to_dict('records')

results = []
with ThreadPoolExecutor(max_workers=24) as executor:
    future_to_city = {executor.submit(fetch_store_data, city, HEADERS, KEYS, BASE_URL): city for city in city_list}
    for future in tqdm(as_completed(future_to_city), total=len(future_to_city), desc="Fetching data"):
        result = future.result()
        if result:
            results.extend(result)

data = pd.DataFrame(results)
data = data.drop_duplicates(subset='id')
data = data.to_csv('data/all_stores.csv', index=False)
