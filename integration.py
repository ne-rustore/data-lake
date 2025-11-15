import time
import json
import requests
import pandas as pd
from urllib.parse import quote

COUNTRY = 'ru'
ENTITY = 'software'
LIMIT = 200
TARGET_UNIQUE_APPS = 10000
SLEEP_BETWEEN_REQUESTS = 0.2

OUTPUT_CSV = 'apps.csv'

TERMS = [
    "а","б","в","г","д","е","ж","з","и","й","к","л","м","н","о","п","р","с","т","у","ф","х","ц","ч","ш","щ","э","ю","я",
    
    "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",

    "game","games","bank","finance","photo","music","video","chat","social","fitness","food",
    "kids","travel","taxi","note","weather","sport","news","education","shop","store","movie",
]

seen_ids = set()
rows = []
def fetch_apps(term: str):
    url = (
        "https://itunes.apple.com/search"
        f"?term={quote(term)}"
        f"&entity={ENTITY}"
        f"&country={COUNTRY}"
        f"&limit={LIMIT}"

    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])

def process_result(app: dict) -> dict:
    return {
        "track_id": app.get('trackId'),
        'track_name': app.get('trackName'),
        'description': app.get('description'),
        'primary_genre': app.get('primaryGenreName'),
        'genres': ', '.join(app.get('genres', [])),
        'bundle_id': app.get('bundleId'),
        'seller_name': app.get('sellerName'),
        'developer_name': app.get('artistName'),
        'average_rating': app.get('averageUserRating'),
        'rating_count': app.get('userRatingCount'),
        'price': app.get('price'),
        'currency': app.get('currency'),
        'icon_url': app.get('artworkUrl100'),

        'screenshot_urls': json.dumps(app.get('screenshotUrls', []), ensure_ascii=False),
        'language_codes': ', '.join(app.get('languageCodesISO2A', [])),
        'release_date': app.get('releaseDate'),
        'version': app.get('version'),
    }

def main():
    global rows

    for idx, term in enumerate(TERMS):
        if len(seen_ids) >= TARGET_UNIQUE_APPS:
            print('набрали максимум')
            break

        print(f"[{idx+1}/len{TERMS}] term='{term}' ...", end = ' ', flush=True)

        try:
            results = fetch_apps(term)
        except Exception as e:
            print(f"ERROR: {e}")
            continue

        added_for_term = 0
        for app in results:
            track_id = app.get('trackId')
            if not track_id or track_id in seen_ids:
                continue

            seen_ids.add(track_id)
            rows.append(process_result(app))
            added_for_term += 1
            
            if len(seen_ids) >= TARGET_UNIQUE_APPS:
                break

        print(f"получили {len(results)}, добавили {added_for_term}, уникальных: {len(seen_ids)}")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    if not rows:
        print("Ниче нету")
        return
    
    df = pd.DataFrame(rows)
    print(f"Итог: {len(df)}")

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved to {OUTPUT_CSV}")

    print(df[['track_id', 'track_name', 'primary_genre', 'average_rating', 'rating_count']].head(10))

if __name__ == '__main__':
    main()