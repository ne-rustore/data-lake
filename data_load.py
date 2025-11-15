import os
import io
import json
import mimetypes
from urllib.parse import urlparse

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import Json
from minio import Minio

CSV_PATH = 'apps.csv' 

PG_HOST = 'localhost'
PG_PORT = 5433
PG_USER = 'rustore_pg'
PG_PASSWORD = 'rustore_pg'
PG_DB = 'rustore'

MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'rustore_minio'
MINIO_SECRET_KEY = 'rustore_minio'
MINIO_BUCKET = 'apps-media'
MINIO_SECURE = False

HTTP_TIMEOUT = 10

def guess_ext(url: str, default='.jpg') -> str:
    path = urlparse(url).path
    ext = os.path.splitext(path)[1]
    return ext if ext else default

def download(url: str) -> bytes | None:
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        print(f"[WARN] failed to download {url}: {e}")
        return None
    
def get_mime_from_ext(ext: str) -> str:
    mime, _ = mimetypes.guess_type('x' + ext)
    return mime or 'application/octet-stream'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"[MINIO] create bucket {MINIO_BUCKET}")

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    user=PG_USER,
    password=PG_PASSWORD,
    dbname=PG_DB,
)
conn.autocommit = True
cur = conn.cursor()

def upload_to_minio(key: str, data: bytes):
    ext = os.path.splitext(key)[1]
    mime = get_mime_from_ext(ext)
    minio_client.put_object(
        MINIO_BUCKET,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type=mime,
    )

def process_row(row):
    track_id = int(row['track_id'])
    track_name = row.get('track_name')
    print(f'\n[APP] {track_id} - {track_name}')

    icon_url = row.get('icon_url')
    icon_key = None

    if isinstance(icon_url, str) and icon_url.strip():
        ext = guess_ext(icon_url, '.jpg')
        icon_key = f"icons/{track_id}{ext}"
        data = download(icon_url)
        if data:
            upload_to_minio(icon_key, data)
            print(f"[+] icon -> {icon_key}")
        else:
            icon_key = None

    screenshots_raw = row.get('screenshot_urls')
    screenshot_keys = []

    if isinstance(screenshots_raw, str) and screenshots_raw.strip():
        try:
            urls = json.loads(screenshots_raw)
            if isinstance(urls, list):
                for idx, url in enumerate(urls):
                    if not isinstance(url, str) or not url.strip():
                        continue
                    ext = guess_ext(url, '.jpg')
                    key = f"screenshots/{track_id}/{idx+1}{ext}"
                    data = download(url)
                    if data:
                        upload_to_minio(key, data)
                        screenshot_keys.append(key)
                        print(f"[+] screenshot -> {key}")
        except Exception as e:
            print("[WARN] failed to parse screenshot URLs")

    cur.execute(
        """
        insert into applications (
            track_id,
            track_name,
            description,
            primary_genre,
            genres,
            bundle_id,
            seller_name,
            developer_name,
            average_rating,
            rating_count,
            price,
            currency,
            icon_key,
            screenshot_keys,
            release_date,
            version
        )
        values (
            %(track_id)s,
            %(track_name)s,
            %(description)s,
            %(primary_genre)s,
            %(genres)s,
            %(bundle_id)s,
            %(seller_name)s,
            %(developer_name)s,
            %(average_rating)s,
            %(rating_count)s,
            %(price)s,
            %(currency)s,
            %(icon_key)s,
            %(screenshot_keys)s,
            %(release_date)s,
            %(version)s
        )
        on conflict (track_id) do update set
            track_name = excluded.track_name, 
            description = excluded.description,
            primary_genre = excluded.primary_genre,
            genres = excluded.genres,
            bundle_id = excluded.bundle_id,
            seller_name = excluded.seller_name,
            developer_name = excluded.developer_name,
            average_rating = excluded.average_rating,
            rating_count = excluded.rating_count,
            price = excluded.price,
            currency = excluded.currency,
            icon_key = excluded.icon_key,
            screenshot_keys = excluded.screenshot_keys,
            release_date = excluded.release_date,
            version = excluded.version;
        """,
        {
            'track_id': track_id,
            'track_name': row.get('track_name'),
            'description': row.get('description'),
            'primary_genre': row.get('primary_genre'),
            'genres': row.get('genres'),
            'bundle_id': row.get('bundle_id'),
            'seller_name': row.get('seller_name'),
            'developer_name': row.get('developer_name'),
            'average_rating': row.get('average_rating'),
            'rating_count': row.get('rating_count'),
            'price': row.get('price'),
            'currency': row.get('currency'),
            'icon_key': icon_key,
            'screenshot_keys': Json(screenshot_keys),
            'release_date': row.get('release_date'),
            'version': row.get('version'),
        },
    )
    print(" [pg] row upserted")

def main():
    df = pd.read_csv(CSV_PATH)
    print(f"Строк в CSV: {len(df)}")

    for i, row in df.iterrows():
        process_row(row)

    print("Done")
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()