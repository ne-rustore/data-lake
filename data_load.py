import os
import io
import json
import time
import random
import mimetypes
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import Json, execute_values
from minio import Minio
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CSV_PATH = 'apps.csv' 

PG_HOST = 'localhost'
PG_PORT = 15433
PG_USER = 'rustore_pg'
PG_PASSWORD = 'rustore_pg'
PG_DB = 'rustore'

MINIO_ENDPOINT = 'localhost:19000'
MINIO_ACCESS_KEY = 'rustore_minio'
MINIO_SECRET_KEY = 'rustore_minio'
MINIO_BUCKET = 'apps-media'
MINIO_SECURE = False

HTTP_TIMEOUT = 10
MAX_WORKERS = 8
MEDIA_PASSES = 3

def make_session() -> requests.Session:
    retry = Retry(
        total=10,
        connect=10,
        read=10,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=100,
        pool_maxsize=100,
    )

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        )
    })
    return session

http_session = make_session()

def guess_ext(url: str, default='.jpg') -> str:
    path = urlparse(url).path
    ext = os.path.splitext(path)[1]
    return ext if ext else default

def download(url: str) -> bytes | None:
    if not url:
        return None
    try:
        resp = http_session.get(url, timeout=HTTP_TIMEOUT, stream=True)
        resp.raise_for_status()
        data = resp.content
        return data
    except requests.exceptions.RequestException as e:
        print(f"[WARN] failed to download {url}: {e!r}")
        return None
    finally:
        time.sleep(0.05 + random.random() * 0.1)
    
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

def prepare_data(df: pd.DataFrame):
    records_for_db = []
    media_jobs = []

    for row in df.itertuples(index=False):
        track_id = int(getattr(row, "track_id"))
        track_name = getattr(row, "track_name", None)
        print(f"\n[APP] {track_id} - {track_name}")

        icon_url = getattr(row, "icon_url", None)
        icon_key = None

        if isinstance(icon_url, str) and icon_url.strip():
            ext = guess_ext(icon_url, ".jpg")
            icon_key = f"icons/{track_id}{ext}"
            media_jobs.append(
                {
                    "url": icon_url,
                    "key": icon_key,
                    "track_id": track_id,
                    "kind": "icon",
                }
            )

        screenshots_raw = getattr(row, "screenshot_urls", None)
        screenshot_keys: list[str] = []

        if isinstance(screenshots_raw, str) and screenshots_raw.strip():
            try:
                urls = json.loads(screenshots_raw)
                if isinstance(urls, list):
                    for idx, url in enumerate(urls):
                        if not isinstance(url, str) or not url.strip():
                            continue
                        ext = guess_ext(url, ".jpg")
                        key = f"screenshots/{track_id}{idx+1}{ext}"
                        screenshot_keys.append(key)
                        media_jobs.append(
                            {
                                "url": url,
                                "key": key,
                                "track_id": track_id,
                                "kind": "screenshot", 
                            }
                        )
            except Exception as e:
                print(f"[WARN] failed to parse screenshot URLs for {track_id}: {e}")

        records_for_db.append(
            (
                track_id,
                getattr(row, "track_name", None),
                getattr(row, "description", None),
                getattr(row, "primary_genre", None),
                getattr(row, "genres", None),
                getattr(row, "bundle_id", None),
                getattr(row, "seller_name", None),
                getattr(row, "developer_name", None),
                getattr(row, "average_rating", None),
                getattr(row, "rating_count", None),
                getattr(row, "price", None),
                getattr(row, "currency", None),
                icon_key,
                Json(screenshot_keys),
                getattr(row, "release_date", None),
                getattr(row, "version", None),
            )
        )

    return records_for_db, media_jobs

def upsert_apps(records_for_db):
    if not records_for_db:
        print("[PG] nothing to insert")
        return
    
    print(f"[PG] upserting {len(records_for_db)} apps...")

    query = """
        INSERT INTO applications (
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
        VALUES %s
        ON CONFLICT (track_id) DO UPDATE SET
            track_name = EXCLUDED.track_name,
            description = EXCLUDED.description,
            primary_genre = EXCLUDED.primary_genre,
            genres = EXCLUDED.genres,
            bundle_id = EXCLUDED.bundle_id,
            seller_name = EXCLUDED.seller_name,
            developer_name = EXCLUDED.developer_name,
            average_rating = EXCLUDED.average_rating,
            rating_count = EXCLUDED.rating_count,
            price = EXCLUDED.price,
            currency = EXCLUDED.currency,
            icon_key = EXCLUDED.icon_key,
            screenshot_keys = EXCLUDED.screenshot_keys,
            release_date = EXCLUDED.release_date,
            version = EXCLUDED.version;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, records_for_db, page_size=500)

    print("[PG] upsert done")

def process_media_job(job: dict) -> bool:
    url = job['url']
    key = job['key']
    track_id = job['track_id']
    kind = job['kind']

    data = download(url)
    if not data:
        print(f"[MEDIA] {kind} for {track_id} failed: download error")
        return False
    
    try:
        upload_to_minio(key, data)
        return True
    except Exception as e:
        print(f"[MEDIA] {kind} for {track_id} upload failed: {e}")
        return False
    
def upload_media_parallel(media_jobs: list[dict]):
    if not media_jobs:
        print("[MEDIA] no jobs")
        return

    remaining = list(media_jobs)
    total = len(remaining)
    total_ok = 0
    total_fail = 0

    for attempt in range(1, MEDIA_PASSES + 1):
        if not remaining:
            break

        print(f"[MEDIA] pass {attempt}/{MEDIA_PASSES}: "
              f"trying {len(remaining)} jobs with {MAX_WORKERS} workers...")

        ok = 0
        fail = 0
        next_remaining: list[dict] = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_media_job, job): job for job in remaining}

            for fut in as_completed(futures):
                job = futures[fut]
                try:
                    if fut.result():
                        ok += 1
                        total_ok += 1
                    else:
                        fail += 1
                        next_remaining.append(job)
                except Exception as e:
                    fail += 1
                    total_fail += 1
                    next_remaining.append(job)
                    print(f"[MEDIA] worker error for {job.get('track_id')}: {e}")

        total_fail += fail
        remaining = next_remaining

        print(f"[MEDIA] pass {attempt} done: ok={ok}, fail={fail}, "
              f"remaining={len(remaining)}")

        if remaining and attempt < MEDIA_PASSES:
            time.sleep(2)

    if remaining:
        print(f"[MEDIA] after {MEDIA_PASSES} passes still failed: {len(remaining)} jobs")

    print(f"[MEDIA] total: requested={total}, ok={total_ok}, fail={total}")

def main():
    df = pd.read_csv(CSV_PATH)
    print(f"[CSV] rows: {len(df)}")

    print("[STEP 1] preparing records & media jobs...")
    records_for_db, media_jobs = prepare_data(df)
    print(f"[STEP 1] prepared {len(records_for_db)} db records, {len(media_jobs)} media jobs")

    print("[STEP 2] upserting into Postgres")
    upsert_apps(records_for_db)

    print("[STEP 3] uploading media in parallel...")
    upload_media_parallel(media_jobs)

    conn.close()
    print("[ALL] done")

if __name__ == '__main__':
    main()