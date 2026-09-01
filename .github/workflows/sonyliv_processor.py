#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SONY LIV PROCESSOR – GitHub Actions Edition
(No caching, cross-platform, resume-capable)
"""

import os
import sys
import asyncio
import pandas as pd
import time
import aiohttp
import csv
import signal
import json
import re
import datetime
from typing import Dict, List, Set
from aiohttp import ClientTimeout, TCPConnector, ClientError
from tqdm.asyncio import tqdm as tqdm_asyncio

# ============================================================
# 0. CONFIGURATION – READ FROM ENVIRONMENT
# ============================================================
PART_NUMBER = int(os.environ.get('PART_NUMBER', 1))
INPUT_FILE = f"part_{PART_NUMBER:02d}.csv"
OUTPUT_DIR = "./results"
PROGRESS_FILE = os.path.join(OUTPUT_DIR, f"part_{PART_NUMBER:02d}_progress.txt")
EMPTY_RESPONSES_DIR = os.path.join(OUTPUT_DIR, "empty_responses")

REBUILD_PROGRESS_FROM_CSV = True
SAVE_EMPTY = True
PRINT_ALTERNATES = False           # Print when alternate queries are tried
PRINT_SEARCH_LOGS = False          # NEW: turn off per-request logging

ROW_INDEX_COLUMN = "row_index"
TITLE_COLUMN = "primaryTitle"
TCONST_COLUMN = "tconst"

MAX_VIDEOS_PER_TITLE = 20
MAX_CONCURRENT_REQUESTS = 5
BATCH_SIZE = 10
REQUEST_TIMEOUT = 45
SEMAPHORE_LIMIT = 5
CHUNK_SIZE = 50000

SONYLIV_SEARCH_URL = "https://apiv3.sonyliv.com/AGL/4.8/A/ENG/WEB/IN/TN/TRAY/SEARCH"
SONYLIV_PARAMS = {
    "app_version": "3.10.3",
    "tabs": 1,
    "kids_safe": "false",
    "from": 0,
    "to": 30,
}
SONYLIV_HEADERS = {
    "Origin": "https://www.sonyliv.com",
    "Referer": "https://www.sonyliv.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

keep_running = True

# ============================================================
# 1. HELPER FUNCTIONS (unchanged)
# ============================================================
def get_category_path(category_name: str) -> str:
    if not category_name:
        return "videos"
    cat = category_name.lower()
    if cat == "movies":
        return "movies"
    elif cat in ("shows", "web_series"):
        return "shows"
    elif cat == "sports":
        return "sports"
    else:
        return "videos"

def build_url(title: str, video_id: str, category_name: str) -> str:
    if not video_id:
        return None
    if title:
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', str(title).lower()).strip('-')
    else:
        slug = str(video_id)
    path = get_category_path(category_name)
    return f"https://www.sonyliv.com/{path}/{slug}-{video_id}"

def extract_assets_with_urls(data: Dict, limit: int = MAX_VIDEOS_PER_TITLE) -> List[Dict]:
    if "error" in data:
        return []
    assets = []
    try:
        containers = data.get("resultObj", {}).get("containers", [])
        for container_group in containers:
            for container in container_group.get("containers", []):
                for asset in container.get("assets", []):
                    title = (
                        asset.get("title") or
                        asset.get("metadata", {}).get("episodeTitle") or
                        asset.get("metadata", {}).get("title")
                    )
                    video_id = asset.get("id") or asset.get("contentId")
                    if not video_id:
                        continue
                    category_name = None
                    categories = asset.get("categories", [])
                    if categories:
                        category_name = categories[0].get("categoryName")
                    url = build_url(title, video_id, category_name)
                    assets.append({
                        "title": title,
                        "video_id": str(video_id),
                        "watch_url": url,
                        "category": category_name,
                        "imdb_id": asset.get("metadata", {}).get("emfAttributes", {}).get("ID_IMDB")
                    })
                    if len(assets) >= limit:
                        break
                if len(assets) >= limit:
                    break
            if len(assets) >= limit:
                break
    except Exception as e:
        print(f"⚠️ Error extracting assets: {e}")
    return assets

def save_empty_response(data: Dict, movie_name: str, row_index: int, attempt: int, query: str = None):
    if not SAVE_EMPTY:
        return
    os.makedirs(EMPTY_RESPONSES_DIR, exist_ok=True)
    sanitized_name = re.sub(r'[^\w\-_\. ]', '_', movie_name)[:50]
    if query and query != movie_name:
        sanitized_query = re.sub(r'[^\w\-_]', '_', query[:30])
        query_suffix = f"_query_{sanitized_query}"
    else:
        query_suffix = ""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    filename = f"row{row_index}_{sanitized_name}_attempt{attempt}{query_suffix}_{timestamp}.json"
    filepath = os.path.join(EMPTY_RESPONSES_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def generate_alternate_queries(original: str) -> List[str]:
    original = original.strip()
    if not original:
        return []
    alternates = set()
    alternates.add(original)

    # Remove year in parentheses
    cleaned = re.sub(r'\s*\(\d{4}\)', '', original).strip()
    if cleaned != original:
        alternates.add(cleaned)

    # Remove common prefixes
    for prefix in ["The ", "A ", "An "]:
        if cleaned.startswith(prefix):
            alt = cleaned[len(prefix):].strip()
            if alt and alt != cleaned:
                alternates.add(alt)

    # Drop words from the end (up to 3)
    tokens = cleaned.split()
    if len(tokens) > 2:
        for i in range(1, min(4, len(tokens))):
            alt = ' '.join(tokens[:-i])
            if alt and alt != cleaned:
                alternates.add(alt)

    # Drop words from the beginning (up to 2)
    if len(tokens) > 2:
        for i in range(1, min(3, len(tokens))):
            alt = ' '.join(tokens[i:])
            if alt and alt != cleaned:
                alternates.add(alt)

    # First word only
    if tokens and len(tokens[0]) > 2:
        alternates.add(tokens[0])

    # Remove special characters
    simple = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned).strip()
    if simple != cleaned:
        alternates.add(simple)

    result = [alt for alt in alternates if alt and len(alt) > 1]
    seen = set()
    ordered = []
    for alt in result:
        if alt not in seen:
            seen.add(alt)
            ordered.append(alt)
    return ordered[:100]

# ============================================================
# 2. ASYNC FUNCTIONS – WITH OPTIONAL LOGGING
# ============================================================
async def search_sonyliv_async(session, movie_name, semaphore) -> Dict:
    async with semaphore:
        if PRINT_SEARCH_LOGS:
            now = datetime.datetime.now().isoformat(timespec='seconds')
            print(f"[{now}] 🔍 Searching: '{movie_name[:40]}...'")
        params = SONYLIV_PARAMS.copy()
        params["query"] = movie_name
        try:
            async with session.get(
                SONYLIV_SEARCH_URL,
                params=params,
                headers=SONYLIV_HEADERS,
                timeout=REQUEST_TIMEOUT
            ) as response:
                if PRINT_SEARCH_LOGS:
                    print(f"   ← Status {response.status} for '{movie_name[:30]}'")
                if response.status != 200:
                    text = await response.text()
                    if PRINT_SEARCH_LOGS:
                        print(f"   ❌ Non-200 response: {text[:200]}")
                    return {"error": f"HTTP {response.status}: {text[:100]}"}
                data = await response.json()
                if "error" in data and PRINT_SEARCH_LOGS:
                    print(f"   ⚠️ API error: {data['error']}")
                return data
        except asyncio.TimeoutError:
            if PRINT_SEARCH_LOGS:
                print(f"   ⏰ TIMEOUT for '{movie_name[:30]}'")
            return {"error": "Request timed out"}
        except ClientError as e:
            if PRINT_SEARCH_LOGS:
                print(f"   ❌ Client error: {e}")
            return {"error": f"Client error: {str(e)}"}
        except Exception as e:
            if PRINT_SEARCH_LOGS:
                print(f"   ❌ Unexpected error: {e}")
            return {"error": f"Request failed: {str(e)}"}

async def process_movie_async(session, row_index, movie_name, tconst, semaphore) -> Dict:
    try:
        if not movie_name or pd.isna(movie_name):
            return {"row_index": row_index, "tconst": tconst, "movie_name": "", "is_present": False, "video_data": "[]"}
        movie_name = str(movie_name).strip()
        if not movie_name:
            return {"row_index": row_index, "tconst": tconst, "movie_name": "", "is_present": False, "video_data": "[]"}

        video_assets = []
        attempt = 0
        data = None

        attempt += 1
        data = await search_sonyliv_async(session, movie_name, semaphore)
        video_assets = extract_assets_with_urls(data)
        if not video_assets and SAVE_EMPTY:
            save_empty_response(data, movie_name, row_index, attempt, movie_name)

        MAX_SAME_NAME_RETRIES = 2
        while not video_assets and attempt <= MAX_SAME_NAME_RETRIES:
            attempt += 1
            await asyncio.sleep(2)
            data = await search_sonyliv_async(session, movie_name, semaphore)
            video_assets = extract_assets_with_urls(data)
            if not video_assets and SAVE_EMPTY:
                save_empty_response(data, movie_name, row_index, attempt, movie_name)

        if not video_assets:
            alternates = generate_alternate_queries(movie_name)
            alternates = [alt for alt in alternates if alt != movie_name]
            if alternates and PRINT_ALTERNATES:
                print(f"🔍 Trying alternates for '{movie_name[:40]}...' ({len(alternates)} alt(s))")
            for idx, alt in enumerate(alternates, 1):
                data = await search_sonyliv_async(session, alt, semaphore)
                video_assets = extract_assets_with_urls(data)
                if video_assets:
                    if PRINT_ALTERNATES:
                        print(f"   ✅ Alternate #{idx} ('{alt[:30]}') produced {len(video_assets)} results.")
                    break
                else:
                    if SAVE_EMPTY:
                        save_empty_response(data, movie_name, row_index, f"{attempt}_{idx}", alt)

        if not video_assets:
            return {
                "row_index": row_index,
                "tconst": tconst,
                "movie_name": movie_name,
                "is_present": False,
                "video_data": "[]"
            }
        else:
            titles = [asset.get("title", "") for asset in video_assets]
            is_present = any(movie_name.casefold() == str(t).casefold() for t in titles if t)
            video_data = json.dumps(video_assets, ensure_ascii=False)
            return {
                "row_index": row_index,
                "tconst": tconst,
                "movie_name": movie_name,
                "is_present": is_present,
                "video_data": video_data
            }
    except Exception as e:
        print(f"❌ Error processing row {row_index} ('{movie_name}'): {e}")
        return {"row_index": row_index, "tconst": tconst, "movie_name": movie_name, "is_present": False, "video_data": "[]"}

async def process_batch_async(session, batch, semaphore):
    tasks = [process_movie_async(session, idx, name, tconst, semaphore) for idx, tconst, name in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed_results = []
    for i, result in enumerate(results):
        idx, tconst, name = batch[i]
        if isinstance(result, Exception):
            processed_results.append({"row_index": idx, "tconst": tconst, "movie_name": name, "is_present": False, "video_data": "[]"})
        else:
            processed_results.append(result)
    return processed_results

# ============================================================
# 3. PROGRESS FILE MANAGEMENT (unchanged)
# ============================================================
def rebuild_progress_from_csv(csv_file: str, progress_file: str):
    if not os.path.exists(csv_file):
        print(f"ℹ️ No output CSV found – will start fresh.")
        return
    try:
        df_ids = pd.read_csv(csv_file, usecols=['id'])
        ids = df_ids['id'].tolist()
        if not ids:
            print("ℹ️ Output CSV is empty – progress file will be cleared.")
            with open(progress_file, 'w') as f:
                pass
            return
        with open(progress_file, 'w') as f:
            f.write('\n'.join(str(i) for i in ids))
        print(f"✅ Rebuilt progress file with {len(ids):,} successful rows from CSV.")
    except Exception as e:
        print(f"⚠️ Could not rebuild progress file: {e}")

def load_processed_indices(progress_file: str) -> Set[int]:
    processed = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        processed.add(int(line))
                    except ValueError:
                        pass
        print(f"✅ Loaded {len(processed):,} processed row indices from progress file")
    else:
        print(f"ℹ️ No progress file found – starting fresh")
    return processed

# ============================================================
# 4. MAIN PROCESSING FUNCTION
# ============================================================
async def process_part():
    global keep_running

    print("=" * 70)
    print(f"📊 SONY LIV PROCESSOR - GitHub Actions (No Cache) - Part {PART_NUMBER:02d}")
    print("=" * 70)
    print(f"📂 Input file: {INPUT_FILE}")
    print(f"📂 Output dir: {OUTPUT_DIR}")
    print(f"📂 Progress file: {PROGRESS_FILE}")
    print(f"🔁 Rebuild progress from CSV: {REBUILD_PROGRESS_FROM_CSV}")
    print(f"📂 Saving empty responses: {SAVE_EMPTY}")
    print(f"🔍 Print alternates: {PRINT_ALTERNATES}")
    print(f"📢 Search logs: {'ON' if PRINT_SEARCH_LOGS else 'OFF'}")
    print("=" * 70)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(INPUT_FILE):
        print(f"❌ ERROR: Input file '{INPUT_FILE}' does not exist.")
        sys.exit(1)

    output_file = os.path.join(OUTPUT_DIR, f"part_{PART_NUMBER:02d}_sonyliv_results.csv")

    if REBUILD_PROGRESS_FROM_CSV:
        rebuild_progress_from_csv(output_file, PROGRESS_FILE)

    processed_set = load_processed_indices(PROGRESS_FILE)

    print("\n📖 Reading input file metadata...")
    try:
        df_sample = pd.read_csv(INPUT_FILE, nrows=5)
        print(f"✅ Found columns: {', '.join(df_sample.columns.tolist())}")
        if ROW_INDEX_COLUMN not in df_sample.columns or TITLE_COLUMN not in df_sample.columns:
            print("❌ Required columns missing.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR reading input file: {e}")
        sys.exit(1)

    try:
        df_count = pd.read_csv(INPUT_FILE, usecols=[ROW_INDEX_COLUMN])
        total_rows = len(df_count)
        print(f"📊 Total rows in file: {total_rows:,}")
    except Exception as e:
        total_rows = 0

    remaining = total_rows - len(processed_set)
    print(f"📊 Already processed (in progress file): {len(processed_set):,}")
    print(f"📊 Remaining to process: {remaining:,}")
    if remaining <= 0:
        print("\n✅ All rows already processed! Nothing to do.")
        return

    output_exists = os.path.exists(output_file)
    mode = 'a' if output_exists else 'w'
    print(f"\n📝 Output file: {os.path.basename(output_file)}")

    # ---- ONE-TIME TEST REQUEST (always prints) ----
    print("\n🔬 Sending test request to SonyLIV API...")
    semaphore = asyncio.Semaphore(1)
    connector = TCPConnector(limit=1)
    timeout = ClientTimeout(total=REQUEST_TIMEOUT, connect=10)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as test_session:
        test_data = await search_sonyliv_async(test_session, "The Godfather", semaphore)
        if "error" in test_data and test_data["error"]:
            print(f"❌ Test request failed: {test_data['error']}")
            print("   ⚠️ The API may be unreachable or the endpoint/headers are incorrect.")
            print("   Exiting to avoid wasting time.")
            sys.exit(1)
        else:
            assets = extract_assets_with_urls(test_data)
            print(f"✅ Test request succeeded! Found {len(assets)} assets for 'The Godfather'.")
            print("   Continuing with full processing...")

    # ---- MAIN PROCESSING LOOP ----
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    connector = TCPConnector(limit=MAX_CONCURRENT_REQUESTS, limit_per_host=MAX_CONCURRENT_REQUESTS,
                             ttl_dns_cache=300, enable_cleanup_closed=True)
    timeout = ClientTimeout(total=REQUEST_TIMEOUT, connect=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        with open(output_file, mode, newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            if not output_exists:
                writer.writerow(['id', 'tconst', 'movie_name', 'is_present', 'video_data'])
                print("📝 Created new output file with header")
            else:
                print("📝 Appending to existing output file")

            batch = []
            start_time = time.time()
            processed_this_run = 0
            successful_this_run = 0

            pbar = tqdm_asyncio(total=remaining, desc=f"Part {PART_NUMBER:02d}", unit="rows")
            usecols = [ROW_INDEX_COLUMN, TITLE_COLUMN]
            if TCONST_COLUMN:
                usecols.append(TCONST_COLUMN)

            for chunk in pd.read_csv(INPUT_FILE, usecols=usecols, chunksize=CHUNK_SIZE,
                                     dtype={ROW_INDEX_COLUMN: str, TITLE_COLUMN: str}):
                if not keep_running:
                    print("🛑 Stopping...")
                    break
                for _, row in chunk.iterrows():
                    row_index = int(row[ROW_INDEX_COLUMN])
                    if row_index in processed_set:
                        continue
                    title = str(row[TITLE_COLUMN])
                    tconst = str(row[TCONST_COLUMN]) if TCONST_COLUMN and TCONST_COLUMN in row else ""
                    batch.append((row_index, tconst, title))
                    if len(batch) >= BATCH_SIZE:
                        results = await process_batch_async(session, batch, semaphore)
                        output_results = [r for r in results if r['video_data'] != "[]"]
                        if output_results:
                            writer.writerows([[r['row_index'], r['tconst'], r['movie_name'], r['is_present'], r['video_data']] for r in output_results])
                            csvfile.flush()
                            successful_indices = [r['row_index'] for r in output_results]
                            with open(PROGRESS_FILE, 'a') as pf:
                                pf.write('\n'.join(str(i) for i in successful_indices) + '\n')
                            processed_set.update(successful_indices)
                            successful_this_run += len(output_results)
                        processed_this_run += len(results)
                        pbar.update(len(results))
                        pbar.set_description(f"Part {PART_NUMBER:02d} (ok: {successful_this_run:,})")
                        batch.clear()
                # Flush remaining batch
                if batch:
                    results = await process_batch_async(session, batch, semaphore)
                    output_results = [r for r in results if r['video_data'] != "[]"]
                    if output_results:
                        writer.writerows([[r['row_index'], r['tconst'], r['movie_name'], r['is_present'], r['video_data']] for r in output_results])
                        csvfile.flush()
                        successful_indices = [r['row_index'] for r in output_results]
                        with open(PROGRESS_FILE, 'a') as pf:
                            pf.write('\n'.join(str(i) for i in successful_indices) + '\n')
                        processed_set.update(successful_indices)
                        successful_this_run += len(output_results)
                    processed_this_run += len(results)
                    pbar.update(len(results))
                    pbar.set_description(f"Part {PART_NUMBER:02d} (ok: {successful_this_run:,})")
                    batch.clear()

            pbar.close()
            elapsed = time.time() - start_time
            print("\n" + "="*60)
            print(f"✅ FINISHED PROCESSING Part {PART_NUMBER:02d}")
            print(f"📊 Processed this run: {processed_this_run:,} rows")
            print(f"📊 Successful (with video data): {successful_this_run:,}")
            print(f"📊 Total successful rows (cumulative): {len(processed_set):,}")
            if processed_this_run > 0:
                print(f"🚀 Average rate: {processed_this_run/elapsed:.2f} rows/sec")
                print(f"⏱️ Time: {elapsed:.2f} seconds")
            print("="*60)

    try:
        total_rows_in_csv = 0
        for chunk in pd.read_csv(output_file, chunksize=10000):
            total_rows_in_csv += len(chunk)
        print(f"\n📊 Final CSV row count (using chunked read): {total_rows_in_csv:,} rows")
    except Exception as e:
        print(f"⚠️ Could not read final CSV: {e}")

# ============================================================
# 5. SIGNAL HANDLER & MAIN
# ============================================================
def signal_handler(sig, frame):
    global keep_running
    print("\n🛑 Interrupt received. Finishing batch...")
    keep_running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    print("=" * 60)
    print(f"🍿 SONY LIV PROCESSOR - GitHub Actions (No Cache) - Part {PART_NUMBER:02d}")
    print("=" * 60)
    print(f"📁 Input file: {INPUT_FILE}")
    print(f"📁 Output dir: {OUTPUT_DIR}")
    print(f"📁 Progress file: {PROGRESS_FILE}")
    print(f"⚡ Concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print("=" * 60)

    asyncio.run(process_part())

    print("\n" + "=" * 60)
    print("🎉 PROCESSING COMPLETE!")
    print("=" * 60)
    print(f"📁 Results saved in: {OUTPUT_DIR}")
    print(f"📄 Output file: part_{PART_NUMBER:02d}_sonyliv_results.csv")
    print(f"📄 Progress tracked in: part_{PART_NUMBER:02d}_progress.txt")
    print("=" * 60)
