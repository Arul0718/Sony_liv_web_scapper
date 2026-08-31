#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SONY LIV PROCESSOR – GitHub Actions Version
"""

import os
import sys
import pandas as pd
import time
import asyncio
import aiohttp
import csv
import json
import re
import nest_asyncio
nest_asyncio.apply()

# ============================================================
# 1. CONFIGURATION
# ============================================================
PART_NUMBER = int(os.environ.get('PART_NUMBER', 1))
INPUT_FILE = f"part_{PART_NUMBER:02d}.csv"
OUTPUT_DIR = "./results"
PROGRESS_FILE = os.path.join(OUTPUT_DIR, f"part_{PART_NUMBER:02d}_progress.txt")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"part_{PART_NUMBER:02d}_sonyliv_results.csv")

# Column names (these are exactly as in your CSV)
ROW_INDEX_COLUMN = "row_index"
TITLE_COLUMN = "primaryTitle"
TCONST_COLUMN = "tconst"

# Processing settings
MAX_VIDEOS_PER_TITLE = 10
MAX_CONCURRENT_REQUESTS = 20
BATCH_SIZE = 100
REQUEST_TIMEOUT = 45
SEMAPHORE_LIMIT = 20
CHUNK_SIZE = 50000

# ---------- SonyLIV API ----------
SONYLIV_SEARCH_URL = "https://apiv3.sonyliv.com/AGL/4.8/A/ENG/WEB/IN/TN/TRAY/SEARCH"
SONYLIV_PARAMS = {
    "app_version": "3.10.3",
    "tabs": 1,
    "kids_safe": "false",
    "from": 0,
    "to": 13,
}
SONYLIV_HEADERS = {
    "Origin": "https://www.sonyliv.com",
    "Referer": "https://www.sonyliv.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Accept": "application/json",
}

# ============================================================
# 2. GLOBALS
# ============================================================
_search_cache = {}
_cache_lock = asyncio.Lock()
CACHE_SIZE = 20000
keep_running = True

# ============================================================
# 3. HELPER FUNCTIONS (unchanged)
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
                        "category": category_name
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

def generate_alternate_queries(original: str) -> List[str]:
    original = original.strip()
    if not original:
        return []
    tokens = original.split()
    ordered = []
    MIN_LENGTH = 1
    if len(tokens) > 1:
        for i in range(1, len(tokens)):
            alt = ' '.join(tokens[:len(tokens) - i])
            if alt and alt != original and alt not in ordered:
                ordered.append(alt)
        single_word = tokens[0]
        if len(single_word) > MIN_LENGTH:
            for j in range(1, len(single_word) - MIN_LENGTH + 1):
                alt = single_word[:-j]
                if alt and alt != original and alt not in ordered:
                    ordered.append(alt)
    else:
        single_word = tokens[0]
        if len(single_word) > MIN_LENGTH:
            for i in range(1, len(single_word) - MIN_LENGTH + 1):
                alt = single_word[:-i]
                if alt and alt != original and alt not in ordered:
                    ordered.append(alt)
    return ordered[:100]

async def search_sonyliv_async(session, movie_name, semaphore, use_cache=True):
    if use_cache:
        async with _cache_lock:
            if movie_name in _search_cache:
                return _search_cache[movie_name]
    async with semaphore:
        params = SONYLIV_PARAMS.copy()
        params["query"] = movie_name
        try:
            async with session.get(SONYLIV_SEARCH_URL, params=params, headers=SONYLIV_HEADERS,
                                   timeout=ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if "error" in data:
                    print(f"⚠️ SonyLIV error for '{movie_name[:30]}...': {data['error']}")
                if use_cache:
                    async with _cache_lock:
                        if len(_search_cache) < CACHE_SIZE:
                            _search_cache[movie_name] = data
                return data
        except Exception as e:
            error_data = {"error": f"Request failed: {str(e)}"}
            if use_cache:
                async with _cache_lock:
                    if len(_search_cache) < CACHE_SIZE:
                        _search_cache[movie_name] = error_data
            return error_data

async def process_movie_async(session, row_index, movie_name, tconst, semaphore):
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
        data = await search_sonyliv_async(session, movie_name, semaphore, use_cache=True)
        video_assets = extract_assets_with_urls(data)
        MAX_SAME_NAME_RETRIES = 2
        while not video_assets and attempt <= MAX_SAME_NAME_RETRIES:
            attempt += 1
            await asyncio.sleep(2)
            data = await search_sonyliv_async(session, movie_name, semaphore, use_cache=False)
            video_assets = extract_assets_with_urls(data)
        if not video_assets:
            alternates = generate_alternate_queries(movie_name)
            for alt in alternates:
                data = await search_sonyliv_async(session, alt, semaphore, use_cache=False)
                video_assets = extract_assets_with_urls(data)
                if video_assets:
                    break
        if not video_assets:
            return {"row_index": row_index, "tconst": tconst, "movie_name": movie_name, "is_present": False, "video_data": "[]"}
        else:
            titles = [asset.get("title", "") for asset in video_assets]
            is_present = any(movie_name.casefold() == str(t).casefold() for t in titles if t)
            video_data = json.dumps(video_assets, ensure_ascii=False)
            return {"row_index": row_index, "tconst": tconst, "movie_name": movie_name, "is_present": is_present, "video_data": video_data}
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
            processed_results.append({"row_index": idx, "tconst": tconst, "movie_name": result['movie_name'], "is_present": result['is_present'], "video_data": result['video_data']})
    return processed_results

def load_processed_indices(progress_file):
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
        print(f"✅ Loaded {len(processed):,} processed row indices")
    return processed

# ============================================================
# 4. MAIN PROCESSING FUNCTION (with encoding fix)
# ============================================================
async def process_part():
    print("="*70)
    print(f"📊 GitHub Actions - Processing Part {PART_NUMBER:02d}")
    print("="*70)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(INPUT_FILE):
        print(f"❌ ERROR: Input file '{INPUT_FILE}' not found!")
        sys.exit(1)

    processed_set = load_processed_indices(PROGRESS_FILE)

    # Read a sample to check columns – use utf-8-sig to handle BOM
    try:
        df_sample = pd.read_csv(INPUT_FILE, nrows=5, encoding='utf-8-sig')
        print("📋 Actual columns found:", df_sample.columns.tolist())
        
        # Check if the required columns exist (strip any leading/trailing whitespace)
        actual_cols = [col.strip() for col in df_sample.columns]
        if ROW_INDEX_COLUMN not in actual_cols or TITLE_COLUMN not in actual_cols:
            print("❌ Required columns missing.")
            print(f"   Expected: {ROW_INDEX_COLUMN}, {TITLE_COLUMN}, {TCONST_COLUMN}")
            print(f"   Found: {df_sample.columns.tolist()}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)

    # Count total rows
    try:
        df_count = pd.read_csv(INPUT_FILE, usecols=[ROW_INDEX_COLUMN], encoding='utf-8-sig')
        total_rows = len(df_count)
        print(f"📊 Total rows: {total_rows:,}")
    except Exception as e:
        print(f"⚠️ Could not count rows: {e}")
        total_rows = 0

    remaining = total_rows - len(processed_set)
    print(f"📊 Already processed: {len(processed_set):,} – Remaining: {remaining:,}")
    if remaining <= 0:
        print("✅ All rows already processed!")
        return

    output_exists = os.path.exists(OUTPUT_FILE)
    mode = 'a' if output_exists else 'w'

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    connector = TCPConnector(limit=MAX_CONCURRENT_REQUESTS, limit_per_host=MAX_CONCURRENT_REQUESTS,
                             ttl_dns_cache=300, enable_cleanup_closed=True)
    timeout = ClientTimeout(total=REQUEST_TIMEOUT, connect=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        with open(OUTPUT_FILE, mode, newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            if not output_exists:
                writer.writerow(['id', 'tconst', 'movie_name', 'is_present', 'video_data'])
                print("📝 Created new output file")

            batch = []
            start_time = time.time()
            processed_this_run = 0
            successful_this_run = 0

            pbar = tqdm_asyncio(total=remaining, desc=f"Part {PART_NUMBER:02d}", unit="rows")
            usecols = [ROW_INDEX_COLUMN, TITLE_COLUMN]
            if TCONST_COLUMN:
                usecols.append(TCONST_COLUMN)

            for chunk in pd.read_csv(INPUT_FILE, usecols=usecols, chunksize=CHUNK_SIZE,
                                     dtype={ROW_INDEX_COLUMN: str, TITLE_COLUMN: str},
                                     encoding='utf-8-sig'):
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
                        batch.clear()
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
                    batch.clear()

            pbar.close()
            elapsed = time.time() - start_time
            print(f"\n✅ FINISHED Part {PART_NUMBER:02d} – Processed: {processed_this_run:,} rows, Success: {successful_this_run:,}, Total with data: {len(processed_set):,}, Time: {elapsed:.2f}s")

if __name__ == "__main__":
    asyncio.run(process_part())
