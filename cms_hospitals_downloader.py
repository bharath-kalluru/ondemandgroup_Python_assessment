#!/usr/bin/env python3
"""
cms_hospitals_downloader.py

- Lists CMS metastore datasets (https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items)
- Filters datasets whose metadata mentions theme "Hospitals"
- For each matching dataset, finds CSV distributions (or files)
- Downloads CSVs in parallel (async), but only when the file has been modified
  since the previous run (tracks metadata/ETags/last_modified per file in metadata.json)
- Converts CSV column headers to snake_case
- Saves processed CSVs under ./data/<dataset_id>/<filename>.csv
- Logs actions and writes a sample output CSV for inspection.

Run:
    pip install -r requirements.txt
    python cms_hospitals_downloader.py

Notes:
 - Designed to run on regular Windows/Linux machines (no cloud-specific libs)
 - Uses aiohttp for parallel downloads and pandas for header processing & CSV IO
"""

import asyncio
import aiohttp
import async_timeout
import os
import re
import json
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

# CONFIGURATION
CMS_LISTING_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
OUTPUT_DIR = Path("data")
METADATA_FILE = Path("metadata.json")
CONCURRENT_DOWNLOADS = 8
REQUEST_TIMEOUT = 30  # seconds
USER_AGENT = "cms-hospitals-downloader/1.0 (+https://example.local/)"

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_metadata() -> Dict[str, Any]:
    if METADATA_FILE.exists():
        try:
            return json.loads(METADATA_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            logging.warning("Failed to read metadata.json, starting fresh: %s", e)
            return {}
    return {}


def save_metadata(meta: Dict[str, Any]):
    METADATA_FILE.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def to_snake_case(s: str) -> str:
    """Convert arbitrary header to snake_case (removes special chars, collapse spaces, lower)."""
    if s is None:
        return ""
    # replace special characters and punctuation with space
    s = re.sub(r"[^\w\s]", " ", s)
    # collapse whitespace, strip
    s = re.sub(r"\s+", " ", s).strip()
    # convert camelCase or PascalCase to space-separated (insert spaces before caps)
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r" \1", s)
    parts = s.split(" ")
    # sanitize each part and join with _
    parts = [re.sub(r"[_\s]+", "_", p).lower() for p in parts if p != ""]
    return "_".join(parts)


def headers_to_snakecase_df(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {col: to_snake_case(col) for col in df.columns}
    df = df.rename(columns=new_cols)
    return df


async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict] = None) -> Any:
    headers = {"User-Agent": USER_AGENT}
    async with async_timeout.timeout(REQUEST_TIMEOUT):
        async with session.get(url, params=params, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.json()


def extract_possible_theme_fields(item: dict) -> List[str]:
    """
    The metastore schema may have different keys (theme, themes, topics, tags etc).
    This tries common fields and returns a list of theme-like strings.
    """
    theme_values = []
    for k in ("theme", "themes", "topic", "topics", "tags", "keywords"):
        v = item.get(k)
        if not v:
            # some fields may be nested under 'metadata'
            if "metadata" in item and isinstance(item["metadata"], dict):
                v = item["metadata"].get(k)
        if not v:
            continue
        # normalize type
        if isinstance(v, str):
            theme_values.append(v)
        elif isinstance(v, (list, tuple)):
            for e in v:
                if isinstance(e, str):
                    theme_values.append(e)
        elif isinstance(v, dict):
            for sub in v.values():
                if isinstance(sub, str):
                    theme_values.append(sub)
    return theme_values


def matches_hospitals_theme(item: dict) -> bool:
    """Return True if the dataset item appears to reference 'Hospitals' theme."""
    theme_candidates = extract_possible_theme_fields(item)
    for t in theme_candidates:
        if "hospital" in t.lower():  # substring match; e.g., "Hospitals", "hospital_data"
            return True
    # fallback: check title or description
    for k in ("title", "name", "description", "abstract"):
        v = item.get(k) or (item.get("metadata", {}) or {}).get(k)
        if isinstance(v, str) and "hospital" in v.lower():
            return True
    return False


def find_csv_distributions(item: dict) -> List[Dict[str, Any]]:
    """
    Many metastore datasets have 'distributions' or 'assets' listing files.
    We try common shapes and return a normalized list with keys:
      - url
      - filename
      - last_modified (if available)
      - etag (if available)
    """
    distributions = []
    # Common locations
    for key in ("distributions", "assets", "resources", "files", "distribution"):
        maybe = item.get(key) or (item.get("metadata", {}) or {}).get(key)
        if not maybe:
            continue
        if isinstance(maybe, dict):
            maybe = [maybe]
        for entry in maybe:
            if not isinstance(entry, dict):
                continue
            url = entry.get("downloadURL") or entry.get("url") or entry.get("accessURL") or entry.get("endpoint")
            fname = entry.get("filename") or entry.get("title") or None
            lm = entry.get("lastModified") or entry.get("modified") or entry.get("last_updated") or entry.get("updated")
            etag = entry.get("etag")
            if url and (url.lower().endswith(".csv") or ".csv?" in url.lower()):
                distributions.append({"url": url, "filename": fname, "last_modified": lm, "etag": etag})
    # Some items include a top-level 'distribution' with formats
    # If none found, attempt to look for links in item['landingPage'] or item['downloadURL']
    if not distributions:
        # try top-level fields
        for candidate in ("downloadURL", "url", "link", "accessURL", "landingPage"):
            url = item.get(candidate)
            if isinstance(url, str) and (url.lower().endswith(".csv") or ".csv?" in url.lower()):
                distributions.append({"url": url, "filename": None, "last_modified": None, "etag": None})
    return distributions


async def head_info(session: aiohttp.ClientSession, url: str) -> Dict[str, Optional[str]]:
    """Make a HEAD request to get headers (ETag, Last-Modified). If HEAD not allowed, fallback to GET with range=0-0."""
    headers = {"User-Agent": USER_AGENT}
    try:
        async with async_timeout.timeout(REQUEST_TIMEOUT):
            async with session.head(url, headers=headers) as resp:
                # If HEAD succeeds, extract headers
                lm = resp.headers.get("Last-Modified") or resp.headers.get("last-modified")
                etag = resp.headers.get("ETag") or resp.headers.get("etag")
                return {"last_modified": lm, "etag": etag}
    except Exception:
        # fallback attempt: small GET
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, headers=headers, params={"$limit": 1}) as resp:
                    lm = resp.headers.get("Last-Modified") or resp.headers.get("last-modified")
                    etag = resp.headers.get("ETag") or resp.headers.get("etag")
                    return {"last_modified": lm, "etag": etag}
        except Exception as e:
            logging.debug("HEAD fallback failed for %s: %s", url, e)
            return {"last_modified": None, "etag": None}


async def download_csv(session: aiohttp.ClientSession, url: str, dest_path: Path) -> None:
    """Download CSV from url to dest_path, streaming to disk."""
    headers = {"User-Agent": USER_AGENT}
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest_path.with_suffix(".tmp")
    try:
        async with async_timeout.timeout(REQUEST_TIMEOUT * 4):
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()
                with tmp.open("wb") as f:
                    async for chunk in resp.content.iter_chunked(1024 * 64):
                        if chunk:
                            f.write(chunk)
        tmp.replace(dest_path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


async def process_distribution(session: aiohttp.ClientSession, dataset_id: str, dist: dict, metadata: dict, semaphore: asyncio.Semaphore):
    """
    For a single distribution:
     - Check HEAD to get ETag/Last-Modified
     - Compare with saved metadata and skip if unchanged
     - Download CSV
     - Convert headers to snake_case using pandas
     - Store file and update metadata
    """
    url = dist["url"]
    filename = dist.get("filename") or Path(url).name.split("?")[0]
    dataset_dir = OUTPUT_DIR / dataset_id
    dest_path = dataset_dir / filename

    async with semaphore:
        logging.info("Checking headers for %s", url)
        info = await head_info(session, url)
        key = f"{dataset_id}::{url}"
        prev = metadata.get(key, {})
        prev_etag = prev.get("etag")
        prev_lm = prev.get("last_modified")
        remote_etag = info.get("etag")
        remote_lm = info.get("last_modified")

        # If we have an etag and it matches, skip
        if remote_etag and prev_etag and remote_etag == prev_etag:
            logging.info("Skipping download (etag unchanged): %s", filename)
            return

        # If last-modified available, try to compare datetime strings
        if remote_lm and prev_lm and remote_lm == prev_lm:
            logging.info("Skipping download (last-modified unchanged): %s", filename)
            return

        # Otherwise download
        try:
            logging.info("Downloading: %s -> %s", url, dest_path)
            await download_csv(session, url, dest_path)
            logging.info("Downloaded: %s", dest_path)

            # Use pandas to re-read and normalize headers
            try:
                df = pd.read_csv(dest_path, dtype=str, low_memory=False)
            except Exception as e:
                logging.warning("pandas failed to read %s: %s (attempting with encoding utf-8)", dest_path, e)
                df = pd.read_csv(dest_path, dtype=str, low_memory=False, encoding="utf-8", errors="replace")

            df = headers_to_snakecase_df(df)
            # Overwrite file with normalized headers
            df.to_csv(dest_path, index=False)
            logging.info("Processed headers to snake_case for %s (%d rows, %d cols)", dest_path.name, len(df), len(df.columns))

            # Update metadata
            metadata[key] = {
                "url": url,
                "filename": filename,
                "last_modified": remote_lm,
                "etag": remote_etag,
                "downloaded_at": datetime.utcnow().isoformat() + "Z",
                "rows": len(df),
                "cols": len(df.columns),
            }
        except Exception as e:
            logging.error("Failed processing distribution %s: %s", url, e)


async def main_async():
    ensure_dirs()
    metadata = load_metadata()

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    conn = aiohttp.TCPConnector(limit_per_host=CONCURRENT_DOWNLOADS, ssl=False)

    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        logging.info("Fetching CMS metastore listing...")
        try:
            items = await fetch_json(session, CMS_LISTING_URL)
        except Exception as e:
            logging.error("Failed to fetch dataset listing: %s", e)
            return

        if not isinstance(items, list):
            # some endpoints return dict with 'data' or 'items'
            if isinstance(items, dict):
                items = items.get("data") or items.get("items") or items.get("results") or []
            if not isinstance(items, list):
                logging.error("Unexpected listing format. Exiting.")
                return

        # Filter items matching hospitals theme
        matching = []
        for it in items:
            try:
                if matches_hospitals_theme(it):
                    matching.append(it)
            except Exception as e:
                logging.debug("Error checking theme on item: %s", e)

        logging.info("Found %d datasets matching 'Hospitals' theme", len(matching))

        # For each matching dataset, find CSV distributions
        tasks = []
        semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
        for ds in matching:
            # identify dataset id
            ds_id = ds.get("id") or ds.get("dataset_id") or ds.get("identifier") or ds.get("name") or ds.get("title") or "unknown_dataset"
            # sanitize ds_id for filesystem
            ds_id = re.sub(r"[^\w\-\.]", "_", str(ds_id))
            dists = find_csv_distributions(ds)
            if not dists:
                logging.info("No CSV distributions found for dataset %s", ds_id)
                continue
            for dist in dists:
                tasks.append(process_distribution(session, ds_id, dist, metadata, semaphore))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            logging.info("No distributions to download.")

    # Save metadata
    save_metadata(metadata)

    # Print a short sample output summary
    logging.info("Run complete. Saved metadata to %s", METADATA_FILE)
    # show a few metadata entries as sample
    sample_keys = list(metadata.keys())[:10]
    if sample_keys:
        logging.info("Sample of downloaded files:")
        for k in sample_keys:
            v = metadata[k]
            logging.info("  - %s -> %s (rows=%s, cols=%s, last_modified=%s)", k, v.get("filename"), v.get("rows"), v.get("cols"), v.get("last_modified"))


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()