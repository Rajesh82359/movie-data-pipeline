#!/usr/bin/env python3
"""
etl.py - MovieLens -> MySQL ETL with optional OMDb enrichment

Drop-in replacement for your script. Key fixes/additions:
- Correct `insert_rating_query` (uses upsert to overwrite bad zero-dates)
- Robust chunked ratings import that ALWAYS sends MySQL-friendly DATETIME strings (or NULL)
- Optional --recreate-ratings flag that renames an existing ratings table to a backup and creates a fresh one
- Keeps your movie enrichment logic intact
"""
import os
import re
import json
import time
import logging
import argparse
import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timezone
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

OMDB_DAILY_LIMIT = 1000
OMDB_CALL_COUNT = 0

# load .env (if present)
load_dotenv()

# --- Database Credentials (from .env preferred) ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")
MYSQL_DB = os.getenv("MYSQL_DB", "movie_db")

# --- OMDb config ---
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_API_URL = os.getenv("OMDB_API_URL", "http://www.omdbapi.com/")
OMDB_CACHE_FILE = "omdb_cache.json"
OMDB_SLEEP = float(os.getenv("OMDB_SLEEP", "0.25"))

# Global flag set when OMDb reports rate limit for this run
RATE_LIMITED = False

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- HTTP session with retries (kept conservative) ---
session = requests.Session()
retries = Retry(total=2, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))


def load_cache():
    if os.path.exists(OMDB_CACHE_FILE):
        try:
            with open(OMDB_CACHE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            logging.warning("Failed to read omdb cache; starting fresh.")
    return {}


def save_cache(cache):
    try:
        with open(OMDB_CACHE_FILE, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error("Failed to save cache: %s", e)


def extract_year_from_title(title):
    if not isinstance(title, str):
        return None
    m = re.search(r"\((\d{4})\)", title)
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None


def clean_title_for_query(title):
    if not isinstance(title, str):
        return ""
    return re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()


def query_omdb(title, year=None):
    """
    Safe OMDb caller with request limit protection.
    Stops calling OMDb after OMDB_DAILY_LIMIT requests.
    """
    global OMDB_CALL_COUNT, RATE_LIMITED

    if OMDB_CALL_COUNT >= OMDB_DAILY_LIMIT:
        RATE_LIMITED = True
        return None

    if not OMDB_API_KEY:
        logging.debug("OMDB_API_KEY not set; skipping enrichment.")
        return None

    params = {"apikey": OMDB_API_KEY, "t": title, "plot": "short", "r": "json"}
    if year:
        params["y"] = str(year)

    try:
        resp = session.get(OMDB_API_URL, params=params, timeout=6)
        OMDB_CALL_COUNT += 1
        if resp.status_code == 401:
            logging.warning("OMDb 401 Unauthorized — API key limit reached.")
            RATE_LIMITED = True
            return None
        data = resp.json()
        if data.get("Response") == "True":
            return data

        # fallback to search
        search_params = {"apikey": OMDB_API_KEY, "s": title, "type": "movie", "r": "json"}
        if year:
            search_params["y"] = str(year)
        sresp = session.get(OMDB_API_URL, params=search_params, timeout=6)
        OMDB_CALL_COUNT += 1
        sdata = sresp.json()
        if sdata.get("Response") == "True" and "Search" in sdata:
            imdb_id = sdata["Search"][0].get("imdbID")
            iresp = session.get(OMDB_API_URL, params={"apikey": OMDB_API_KEY, "i": imdb_id, "plot": "short", "r": "json"}, timeout=6)
            OMDB_CALL_COUNT += 1
            idata = iresp.json()
            if idata.get("Response") == "True":
                return idata
        return None

    except Exception as e:
        logging.warning(f"OMDb request failed: {e}")
        return None


def enrich_movie(title, cache):
    """
    Returns enrichment dict or None. Caches every lookup (including misses).
    """
    year = extract_year_from_title(title)
    clean_title = clean_title_for_query(title)
    key = f"{clean_title}__{year if year else ''}"

    if key in cache:
        logging.debug("Cache hit for %s", key)
        return cache[key]

    if not OMDB_API_KEY:
        cache[key] = None
        return None

    data = None
    if year:
        data = query_omdb(clean_title, year=year)
    if data is None:
        data = query_omdb(clean_title)
    if data is None:
        alt = re.split(r"[:\-–]", clean_title)[0].strip()
        if alt and alt != clean_title:
            data = query_omdb(alt, year=year) or query_omdb(alt)

    enriched = None
    if data:
        enriched = {
            "director": None if data.get("Director") in (None, "N/A") else data.get("Director"),
            "plot": None if data.get("Plot") in (None, "N/A") else data.get("Plot"),
            "box_office": None if data.get("BoxOffice") in (None, "N/A") else data.get("BoxOffice"),
            "year": int(data.get("Year")) if data.get("Year") and str(data.get("Year")).isdigit() else year,
            "imdb_id": data.get("imdbID"),
            "raw": data,
            "enriched_at": datetime.now(timezone.utc).isoformat()
        }
        logging.debug("Director found: title='%s' director='%s'", clean_title, enriched.get("director"))
    else:
        logging.debug("OMDb not found for '%s'", clean_title)

    cache[key] = enriched
    time.sleep(OMDB_SLEEP)
    return enriched


def recreate_ratings_table_if_requested(cursor, conn):
    """
    If --recreate-ratings was provided, rename existing ratings table to ratings_bad_<ts> if it exists,
    and create a fresh ratings table.
    """
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    try:
        cursor.execute("SHOW TABLES LIKE 'ratings'")
        if cursor.fetchone():
            try:
                rename_sql = f"RENAME TABLE ratings TO ratings_bad_{ts}"
                cursor.execute(rename_sql)
                logging.info("Renamed existing ratings -> ratings_bad_%s", ts)
            except Exception as e:
                logging.warning("Could not rename existing ratings table: %s", e)
        create_sql = """
        CREATE TABLE IF NOT EXISTS ratings (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          userId INT NOT NULL,
          movieId INT NOT NULL,
          rating DECIMAL(3,2) NOT NULL,
          `timestamp` DATETIME NULL,
          UNIQUE KEY uq_user_movie (userId, movieId),
          INDEX idx_user (userId),
          INDEX idx_movie (movieId)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_sql)
        conn.commit()
        logging.info("Created fresh ratings table (if not exists).")
    except Exception as e:
        logging.error("Failed to recreate ratings table: %s", e)
        raise


def main(limit=None, no_enrich=False, recreate_ratings=False):
    logging.info("Starting ETL process...")
    # --- Connect to MySQL ---
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            autocommit=False,
            charset="utf8mb4",
        )
        cursor = conn.cursor()
        logging.info("Connected to MySQL at %s", MYSQL_HOST)
    except mysql.connector.Error as err:
        logging.error("Failed to connect to DB: %s", err)
        return

    # Optionally recreate ratings table safely
    if recreate_ratings:
        recreate_ratings_table_if_requested(cursor, conn)

    # --- Load movies.csv ---
    try:
        movies_df = pd.read_csv("movies.csv")
        logging.info("Loaded movies.csv (%d rows)", len(movies_df))
    except FileNotFoundError:
        logging.error("movies.csv not found in current folder. Exiting.")
        try:
            cursor.close()
            conn.close()
        except:
            pass
        return

    # --- Prepare ratings iterator existence check ---
    try:
        # We will not consume this iterator here; ratings load later uses its own pd.read_csv(..., chunksize=...)
        _ = pd.read_csv("ratings.csv", nrows=1)
        ratings_present = True
        logging.info("ratings.csv found and ready for processing")
    except FileNotFoundError:
        logging.warning("ratings.csv not found; continuing only with movies")
        ratings_present = False

    # prepare SQL statements
    insert_movie_query = """
    INSERT INTO movies (movieId, title, genres)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
      title = VALUES(title),
      genres = VALUES(genres)
    """

    update_enrichment_query = """
    UPDATE movies SET director=%s, plot=%s, box_office=%s, year=%s, imdb_id=%s, last_enriched_at=%s
    WHERE movieId=%s
    """

    # UPSERT query for ratings: updates rating and timestamp when duplicate exists
    insert_rating_query = """
    INSERT INTO ratings (userId, movieId, rating, `timestamp`)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      rating = VALUES(rating),
      `timestamp` = IF(VALUES(`timestamp`) IS NOT NULL, VALUES(`timestamp`), `timestamp`)
    """

    # load cache
    cache = load_cache()

    total = 0
    enriched_count = 0

    # iterate movies with graceful interrupt and rate-limit handling
    try:
        for _, row in movies_df.iterrows():
            if RATE_LIMITED:
                logging.warning("Stopping enrichment: OMDb rate limit reached earlier in this run.")
                break

            if limit is not None and total >= limit:
                break

            try:
                movieId = int(row["movieId"])
            except Exception:
                continue
            title = row.get("title", "")
            genres = row.get("genres", None)

            # insert/update basic movie row (idempotent)
            try:
                cursor.execute(insert_movie_query, (movieId, title, genres))
            except mysql.connector.Error as e:
                logging.error("Failed to insert/update movie %s (%s): %s", movieId, title, e)

            # enrichment
            enriched = None
            if not no_enrich:
                try:
                    enriched = enrich_movie(title, cache)
                except Exception as e:
                    logging.error("Error during enrichment for '%s': %s", title, e)
                    enriched = None

            if enriched:
                try:
                    cursor.execute(
                        update_enrichment_query,
                        (
                            enriched.get("director"),
                            enriched.get("plot"),
                            enriched.get("box_office"),
                            enriched.get("year"),
                            enriched.get("imdb_id"),
                            enriched.get("enriched_at"),
                            movieId,
                        )
                    )
                    enriched_count += 1
                    if enriched_count % 50 == 0:
                        try:
                            conn.commit()
                            logging.info("Committed after %d enrichments", enriched_count)
                        except Exception as commit_err:
                            logging.warning("Periodic commit failed: %s", commit_err)
                except mysql.connector.Error as e:
                    logging.warning("Could not write enrichment to DB for movieId %s (columns may be missing): %s", movieId, e)

            total += 1
            if total % 100 == 0:
                save_cache(cache)
                logging.info("Processed %d movies (enriched so far: %d)", total, enriched_count)

    except KeyboardInterrupt:
        logging.warning("Interrupted by user (KeyboardInterrupt). Saving cache and committing DB before exit...")
        save_cache(cache)
        try:
            conn.commit()
            logging.info("Partial DB changes committed.")
        except Exception as e:
            logging.error("Failed to commit on interrupt: %s", e)
        finally:
            try:
                cursor.close()
                conn.close()
            except:
                pass
        return

    # final cache save after movie loop
    save_cache(cache)
    logging.info("Finished processing movies. Total processed: %d, enriched: %d", total, enriched_count)

    # --- Load ratings (chunked, format timestamps, upsert) ---
    if ratings_present:
        total_inserted = 0
        chunk_count = 0
        for chunk in pd.read_csv("ratings.csv", chunksize=250000):
            chunk_count += 1
            logging.info("Processing ratings chunk %d", chunk_count)

            # Convert epoch seconds -> pandas datetime; invalid -> NaT
            if "timestamp" in chunk.columns:
                chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], unit="s", errors="coerce")
            else:
                chunk["timestamp"] = pd.NaT

            ratings_data = []
            bad_rows = 0
            for _, r in chunk.iterrows():
                try:
                    uid = int(r["userId"])
                    mid = int(r["movieId"])
                    rating = float(r["rating"])
                    ts = r.get("timestamp")
                    if pd.isna(ts):
                        ts_val = None
                    else:
                        # explicit MySQL DATETIME string (YYYY-MM-DD HH:MM:SS)
                        ts_val = ts.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
                    ratings_data.append((uid, mid, rating, ts_val))
                except Exception:
                    bad_rows += 1
                    continue

            if not ratings_data:
                logging.info("Chunk %d: no valid rows (bad_rows=%d)", chunk_count, bad_rows)
                continue

            # debug example tuple (comment/uncomment as needed)
            # logging.debug("Example tuple: %s", ratings_data[0])

            try:
                cursor.executemany(insert_rating_query, ratings_data)
                conn.commit()
                total_inserted += len(ratings_data)
                logging.info("Inserted/Upserted chunk %d: rows=%d  bad_rows=%d  total=%d",
                             chunk_count, len(ratings_data), bad_rows, total_inserted)
            except mysql.connector.Error as e:
                logging.error("Failed to insert ratings chunk %d: %s", chunk_count, e)
                try:
                    conn.rollback()
                except:
                    pass

    else:
        logging.info("ratings.csv not provided; skipping ratings import.")

    # commit and close
    try:
        conn.commit()
        logging.info("Database changes committed")
    except mysql.connector.Error as e:
        logging.error("Commit failed: %s", e)
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass
        logging.info("DB connection closed. ETL finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL: MovieLens -> MySQL with optional OMDb enrichment")
    parser.add_argument("--limit", type=int, default=None, help="Only process up to N movie rows (useful for testing)")
    parser.add_argument("--no-enrich", action="store_true", help="Skip OMDb enrichment (fast; only load CSVs into DB)")
    parser.add_argument("--recreate-ratings", action="store_true", help="Rename existing ratings->ratings_bad_<ts> and create fresh ratings table before import")
    args = parser.parse_args()

    main(limit=args.limit, no_enrich=args.no_enrich, recreate_ratings=args.recreate_ratings)
