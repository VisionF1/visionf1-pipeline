"""
Upsert driver standings from FastAPI - Ergast API to MongoDB Atlas.
"""
from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError

import pandas as pd
from fastf1.ergast import Ergast

from utils import get_team_names, get_country_codes


load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "visionf1")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "driver_standings")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_driver_standings")


def fetch_driver_standings(ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    try:
        standings = ergast.get_driver_standings(season=season).content[0]
        logger.info(f"Fetched driver standings for season {season}.")
        return standings
    except Exception as e:
        logger.error(f"Error fetching driver standings: {e}")
        raise e
    
def process_driver_standings(driver_standings: pd.DataFrame) -> pd.DataFrame:
    # Get full name
    driver_standings["driver"] = driver_standings["givenName"] + " " + driver_standings["familyName"]

    # Get nationality in alpha2 and alpha3 codes
    driver_standings[["nationalityCode", "nationality"]] = driver_standings["driverNationality"].apply(
        lambda x: pd.Series(get_country_codes(x))
    )

    # Get current team name and code
    driver_standings["team"] = driver_standings["constructorIds"].apply(lambda x: get_team_names(x[-1]))
    driver_standings["teamCode"] = driver_standings["constructorIds"].apply(lambda x: x[-1])

    # Select and rename columns
    driver_standings_filtered = driver_standings[[
        "position", "driver", "driverCode", "nationality", "nationalityCode", "team", "teamCode", "points"
    ]].copy()

    return driver_standings_filtered.to_dict(orient="records")

def prepare_documents(items: List[Dict]) -> List[Dict]:
    now = datetime.now().isoformat() + "Z"
    docs = []
    for item in items:
        doc = dict(item)  # copy
        doc["_updated_at"] = now
        docs.append(doc)
    return docs

def upsert_to_mongo(docs: List[Dict]) -> None:
    if not MONGODB_URI:
        logger.error("MONGODB_URI undefined. Load .env or export the environment variable.")
        return

    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # forced check
    except ServerSelectionTimeoutError as e:
        logger.error("Couldn't connect to MongoDB: %s", e)
        return
    except Exception as e:
        logger.exception("Error creating MongoDB client: %s", e)
        return

    db = client[MONGODB_DB]
    coll = db[MONGODB_COLLECTION]

    # Ensure unique index on driverCode
    try:
        coll.create_index("driverCode", unique=True)
    except Exception:
        logger.exception("Error creating index (may already exist).")

    ops = []
    for doc in docs:
        filter_q = {"driverCode": doc["driverCode"]}
        update = {"$set": doc, "$setOnInsert": {"_created_at": doc["_updated_at"]}}
        ops.append(UpdateOne(filter_q, update, upsert=True))

    if not ops:
        logger.info("No documents to upsert.")
        client.close()
        return

    try:
        result = coll.bulk_write(ops, ordered=False)
        logger.info("Upsert finished. matched=%d modified=%d upserted=%d", result.matched_count, result.modified_count, len(result.upserted_ids or {}))
    except BulkWriteError as bwe:
        logger.exception("BulkWriteError: %s", bwe.details)
    except Exception:
        logger.exception("Error executing bulk_write")
    finally:
        client.close()

def main() -> None:
    ergast = Ergast()
    driver_standings_raw = fetch_driver_standings(ergast, season=2025)
    driver_standings = process_driver_standings(driver_standings_raw)
    docs = prepare_documents(driver_standings)
    upsert_to_mongo(docs)

if __name__ == "__main__":
    main()