"""
Upsert driver standings from in-memory DB to MongoDB Atlas.
"""
from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB", "visionf1")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "driver_standings")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_driver_standings")

# In-memory driver standings data
DRIVER_STANDINGS: List[Dict] = [
  {"position": 1, "driver": "Oscar Piastri", "driverCode": "PIA", "nationality": "AUS", "nationalityCode": "au", "team": "McLaren", "teamCode": "MCL", "points": 324},
  {"position": 2, "driver": "Lando Norris", "driverCode": "NOR", "nationality": "GBR", "nationalityCode": "gb", "team": "McLaren", "teamCode": "MCL", "points": 293},
  {"position": 3, "driver": "Max Verstappen", "driverCode": "VER", "nationality": "NED", "nationalityCode": "nl", "team": "Red Bull Racing", "teamCode": "RB", "points": 230},
  {"position": 4, "driver": "George Russell", "driverCode": "RUS", "nationality": "GBR", "nationalityCode": "gb", "team": "Mercedes", "teamCode": "MER", "points": 194},
  {"position": 5, "driver": "Charles Leclerc", "driverCode": "LEC", "nationality": "MON", "nationalityCode": "mc", "team": "Ferrari", "teamCode": "FER", "points": 163},
  {"position": 6, "driver": "Lewis Hamilton", "driverCode": "HAM", "nationality": "GBR", "nationalityCode": "gb", "team": "Ferrari", "teamCode": "FER", "points": 117},
  {"position": 7, "driver": "Alexander Albon", "driverCode": "ALB", "nationality": "THA", "nationalityCode": "th", "team": "Williams", "teamCode": "WIL", "points": 70},
  {"position": 8, "driver": "Kimi Antonelli", "driverCode": "ANT", "nationality": "ITA", "nationalityCode": "it", "team": "Mercedes", "teamCode": "MER", "points": 66},
  {"position": 9, "driver": "Isack Hadjar", "driverCode": "HAD", "nationality": "FRA", "nationalityCode": "fr", "team": "Racing Bulls", "teamCode": "RBU", "points": 38},
  {"position": 10, "driver": "Nico Hulkenberg", "driverCode": "HUL", "nationality": "GER", "nationalityCode": "de", "team": "Kick Sauber", "teamCode": "SAU", "points": 37},
  {"position": 11, "driver": "Lance Stroll", "driverCode": "STR", "nationality": "CAN", "nationalityCode": "ca", "team": "Aston Martin", "teamCode": "AM", "points": 32},
  {"position": 12, "driver": "Fernando Alonso", "driverCode": "ALO", "nationality": "ESP", "nationalityCode": "es", "team": "Aston Martin", "teamCode": "AM", "points": 30},
  {"position": 13, "driver": "Esteban Ocon", "driverCode": "OCO", "nationality": "FRA", "nationalityCode": "fr", "team": "Haas", "teamCode": "HAA", "points": 28},
  {"position": 14, "driver": "Pierre Gasly", "driverCode": "GAS", "nationality": "FRA", "nationalityCode": "fr", "team": "Alpine", "teamCode": "ALP", "points": 20},
  {"position": 15, "driver": "Liam Lawson", "driverCode": "LAW", "nationality": "NZL", "nationalityCode": "nz", "team": "Racing Bulls", "teamCode": "RBU", "points": 20},
  {"position": 16, "driver": "Gabriel Bortoleto", "driverCode": "BOR", "nationality": "BRA", "nationalityCode": "br", "team": "Kick Sauber", "teamCode": "SAU", "points": 18},
  {"position": 17, "driver": "Oliver Bearman", "driverCode": "BEA", "nationality": "GBR", "nationalityCode": "gb", "team": "Haas", "teamCode": "HAA", "points": 16},
  {"position": 18, "driver": "Carlos Sainz", "driverCode": "SAI", "nationality": "ESP", "nationalityCode": "es", "team": "Williams", "teamCode": "WIL", "points": 16},
  {"position": 19, "driver": "Yuki Tsunoda", "driverCode": "TSU", "nationality": "JPN", "nationalityCode": "jp", "team": "Red Bull Racing", "teamCode": "RB", "points": 12},
  {"position": 20, "driver": "Franco Colapinto", "driverCode": "COL", "nationality": "ARG", "nationalityCode": "ar", "team": "Alpine", "teamCode": "ALP", "points": 0},
  {"position": 21, "driver": "Jack Doohan", "driverCode": "DOO", "nationality": "AUS", "nationalityCode": "au", "team": "Alpine", "teamCode": "ALP", "points": 0},
]

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
    docs = prepare_documents(DRIVER_STANDINGS)
    upsert_to_mongo(docs)

if __name__ == "__main__":
    main()