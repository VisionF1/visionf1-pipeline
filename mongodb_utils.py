import os
import logging
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError


load_dotenv()

MONGODB_DB = os.getenv("MONGODB_DB", "Visionf1")
MONGODB_URI = os.getenv("MONGODB_URI")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mongodb_utils")


def prepare_documents(items: List[Dict]) -> List[Dict]:
    logger.info("Preparing documents for MongoDB upsert.")

    now = datetime.now().isoformat() + "Z"
    docs = []
    for item in items:
        doc = dict(item)  # copy
        doc["_updated_at"] = now
        docs.append(doc)
    return docs

def upsert_to_mongo(docs: List[Dict], unique_key: str, collection_name: str) -> None:
    logger.info("Upserting documents to MongoDB.")
    
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
    coll = db[collection_name]

    # Ensure unique index
    try:
        coll.create_index(unique_key, unique=True)
    except Exception:
        logger.exception("Error creating index (may already exist).")

    ops = []
    for doc in docs:
        filter_q = {unique_key: doc[unique_key]}
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