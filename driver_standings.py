import logging

import pandas as pd
from fastf1.ergast import Ergast

from utils import get_team_names, get_country_codes
from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_driver_standings")

MONGODB_COLLECTION = "driver_standings"
UNIQUE_KEY = "driverCode"


def fetch_driver_standings(ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    try:
        standings = ergast.get_driver_standings(season=season).content[0]
        logger.info(f"Fetched driver standings for season {season}.")
        return standings
    except Exception as e:
        logger.error(f"Error fetching driver standings: {e}")
        raise e
    
def process_driver_standings(driver_standings: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Processing driver standings.")

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

    logger.info(f"Processed {len(driver_standings_filtered)} driver standings.")

    return driver_standings_filtered.to_dict(orient="records")

def main():
    ergast = Ergast()
    driver_standings_raw = fetch_driver_standings(ergast, season=2025)
    driver_standings_processed = process_driver_standings(driver_standings_raw)
    docs = prepare_documents(driver_standings_processed)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()