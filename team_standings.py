import logging

import pandas as pd
from fastf1.ergast import Ergast

from utils import get_team_names, get_country_codes
from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_team_standings")

MONGODB_COLLECTION = "team_standings"
UNIQUE_KEY = "teamCode"


def fetch_team_standings(ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    try:
        team_standings = ergast.get_constructor_standings(season=season).content[0]
        logger.info(f"Fetched team standings for season {season}.")
        return team_standings
    except Exception as e:
        logger.error(f"Error fetching team standings: {e}")
        raise e

def process_team_standings(team_standings: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Processing team standings.")

    # Get current team name from dictionary
    team_standings["team"] = team_standings["constructorId"].apply(lambda x: get_team_names(x))

    # Get team code
    team_standings["teamCode"] = team_standings["constructorId"]

    # Get nationality in alpha2 and alpha3 codes
    team_standings[["nationalityCode", "nationality"]] = team_standings["constructorNationality"].apply(
        lambda x: pd.Series(get_country_codes(x))
    )

    # Select and rename columns
    team_standings_filtered = team_standings[[
        "position", "team", "teamCode", "nationality", "nationalityCode", "points"
    ]].copy()

    logger.info(f"Processed {len(team_standings_filtered)} team standings.")

    return team_standings_filtered.to_dict(orient="records")

def main():
    ergast = Ergast()
    team_standings_raw = fetch_team_standings(ergast, season=2025)
    team_standings_processed = process_team_standings(team_standings_raw)
    docs = prepare_documents(team_standings_processed)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()