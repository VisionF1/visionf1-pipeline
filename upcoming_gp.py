import logging

import pandas as pd
from fastf1.ergast import Ergast

from utils import get_country_alpha2_code
from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_upcoming_gp")

MONGODB_COLLECTION = "upcoming_gp"
UNIQUE_KEY = "id"


def fetch_races(ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    try:
        races = ergast.get_race_schedule(season=season)
        logger.info(f"Fetched races for season {season}.")
        return races
    except Exception as e:
        logger.error(f"Error fetching races: {e}")
        raise e

def process_races(races: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Processing races.")

    # Get country in alpha2 code
    races[["countryCode"]] = races["country"].apply(
        lambda x: pd.Series(get_country_alpha2_code(x))
    )

    # Ergast/Jolpica uses UTC for all datetime values
    now = pd.Timestamp.utcnow().replace(tzinfo=None)
    # Filter for upcoming GPs based on FP1 date
    upcoming_gp = races[races["fp1Date"] > now].sort_values("fp1Date")

    # If no upcoming GP is found, create a placeholder for the next season
    if upcoming_gp.empty:
        upcoming_gp_filtered = pd.Series(data={"season": 2026, "round": 1, "raceName": "Round 1 of the 2026 F1 Championship", "circuitId": "", "circuitName": "TBD", "countryCode": "", "country": "TBD", "locality": "TBD", "startDate": pd.Timestamp("2026-12-30 00:00:00"), "endDate": pd.Timestamp("2025-12-31 00:00:00")})

    # Else, take the next upcoming GP
    else:
        # Take the first row (next GP)
        upcoming_gp = upcoming_gp.iloc[0].copy()

        # Rename columns
        upcoming_gp["name"] = upcoming_gp["raceName"]
        upcoming_gp["circuit"] = upcoming_gp["circuitName"]
        upcoming_gp["startDate"] = upcoming_gp["fp1Date"]
        
        # Combine raceDate and raceTime into a datetime, then add 3 hours to simulate race duration
        race_datetime = pd.to_datetime(str(upcoming_gp["raceDate"]) + " " + str(upcoming_gp["raceTime"])).tz_localize(None)
        upcoming_gp["endDate"] = race_datetime + pd.Timedelta(hours=3)
        
        # Create a unique ID for the race
        upcoming_gp["id"] = f"{upcoming_gp['season']}-{upcoming_gp['round']}"

        # Select and rename columns
        upcoming_gp_filtered = upcoming_gp[[
            "id", "season", "round", "name", "circuitId", "circuit", "countryCode", "country", "locality", "startDate", "endDate"
        ]]

    logger.info(f"Processed upcoming GP.")

    return [upcoming_gp_filtered.to_dict()] # Because we want a list of dicts (and upcoming_gp is a Series instead of a DataFrame)

def main():
    ergast = Ergast()
    races_raw = fetch_races(ergast, season=2025)
    upcoming_gp_processed = process_races(races_raw)
    docs = prepare_documents(upcoming_gp_processed)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()