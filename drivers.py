import logging

import pandas as pd
import fastf1
from fastf1.ergast import Ergast

from utils import get_team_names, get_country_codes
from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_drivers")

MONGODB_COLLECTION = "drivers"
UNIQUE_KEY = "driverCode"

INACTIVE_DRIVERS_CODES = ["DOO"]

TEAM_COLORS_FALLBACK = {
        "McLaren": "FF8000",
        "Red Bull Racing": "3671C6",
        "Mercedes":	"27F4D2",
        "Williams":	"64C4FF",
        "Aston Martin":	"229971",
        "Kick Sauber":	"52E252",
        "Ferrari":	"E80020",
        "Alpine":	"0093CC",
        "Racing Bulls":	"6692FF",
        "Haas F1 Team":	"B6BABD"
    }


def fetch_drivers(ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    try:
        drivers = ergast.get_driver_info(season=season)
        logger.info(f"Fetched drivers for season {season}.")
        return drivers
    except Exception as e:
        logger.error(f"Error fetching drivers: {e}")
        raise e
    
def fetch_driver_standings(ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    try:
        standings = ergast.get_driver_standings(season=season).content[0]
        logger.info(f"Fetched driver standings for season {season}.")
        return standings
    except Exception as e:
        logger.error(f"Error fetching driver standings: {e}")
        raise e
    
def fetch_team_colors(season: int = 2025) -> pd.DataFrame:
    try:
        event = fastf1.get_event_schedule(year=season).iloc[1]
        event_name = event['EventName']

        session = fastf1.get_session(season, event_name, 'R')
        session.load(telemetry=False, weather=False)

        results = session.results

        team_color_mapping = results[['TeamName', 'TeamColor']].drop_duplicates(subset='TeamName').dropna()
        team_color_mapping = team_color_mapping.rename(columns={'TeamName': 'team', 'TeamColor': 'teamColor'})

        if team_color_mapping.empty:
            raise ValueError("Team colors mapping is empty")
        logger.info(f"Fetched team colors for season {season}.")
        return team_color_mapping
    except Exception as e:
        logger.error(f"Error fetching team colors from FastF1, using fallback colors: {e}")
        return pd.DataFrame.from_dict(TEAM_COLORS_FALLBACK, orient='index', columns=['teamColor']).reset_index().rename(columns={'index': 'team'})

def process_drivers(drivers: pd.DataFrame, driver_standings: pd.DataFrame, team_colors: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Processing drivers.")

    # Get nationality in alpha2 and alpha3 codes
    drivers[["nationalityCode2", "nationalityCode3"]] = drivers["driverNationality"].apply(
        lambda x: pd.Series(get_country_codes(x))
    )

    # Rename name columns
    drivers.rename(columns={"givenName": "firstName", "familyName": "lastName"}, inplace=True)

    # Remove inactive drivers
    drivers = drivers[~drivers["driverCode"].isin(INACTIVE_DRIVERS_CODES)]

    # Get current team name and code
    driver_standings["teamCode"] = driver_standings["constructorIds"].apply(lambda x: x[-1])
    driver_standings["team"] = driver_standings["constructorIds"].apply(lambda x: get_team_names(x[-1]))

    # Merge drivers with their current team information
    drivers_with_team = drivers.merge(
        driver_standings[["driverCode", "teamCode", "team"]],
        on="driverCode",
        how="left"
    )

    # Merge with team colors, using fallback if missing
    drivers_with_team_colors = drivers_with_team.merge(
        team_colors,
        on='team',
        how='left'
    )

    logger.info(f"Processed {len(drivers_with_team_colors)} drivers.")

    return drivers_with_team_colors.to_dict(orient="records")

def main():
    ergast = Ergast()
    drivers_raw = fetch_drivers(ergast, season=2025)
    driver_standings_raw = fetch_driver_standings(ergast, season=2025)
    team_colors_raw = fetch_team_colors(season=2025)
    drivers_processed = process_drivers(drivers_raw, driver_standings_raw, team_colors_raw)
    print(drivers_processed)
    docs = prepare_documents(drivers_processed)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()