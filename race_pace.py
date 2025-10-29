import logging

import pandas as pd
import numpy as np
import fastf1
from fastf1 import plotting

from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_events")

MONGODB_COLLECTION = "race_pace"
UNIQUE_KEY = "race_pace_id"

SEASON = 2025


def get_schedule_data(season: int = 2025) -> pd.DataFrame:
    try:
        schedule = fastf1.get_event_schedule(season)
        schedule = schedule[schedule["EventFormat"] != "testing"]
        logger.info(f"Fetched event schedule for season {season}.")
        return schedule
    except Exception as e:
        logger.error(f"Error fetching event schedule: {e}")
        raise e

def process_race_pace_data(event_data: pd.DataFrame, season: int = 2025) -> pd.DataFrame:
    logger.info(f"Processing race_pace data.")

    all_race_pace = []
    
    for _, event in event_data.iterrows():
        event_name = event["EventName"]
        round_number = event["RoundNumber"]
        try:
            session = fastf1.get_session(season, event_name, 'R')
            session.load()
        except Exception as e:
            print(f"Error loading session {season} {event_name}.")
            continue

        if not hasattr(session, '_laps'):
            print(f"No lap data for {season} {event_name}, skipping.")
            continue
        
        laps = session.laps
        laps = laps[laps['IsAccurate']]
        drivers = laps['Driver'].unique()

        driver_color_mapping = plotting.get_driver_color_mapping(session=session)

        for driver in drivers:
            filtered_laps = laps.pick_drivers([driver])
            
            avg_laptime = filtered_laps["LapTime"].mean()
            std_laptime = filtered_laps["LapTime"].std()

            driver_info = session.get_driver(driver)
            driver_first_name = driver_info.get("FirstName", driver)
            driver_last_name = driver_info.get("LastName", "")
            driver_position = driver_info.get("Position", None)
            driver_color = driver_color_mapping.get(driver, "#cccccc")
            team = driver_info.get("TeamId", None)
            team_name = driver_info.get("TeamName", None)
            team_color = driver_info.get("TeamColor", "#cccccc")

            all_race_pace.append({
                "season": season,
                "round": int(round_number),
                "event": event_name,
                "driver": driver,
                "driver_first_name": driver_first_name,
                "driver_last_name": driver_last_name,
                "driver_position": driver_position,
                "driver_color": driver_color,
                "team": team,
                "team_name": team_name,
                "team_color": team_color,
                "avg_laptime": avg_laptime.total_seconds(),
                "std_laptime": std_laptime.total_seconds() if pd.notnull(std_laptime) else None,
                "race_pace_id": f"{season}_{round_number}_{driver}"
            })

    df_race_pace = pd.DataFrame(all_race_pace)

    df_race_pace = df_race_pace.replace({np.nan: None})

    df_race_pace["race_pace_position"] = (
        df_race_pace.groupby(["season", "round"])["avg_laptime"]
        .rank(method="min", ascending=True)
        .astype(int)
    )

    logger.info(f"Processed {len(all_race_pace)} race pace records.")

    return df_race_pace.to_dict(orient="records")

def main():
    schedule_data_raw = get_schedule_data(season=SEASON)
    race_pace_data_processed = process_race_pace_data(schedule_data_raw, season=SEASON)
    docs = prepare_documents(race_pace_data_processed)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()