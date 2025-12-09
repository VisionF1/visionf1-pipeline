import logging

import pandas as pd
import numpy as np
import fastf1
from fastf1 import plotting
from datetime import datetime

from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_lap_time_distributions")

MONGODB_COLLECTION = "lap_time_distributions"
UNIQUE_KEY = "lap_time_distribution_id"

# SEASON = 2025


def get_schedule_data(season: int = 2025) -> pd.DataFrame:
    try:
        schedule = fastf1.get_event_schedule(season)
        schedule = schedule[schedule["EventFormat"] != "testing"]
        logger.info(f"Fetched event schedule for season {season}.")
        return schedule
    except Exception as e:
        logger.error(f"Error fetching event schedule: {e}")
        raise e
    
def get_most_recent_event() -> tuple[pd.Series, int]:
    current_year = datetime.now().year
    today = pd.Timestamp.now()
    
    # Try current year and previous year
    for year in [current_year, current_year - 1]:
        try:
            schedule = fastf1.get_event_schedule(year)
            schedule = schedule[schedule["EventFormat"] != "testing"].copy()
            
            # Convert EventDate to datetime if not already
            schedule["EventDate"] = pd.to_datetime(schedule["EventDate"], errors="coerce")
            
            # Filter events that have already occurred
            past_events = schedule[schedule["EventDate"] <= today]
            
            if not past_events.empty:
                # Get the most recent event
                most_recent_idx = past_events["EventDate"].idxmax()
                most_recent_event = past_events.loc[most_recent_idx]
                logger.info(f"Found most recent event: {most_recent_event['EventName']} ({year})")
                return most_recent_event, year
        except Exception as e:
            logger.error(f"Error fetching schedule for {year}: {e}")
            continue
    
    raise ValueError("No past events found in current or previous year")

def process_lap_time_distributions(event_data: pd.DataFrame, season: int = 2025) -> pd.DataFrame:
    logger.info(f"Processing lap_time_distributions data.")

    # Convert Series to DataFrame if it's only one event
    if isinstance(event_data, pd.Series):
        event_data = pd.DataFrame([event_data])

    all_lap_time_distributions = []
    
    for _, event in event_data.iterrows():
        event_name = event["EventName"]
        round_number = event["RoundNumber"]
        try:
            session = fastf1.get_session(season, event_name, 'R')
            session.load()
        except Exception as e:
            logger.error(f"Error loading session {season} {event_name}: {e}")
            continue

        if not hasattr(session, '_laps'):
            logger.warning(f"No lap data for {season} {event_name}, skipping.")
            continue
        
        # Filter for quick laps (representative race laps)
        laps = session.laps.pick_quicklaps()
        drivers = laps['Driver'].unique()

        driver_color_mapping = plotting.get_driver_color_mapping(session=session)

        for driver in drivers:
            driver_laps = laps.pick_drivers([driver])
            
            avg_laptime = driver_laps["LapTime"].mean()
            std_laptime = driver_laps["LapTime"].std()
            
            # Prepare list of laps for this driver
            laps_list = []
            for _, lap in driver_laps.iterrows():
                # Handle potentially missing TyreLife
                tyre_life = lap.get("TyreLife", None)
                if pd.isna(tyre_life):
                    tyre_life = None
                else:
                    tyre_life = int(tyre_life)

                laps_list.append({
                    "lap_number": int(lap["LapNumber"]),
                    "lap_time": lap["LapTime"].total_seconds(),
                    "compound": lap["Compound"],
                    "tyre_life": tyre_life
                })

            driver_info = session.get_driver(driver)
            driver_first_name = driver_info.get("FirstName", driver)
            driver_last_name = driver_info.get("LastName", "")
            driver_position = driver_info.get("Position", None)
            driver_color = driver_color_mapping.get(driver, "#cccccc")
            team = driver_info.get("TeamId", None)
            team_name = driver_info.get("TeamName", None)
            team_color = driver_info.get("TeamColor", "#cccccc")

            all_lap_time_distributions.append({
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
                "laps": laps_list,
                "avg_laptime": avg_laptime.total_seconds(),
                "std_laptime": std_laptime.total_seconds() if pd.notnull(std_laptime) else None,
                "lap_time_distribution_id": f"{season}_{round_number}_{driver}"
            })

    df_lap_time_distributions = pd.DataFrame(all_lap_time_distributions)
    
    # Replace NaNs for MongoDB compatibility
    df_lap_time_distributions = df_lap_time_distributions.replace({np.nan: None})

    # Ranking logic
    if not df_lap_time_distributions.empty:
         df_lap_time_distributions["race_pace_position"] = (
            df_lap_time_distributions.groupby(["season", "round"])["avg_laptime"]
            .rank(method="min", ascending=True)
            .astype(int)
        )

    logger.info(f"Processed {len(all_lap_time_distributions)} lap time distribution records.")

    return df_lap_time_distributions.to_dict(orient="records")

def main():
    # Option 1: Process all events of a specific season
    # schedule_data_raw = get_schedule_data(season=SEASON)
    # distributions_data = process_lap_time_distributions(schedule_data_raw, season=SEASON)
    
    # Option 2: Process only the most recent event
    most_recent_event, season = get_most_recent_event()
    distributions_data = process_lap_time_distributions(most_recent_event, season=season)

    docs = prepare_documents(distributions_data)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()
