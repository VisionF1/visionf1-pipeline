import logging

import pandas as pd
import fastf1
from fastf1.ergast import Ergast
from datetime import datetime

from utils import get_country_alpha2_code
from mongodb_utils import prepare_documents, upsert_to_mongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("upsert_events")

MONGODB_COLLECTION = "events"
UNIQUE_KEY = "event_id"

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

def process_event_data(event_data: pd.DataFrame, ergast: Ergast, season: int = 2025) -> pd.DataFrame:
    logger.info(f"Processing event data.")

    # Convert Series to DataFrame if it's only one event
    if isinstance(event_data, pd.Series):
        event_data = pd.DataFrame([event_data])

    all_events = []
    
    for _, event in event_data.iterrows():
        event_name = event["EventName"]
        round_number = event["RoundNumber"]
        location = event["Location"]
        country = event["Country"]
        country_code = get_country_alpha2_code(country)
        event_format = event["EventFormat"]
        event_date = event["EventDate"]

        circuit = ergast.get_circuits(season=season, round=round_number)
        if not circuit.empty:
            circuit_id = circuit.iloc[0]["circuitId"]
            circuit_name = circuit.iloc[0]["circuitName"]
        else:
            print(f"Missing circuit information for {season} {round_number}")

        try:
            session = fastf1.get_session(season, event_name, 'R')
            session.load(telemetry=False, weather=False)
        except Exception as e:
            print(f"Error loading session {season} {event_name}.")
            continue

        results = session.results

        driver_codes = results["Abbreviation"].dropna().unique().tolist()
        driver_names = results["FullName"].dropna().unique().tolist()
        team_codes = results["TeamId"].dropna().unique().tolist()
        team_names = results["TeamName"].dropna().unique().tolist()
        team_colors = results["TeamColor"].dropna().unique().tolist()

        winner = results[results["Position"] == 1].iloc[0]["Abbreviation"] if not results[results["Position"] == 1].empty else None
        pole = results[results["GridPosition"] == 1].iloc[0]["Abbreviation"] if not results[results["GridPosition"] == 1].empty else None

        event_status = "ended" if winner else "upcoming"

        all_events.append({
            "season": season,
            "round": round_number,
            "event_name": event_name,
            "location": location,
            "country": country,
            "country_code": country_code,
            "event_date": event_date,
            "event_format": event_format,
            "event_status": event_status,
            "circuit_id": circuit_id,
            "circuit_name": circuit_name,
            "n_drivers": len(driver_codes),
            "driver_codes": driver_codes,
            "driver_names": driver_names,
            "n_teams": len(team_codes),
            "team_codes": team_codes,
            "team_names": team_names,
            "team_colors": team_colors,
            "winner": winner,
            "pole": pole,
            "event_id": f"{season}_{round_number}_{event_name}"
        })

    df_events = pd.DataFrame(all_events)
    df_events.sort_values(["season", "round"], inplace=True)
    df_events.reset_index(drop=True, inplace=True)

    logger.info(f"Processed {len(all_events)} events.")

    return df_events.to_dict(orient="records")

def main():
    ergast = Ergast()
    
    # Option 1: Process all events of a specific season
    # schedule_data_raw = get_schedule_data(season=SEASON)
    # event_data_processed = process_event_data(schedule_data_raw, ergast, season=SEASON)
    
    # Option 2: Process only the most recent event
    most_recent_event, season = get_most_recent_event()
    event_data_processed = process_event_data(most_recent_event, ergast, season=season)

    docs = prepare_documents(event_data_processed)
    upsert_to_mongo(docs, UNIQUE_KEY, MONGODB_COLLECTION)

if __name__ == "__main__":
    main()