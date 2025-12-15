
# VisionF1 Pipeline

**VisionF1 Pipeline** is the automated Extract, Transform, Load (ETL) engine responsible for populating the VisionF1 platform with real-time and historical Formula 1 data.

It orchestrates the ingestion of telemetry, timing data, and event schedules from external providers (FastF1, Ergast), processes raw signals into advanced analytical metrics, and synchronizes the results with the central MongoDB Atlas database.

---

## Key Features

*   **Automated Data Ingestion**: Systematically fetches data for races, qualifying sessions, and practice runs.
*   **Advanced Metrics Calculation**:
    *   **Race Pace**: Lap time analysis with outlier detection.
    *   **Clean Air Pace**: Filters traffic to isolate pure car performance.
    *   **Lap Time Distributions**: Statistical generation for variability analysis.
*   **Content Synchronization**: Updates static and dynamic content including driver/team standings, calendars, and upcoming event metadata.
*   **Robust Data Storage**: Implements idempotent "upsert" logic to MongoDB, ensuring data consistency and preventing duplication.
*   **Error Handling**: Comprehensive logging and fault tolerance for API rate limits and missing session data.

---

## Tech Stack

*   **Language**: Python 3.10+
*   **Data Sources**: FastF1 API, Ergast Developer API
*   **Database**: MongoDB (via PyMongo)
*   **Processing**: Pandas, NumPy
*   **Visualization**: Matplotlib/Seaborn (for static report generation)

---

## Pipeline Components

The pipeline consists of modular scripts, each responsible for a specific data domain:

| Script | Function | Target Collection |
| :--- | :--- | :--- |
| `race_pace.py` | Calculates average lap times per driver for the latest race. | `race_pace` |
| `clean_air_race_pace.py` | computes pace excluding laps with traffic interference. | `clean_air_race_pace` |
| `lap_time_distributions.py` | Generates statistical distributions for lap consistency. | `lap_time_distributions` |
| `driver_standings.py` | Updates the current season's driver championship points. | `driver_standings` |
| `team_standings.py` | Updates the constructor championship points. | `team_standings` |
| `upcoming_gp.py` | Fetches metadata for the next scheduled Grand Prix. | `upcoming_gp` |
| `events.py` | Synchronizes the full season calendar. | `events` |

---

## Data Flow Architecture

1.  **Ingest**: Scripts query the FastF1 API for specific `Season`, `Event`, and `Session` data.
2.  **Transform**: Raw telemetry is cleaned, outliers are removed, and derived metrics (e.g., standard deviation of lap times) are calculated using Pandas.
3.  **Load**: Processed documents are structured with unique keys (e.g., `2025_24_VER`) and upserted to MongoDB Cloud.

---

## Installation & Usage

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/visionf1-pipeline.git
    cd visionf1-pipeline
    ```

2.  **Environment Setup:**
    Create a `.env` file in the root directory:
    ```env
    MONGODB_URI=your_mongodb_connection_string
    MONGODB_DB=Visionf1
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run a Pipeline Job:**
    Execute specific scripts manually or via cron scheduler:
    ```bash
    # Update race pace data for the latest event
    python race_pace.py

    # Update driver standings
    python driver_standings.py
    ```

---

© 2025 VisionF1 Team
