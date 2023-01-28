import pandas as pd
import numpy as np
import fastf1
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
import argparse
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import utils

path_cache = Path(__file__).parent.parent.parent / ".cache"
fastf1.Cache.enable_cache(str(path_cache))
path_dotenv = Path(__file__).parent.parent.parent / ".envrc"
load_dotenv(str(path_dotenv))

def get_year_schedule(year):
    schedule = fastf1.get_event_schedule(year)
    return schedule

def load_multiple_years(year_min, year_max):
    schedules = [
        (year, get_year_schedule(year))
        for year in range(year_min, year_max+1)
    ]
    df_schedules = pd.DataFrame(pd.concat([
        val.assign(Year=key) for key, val in schedules
    ]))
    return df_schedules

def get_bigquery_params():
    bigquery_params = {
        "project": os.environ["BIGQUERY_PROJECT"],
        "dataset": os.environ["BIGQUERY_DATASET"],
        "credentials_path": os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        "table": "src_schedule",
        "schema": [
            ("Year", "INTEGER", "NULLABLE"),
            ("RoundNumber", "INTEGER", "NULLABLE"),
            ("Country", "STRING", "NULLABLE"),
            ("Location", "STRING", "NULLABLE"),
            ("OfficialEventName", "STRING", "NULLABLE"),
            ("EventDate", "TIMESTAMP", "NULLABLE"),
            ("EventName", "STRING", "NULLABLE"),
            ("EventFormat", "STRING", "NULLABLE"),
            ("Session1", "STRING", "NULLABLE"),
            ("Session1Date", "TIMESTAMP", "NULLABLE"),
            ("Session2", "STRING", "NULLABLE"),
            ("Session2Date", "TIMESTAMP", "NULLABLE"),
            ("Session3", "STRING", "NULLABLE"),
            ("Session3Date", "TIMESTAMP", "NULLABLE"),
            ("Session4", "STRING", "NULLABLE"),
            ("Session4Date", "TIMESTAMP", "NULLABLE"),
            ("Session5", "STRING", "NULLABLE"),
            ("Session5Date", "TIMESTAMP", "NULLABLE"),
            ("F1ApiSupport", "BOOLEAN", "NULLABLE"),
            ("uploaded_at", "TIMESTAMP", "NULLABLE"),
        ],
    }
    return bigquery_params

def main(year_min, year_max):
    df = load_multiple_years(year_min, year_max)
    bigquery_params = get_bigquery_params()
    bq = utils.BigQuery(
        project=bigquery_params["project"],
        dataset=bigquery_params["dataset"],
        credentials_path=bigquery_params["credentials_path"],
    )
    bq.write_data(
        df.assign(uploaded_at=datetime.utcnow()),
        table_name=bigquery_params["table"],
        schema=bigquery_params["schema"],
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year_min')
    parser.add_argument('--year_max')
    args = parser.parse_args()
    main(int(args.year_min), int(args.year_max))
