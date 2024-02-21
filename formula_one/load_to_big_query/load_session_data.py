import pandas as pd
import numpy as np
import fastf1
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
import argparse
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import utils

path_cache = Path(__file__).parent.parent.parent / ".cache"
fastf1.Cache.enable_cache(str(path_cache))
path_dotenv = Path(__file__).parent.parent.parent / ".envrc"
load_dotenv(str(path_dotenv))

class Session:
    def __init__(self, year, name, type_):
        self.year = year
        self.name = name
        self.type_ = type_
    
    def load_data(self):
        self.session = fastf1.get_session(self.year, self.name, self.type_)
        self.session.load()
        return self

    def prepare_dataframe(self, df):
        df_out = pd.DataFrame(df).copy()
        df_out["EventYear"] = self.year
        df_out["EventName"] = self.name
        df_out["SessionType"] = self.type_
        return df_out

    def prepare_schema(self, schema):
        schema_full = [
            ("EventYear", "INTEGER", "NULLABLE"),
            ("EventName", "STRING", "NULLABLE"),
            ("SessionType", "STRING", "NULLABLE"),
        ] + schema
        return schema_full

    def get_for_bigquery(self):
        data = self.prepare_dataframe(self.get_data())
        schema = self.prepare_schema(self.get_schema())
        missing_columns = [
            c[0] for c in schema
            if c[0] not in data.columns
        ]
        extra_columns = [
            c for c in data.columns
            if c not in [s[0] for s in schema]
        ]
        assert len(missing_columns)==0, "Missing columns: {}".format(",".join(missing_columns))
        assert len(extra_columns)==0, "Extra columns: {}".format(",".join(extra_columns))
        data = data[[c[0] for c in schema]]
        return {"data": data, "schema": schema}

    @staticmethod
    def convert_interval_to_seconds(value):
        if isinstance(value, (float, np.float64)):
            return float(value)
        try:
            return float(value.total_seconds())
        except KeyboardInterrupt:
            return np.nan

class Results(Session):
    output_table = "src_session_results"

    def get_data(self):
        series = [
            pd.Series(self.session.get_driver(dr))
            for dr in self.session.drivers
        ]
        df = pd.concat([pd.DataFrame(x).T for x in series])
        for col in ['Time', 'Q1', 'Q2', 'Q3']:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        for col in ['DriverNumber']:
            df[col] = df[col].apply(int).astype(np.int64)
        for col in ['Points']:
            df[col] = df[col].apply(float).astype(np.float64)
        return df

    def get_schema(self):
        schema = [
            ("DriverNumber", "INTEGER", "NULLABLE"),
            ("BroadcastName", "STRING", "NULLABLE"),
            ("Abbreviation", "STRING", "NULLABLE"),
            ("TeamName", "STRING", "NULLABLE"),
            ("TeamColor", "STRING", "NULLABLE"),
            ("FirstName", "STRING", "NULLABLE"),
            ("LastName", "STRING", "NULLABLE"),
            ("FullName", "STRING", "NULLABLE"),
            ("Position", "INTEGER", "NULLABLE"),
            ("GridPosition", "INTEGER", "NULLABLE"),
            ("Q1", "FLOAT", "NULLABLE"),
            ("Q2", "FLOAT", "NULLABLE"),
            ("Q3", "FLOAT", "NULLABLE"),
            ("Time", "FLOAT", "NULLABLE"),
            ("Status", "STRING", "NULLABLE"),
            ("Points", "FLOAT", "NULLABLE"),
        ]
        return schema

class Laps(Session):
    output_table = "src_session_laps"

    def get_data(self):
        df = pd.DataFrame(self.session.laps)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        columns_to_seconds = [
            "Time",
            "LapTime",
            "PitOutTime",
            "PitInTime",
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "Sector1SessionTime",
            "Sector2SessionTime",
            "Sector3SessionTime",
            "LapStartTime",
        ]
        for col in columns_to_seconds:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        for col in ['DriverNumber', 'LapNumber', 'Stint', 'TrackStatus', 'IsAccurate']:
            df[col] = df[col].apply(int).astype(np.int64)
        return df

    def get_schema(self):
        schema = [
            ("Time", "FLOAT", "NULLABLE"),
            ("DriverNumber", "INTEGER", "NULLABLE"),
            ("LapTime", "FLOAT", "NULLABLE"),
            ("LapNumber", "INTEGER", "NULLABLE"),
            ("PitOutTime", "FLOAT", "NULLABLE"),
            ("PitInTime", "FLOAT", "NULLABLE"),
            ("Sector1Time", "FLOAT", "NULLABLE"),
            ("Sector2Time", "FLOAT", "NULLABLE"),
            ("Sector3Time", "FLOAT", "NULLABLE"),
            ("Sector1SessionTime", "FLOAT", "NULLABLE"),
            ("Sector2SessionTime", "FLOAT", "NULLABLE"),
            ("Sector3SessionTime", "FLOAT", "NULLABLE"),
            ("SpeedI1", "FLOAT", "NULLABLE"),
            ("SpeedI2", "FLOAT", "NULLABLE"),
            ("SpeedFL", "FLOAT", "NULLABLE"),
            ("SpeedST", "FLOAT", "NULLABLE"),
            ("IsPersonalBest", "BOOLEAN", "NULLABLE"),
            ("Compound", "STRING", "NULLABLE"),
            ("TyreLife", "FLOAT", "NULLABLE"),
            ("FreshTyre", "BOOLEAN", "NULLABLE"),
            ("Stint", "INTEGER", "NULLABLE"),
            ("LapStartTime", "FLOAT", "NULLABLE"),
            ("Team", "STRING", "NULLABLE"),
            ("Driver", "STRING", "NULLABLE"),
            ("TrackStatus", "INTEGER", "NULLABLE"),
            ("IsAccurate", "INTEGER", "NULLABLE"),
            ("LapStartDate", "TIMESTAMP", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema

class Weather(Session):
    output_table = "src_session_weather"

    def get_data(self):
        df = pd.DataFrame(self.session.weather_data)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Time"]:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        return df

    def get_schema(self):
        schema = [
            ("Time", "FLOAT", "NULLABLE"),
            ("AirTemp", "FLOAT", "NULLABLE"),
            ("Humidity", "FLOAT", "NULLABLE"),
            ("Pressure", "FLOAT", "NULLABLE"),
            ("Rainfall", "BOOLEAN", "NULLABLE"),
            ("TrackTemp", "FLOAT", "NULLABLE"),
            ("WindDirection", "FLOAT", "NULLABLE"),
            ("WindSpeed", "FLOAT", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema

class Status(Session):
    output_table = "src_session_status"

    def get_data(self):
        df = pd.DataFrame(self.session.session_status)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Time"]:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        return df

    def get_schema(self):
        schema = [
            ("Time", "FLOAT", "NULLABLE"),
            ("Status", "STRING", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema

class RaceControlMessages(Session):
    output_table = "src_session_messages"

    def get_data(self):
        df = pd.DataFrame(self.session.race_control_messages)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Sector", "RacingNumber"]:
            df[col] = df[col].astype(pd.Int64Dtype())
        return df

    def get_schema(self):
        schema = [
            ("Time", "TIMESTAMP", "NULLABLE"),
            ("Category", "STRING", "NULLABLE"),
            ("Message", "STRING", "NULLABLE"),
            ("Status", "STRING", "NULLABLE"),
            ("Flag", "STRING", "NULLABLE"),
            ("Scope", "STRING", "NULLABLE"),
            ("Sector", "INTEGER", "NULLABLE"),
            ("RacingNumber", "INTEGER", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema

class CarData(Session):
    output_table = "src_session_cardata"

    def _get_by_driver(self, driver_number):
        df = pd.DataFrame(
            self.session.laps.pick_driver(driver_number)
            .get_car_data()
            .add_differential_distance()
            .sort_values(by=["Date"])
        )
        return df

    def get_data(self):
        dfs = []
        for dr in self.session.drivers:
            try:
                this = self._get_by_driver(dr).assign(DriverNumber=dr)
            except (KeyError, ValueError) as e:
                logger.error(f"Issue with driver {dr}")
                logger.error(e)
                continue
            dfs.append(this)
        df = pd.concat(dfs)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Time", "SessionTime"]:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        for col in ["nGear", "DriverNumber"]:
            df[col] = df[col].astype(pd.Int64Dtype())
        return df

    def get_schema(self):
        schema = [
            ("Date", "TIMESTAMP", "NULLABLE"),
            ("Time", "FLOAT", "NULLABLE"),
            ("SessionTime", "FLOAT", "NULLABLE"),
            ("DriverNumber", "INTEGER", "NULLABLE"),
            ("RPM", "FLOAT", "NULLABLE"),
            ("Speed", "FLOAT", "NULLABLE"),
            ("nGear", "INTEGER", "NULLABLE"),
            ("Throttle", "FLOAT", "NULLABLE"),
            ("Brake", "BOOLEAN", "NULLABLE"),
            ("DRS", "INTEGER", "NULLABLE"),
            ("Source", "STRING", "NULLABLE"),
            ("DifferentialDistance", "FLOAT", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema

class PosData(Session):
    output_table = "src_session_posdata"

    def _get_by_driver(self, driver_number):
        df = pd.DataFrame(
            self.session.laps.pick_driver(driver_number)
            .get_pos_data()
            .sort_values(by=["Date"])
        )
        return df

    def get_data(self):
        dfs = []
        for dr in self.session.drivers:
            try:
                this = self._get_by_driver(dr).assign(DriverNumber=dr)
            except (KeyError, ValueError) as e:
                logger.error(f"Issue with driver {dr}")
                logger.error(e)
                continue
            dfs.append(this)
        df = pd.concat(dfs)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Time", "SessionTime"]:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        for col in ["DriverNumber"]:
            df[col] = df[col].astype(pd.Int64Dtype())
        return df

    def get_schema(self):
        schema = [
            ("Date", "TIMESTAMP", "NULLABLE"),
            ("Time", "FLOAT", "NULLABLE"),
            ("SessionTime", "FLOAT", "NULLABLE"),
            ("DriverNumber", "INTEGER", "NULLABLE"),
            ("Status", "STRING", "NULLABLE"),
            ("X", "FLOAT", "NULLABLE"),
            ("Y", "FLOAT", "NULLABLE"),
            ("Z", "FLOAT", "NULLABLE"),
            ("Source", "STRING", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema

class CarLapData(Session):
    output_table = "src_session_carlapdata"

    def _get_by_driver(self, driver_number):
        dfls = []
        for _, lap in self.session.laps.pick_driver(driver_number).iterlaps():
            df = lap.get_car_data().add_differential_distance()
            df["LapNumber"] = lap["LapNumber"]
            dfls.append(df)
        if len(dfls)>0:
            df = pd.concat(dfls).sort_values(by=["Date"])
            return df

    def get_data(self):
        dfs = []
        for dr in self.session.drivers:
            try:
                this = self._get_by_driver(dr)
                if this is None:
                    continue
                this = this.assign(DriverNumber=dr)
            except (KeyError, ValueError) as e:
                logger.error(f"Issue with driver {dr}")
                logger.error(e)
                continue
            dfs.append(this)
        df = pd.concat(dfs)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Time", "SessionTime"]:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        for col in ["nGear", "DriverNumber", "LapNumber"]:
            df[col] = df[col].astype(pd.Int64Dtype())
        return df

    def get_schema(self):
        schema = [
            ("Date", "TIMESTAMP", "NULLABLE"),
            ("Time", "FLOAT", "NULLABLE"),
            ("SessionTime", "FLOAT", "NULLABLE"),
            ("DriverNumber", "INTEGER", "NULLABLE"),
            ("LapNumber", "INTEGER", "NULLABLE"),
            ("RPM", "FLOAT", "NULLABLE"),
            ("Speed", "FLOAT", "NULLABLE"),
            ("nGear", "INTEGER", "NULLABLE"),
            ("Throttle", "FLOAT", "NULLABLE"),
            ("Brake", "BOOLEAN", "NULLABLE"),
            ("DRS", "INTEGER", "NULLABLE"),
            ("Source", "STRING", "NULLABLE"),
            ("DifferentialDistance", "FLOAT", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema


class PosLapData(Session):
    output_table = "src_session_poslapdata"

    def _get_by_driver(self, driver_number):
        dfls = []
        for _, lap in self.session.laps.pick_driver(driver_number).iterlaps():
            df = lap.get_pos_data()
            df["LapNumber"] = lap["LapNumber"]
            dfls.append(df)
        if len(dfls)>0:
            df = pd.concat(dfls).sort_values(by=["Date"])
            return df

    def get_data(self):
        dfs = []
        for dr in self.session.drivers:
            try:
                this = self._get_by_driver(dr)
                if this is None:
                    continue
                this = this.assign(DriverNumber=dr)
            except (KeyError, ValueError) as e:
                logger.error(f"Issue with driver {dr}")
                logger.error(e)
                continue
            dfs.append(this)
        df = pd.concat(dfs)
        df["ReferenceTime"] = self.session.t0_date
        df["SessionStartTime"] = self.convert_interval_to_seconds(self.session.session_start_time)
        for col in ["Time", "SessionTime"]:
            df[col] = df[col].apply(self.convert_interval_to_seconds).astype(float)
        for col in ["DriverNumber", "LapNumber"]:
            df[col] = df[col].astype(pd.Int64Dtype())
        return df

    def get_schema(self):
        schema = [
            ("Date", "TIMESTAMP", "NULLABLE"),
            ("Time", "FLOAT", "NULLABLE"),
            ("SessionTime", "FLOAT", "NULLABLE"),
            ("DriverNumber", "INTEGER", "NULLABLE"),
            ("LapNumber", "INTEGER", "NULLABLE"),
            ("Status", "STRING", "NULLABLE"),
            ("X", "FLOAT", "NULLABLE"),
            ("Y", "FLOAT", "NULLABLE"),
            ("Z", "FLOAT", "NULLABLE"),
            ("Source", "STRING", "NULLABLE"),
            ("ReferenceTime", "TIMESTAMP", "NULLABLE"),
            ("SessionStartTime", "FLOAT", "NULLABLE"),
        ]
        return schema


def run_event(year, name):
    bq = utils.BigQuery(
        project=os.environ["BIGQUERY_PROJECT"],
        dataset=os.environ["BIGQUERY_DATASET"],
        credentials_path=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    )
    session_types = ['FP1', 'FP2', 'FP3', 'Q', 'S', 'R']
    classes = [
        Results,
        Laps,
        Weather,
        Status,
        RaceControlMessages,
        CarData,
        PosData,
        CarLapData,
        PosLapData,
    ]
    for session_type in session_types:
        for cl in classes:
            try:
                obj = cl(year, name, session_type).load_data()
            except ValueError as e:
                print(e)
                continue
            to_save = obj.get_for_bigquery()
            print("TABLE NAME", cl.output_table)
            bq.write_data(
                to_save['data'].assign(uploaded_at=datetime.utcnow()),
                table_name=cl.output_table,
                schema=to_save['schema'] + [("uploaded_at", "TIMESTAMP", "NULLABLE")],
            )            

def run_year(year):
    bq = utils.BigQuery(
        project=os.environ["BIGQUERY_PROJECT"],
        dataset=os.environ["BIGQUERY_DATASET"],
        credentials_path=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    )
    query = """
        SELECT Year, EventName, EventDate
        FROM {project}.{dataset}.stg_schedule
        WHERE Year = {year}
        GROUP BY 1,2,3 ORDER BY EventDate
    """
    events = bq.run_query(query, year=year)
    for _, row in events.iterrows():
        try:
            run_event(row['Year'], row['EventName'])
        except ValueError:
            logger.error("Cannot fetch {} {}".format(row['Year'], row['EventName']))
            continue
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year')
    args = parser.parse_args()
    run_year(int(args.year))
