from pathlib import Path
import pandas as pd
import numpy as np
import argparse


class TrustedToRefined:
    trusted_folder = (
        Path("~/localdatalake/formula_one").expanduser() / "data" / "trusted"
    )
    refined_folder = (
        Path("~/localdatalake/formula_one").expanduser() / "data" / "refined"
    )

    def read_from_trusted(self, dataset_name):
        path = self.trusted_folder / (dataset_name + ".parquet")
        df = pd.read_parquet(path)
        return df

    def read_from_refined(self, dataset_name):
        path = self.refined_folder / (dataset_name + ".parquet")
        df = pd.read_parquet(path)
        return df

    def run(self):
        df_save = self.transform()
        self.refined_folder.mkdir(parents=True, exist_ok=True)
        path = self.refined_folder / (self.dataset_name + ".parquet")
        df_save.to_parquet(path)

    def get_schedule(self):
        df_in = self.read_from_trusted("Schedule")
        pkey = [col for col in df_in.columns if not col.startswith("Session")]
        df_out = pd.concat(
            [
                (
                    df_in[pkey + [f"Session{i}", f"Session{i}Date"]]
                    .rename(
                        columns={
                            f"Session{i}": "SessionName",
                            f"Session{i}Date": "SessionDate",
                        }
                    )
                    .assign(session_index=i)
                )
                for i in range(1, 6)
            ]
        )
        df_out = df_out.rename(
            columns={
                "RoundNumber": "round_number",
                "EventName": "weekend_short_name",
                "OfficialEventName": "weekend_official_name",
                "Country": "weekend_country",
                "Location": "weekend_location",
                "EventDate": "weekend_date",
                "EventFormat": "weekend_format",
                "F1ApiSupport": "weekend_api_support",
                "is_testing": "weekend_is_testing",
                "SessionName": "session_name",
                "SessionDate": "session_date",
            }
        )[
            [
                "year",
                "round_number",
                "session_index",
                "weekend_short_name",
                "weekend_official_name",
                "weekend_country",
                "weekend_location",
                "weekend_date",
                "weekend_format",
                "weekend_api_support",
                "weekend_is_testing",
                "session_name",
                "session_date",
            ]
        ]
        return df_out


class RefinedTiming(TrustedToRefined):
    def __init__(self, year):
        self.year = year
        self.dataset_name = f"SessionTiming_Y{year}"

    def transform(self):
        df_laps = self.read_from_trusted(f"Laps_Y{self.year}")
        pkey = ["year", "round_number", "session_type", "DriverNumber"]
        df_refined_laps = (
            df_laps.groupby(["year", "round_number", "session_type", "DriverNumber"])
            .apply(lambda df: self.split_laps_by_stint(df).drop(pkey, axis=1))
            .reset_index()
            .rename(
                columns={
                    "session_type": "session_index",
                    "DriverNumber": "driver_id",
                    "LapNumber": "lap_number",
                    "IsAccurate": "lap_is_accurate",
                }
            )
        )
        df_out = (
            df_refined_laps.merge(
                self.get_schedule(),
                on=["year", "round_number", "session_index"],
                how="left",
            )
            .assign(ts_reference=lambda df: df["t0_date"].dt.tz_localize("utc"))
            .assign(
                session_reference_time=lambda df: df["ts_reference"],
                session_start_time=lambda df: df["ts_reference"]
                + df["session_start_time"],
                start_timestamp=lambda df: df["ts_reference"] + df["start_timestamp"],
                end_timestamp=lambda df: df["ts_reference"] + df["end_timestamp"],
                elapsed_time=lambda df: df["elapsed_time"].apply(
                    lambda v: v.total_seconds()
                ),
            )
            .rename(
                columns={
                    "year": "weekend_year",
                    "round_number": "weekend_index",
                }
            )
        )
        integer_columns = [
            "weekend_year",
            "weekend_index",
            "session_index",
            "driver_id",
            "event_index",
            "lap_number",
            "stint_number",
            "lap_number_in_stint",
        ]
        for col in integer_columns:
            df_out[col] = df_out[col].astype(pd.Int64Dtype())
        df_out = df_out[
            [
                "weekend_year",
                "weekend_index",
                "weekend_short_name",
                "weekend_official_name",
                "weekend_country",
                "weekend_location",
                "weekend_date",
                "weekend_format",
                "weekend_api_support",
                "weekend_is_testing",
                "session_index",
                "session_name",
                "session_date",
                "session_reference_time",
                "session_start_time",
                "driver_id",
                "event_index",
                "event_type",
                "lap_number",
                "stint_number",
                "lap_number_in_stint",
                "lap_is_accurate",
                "start_location",
                "end_location",
                "start_timestamp",
                "end_timestamp",
                "elapsed_time",
            ]
        ]
        return df_out

    @staticmethod
    def split_laps_by_stint(df_session_driver):
        """
        Calculates timing for each lap and pit event.
        """
        # Split by stint
        df_in = df_session_driver.sort_values(by="LapNumber")
        counter_stint = 1
        counter_lap = 1
        df_out = df_session_driver.copy()
        df_out["stint_number"] = None
        df_out["lap_number_in_stint"] = None
        for i in range(len(df_out)):
            df_out["stint_number"].iloc[i] = counter_stint
            df_out["lap_number_in_stint"].iloc[i] = counter_lap
            if pd.notna(df_out.iloc[i]["PitInTime"]):
                counter_stint += 1
                counter_lap = 0
            counter_lap += 1

        flter = df_out["PitOutTime"].notna() & (
            df_out["PitOutTime"] > df_out["LapStartTime"]
        )
        df_out.loc[flter, "start_timestamp"] = df_out.loc[flter, "PitOutTime"]
        df_out.loc[flter, "start_location"] = "pit_exit"
        df_out.loc[~flter, "start_timestamp"] = df_out.loc[~flter, "LapStartTime"]
        df_out.loc[~flter, "start_location"] = "track"

        flter = df_out["PitInTime"].notna() & (df_out["PitInTime"] < df_out["Time"])
        df_out.loc[flter, "end_timestamp"] = df_out.loc[
            flter, ["PitInTime", "Time"]
        ].min(axis=1)
        df_out.loc[flter, "end_location"] = "pit_entry"
        df_out.loc[~flter, "end_timestamp"] = df_out.loc[~flter, "Time"]
        df_out.loc[~flter, "end_location"] = "track"

        cols_output = [
            "year",
            "round_number",
            "session_type",
            "DriverNumber",
            "LapNumber",
            "IsAccurate",
            "stint_number",
            "lap_number_in_stint",
            "start_timestamp",
            "start_location",
            "end_timestamp",
            "end_location",
            "t0_date",
            "session_start_time",
        ]
        dfls_stints = [
            df_out[df_out["stint_number"] == st][cols_output].assign(event_type="lap")
            for st in sorted(df_out["stint_number"].unique())
        ]
        dfls_pits = [
            stA.iloc[-1:].assign(
                event_type="pit",
                LapNumber=np.nan,
                stint_number=np.nan,
                lap_number_in_stint=np.nan,
                start_timestamp=stA["end_timestamp"].max(),
                start_location="pit_entry",
                end_timestamp=stB["start_timestamp"].min(),
                end_location="pit_exit",
                IsAccurate=False,
            )
            for stA, stB in zip(dfls_stints, dfls_stints[1:])
        ]
        df_out = (
            pd.concat(dfls_stints + dfls_pits)
            .sort_values(by=["start_timestamp"])
            .reset_index(drop=True)
        )
        df_out["elapsed_time"] = df_out["end_timestamp"] - df_out["start_timestamp"]
        df_out.index.name = "event_index"
        return df_out


class RefinedDrivers(TrustedToRefined):
    def __init__(self, year):
        self.year = year
        self.dataset_name = f"Drivers_Y{year}"

    def transform(self):
        df_out = self.read_from_trusted(f"Drivers_Y{self.year}").rename(
            columns={
                "year": "weekend_year",
                "round_number": "weekend_index",
                "session_type": "session_index",
                "DriverNumber": "driver_id",
                "BroadcastName": "broadcast_name",
                "Abbreviation": "abbreviation",
                "TeamName": "team_name",
                "TeamColor": "team_color",
                "FirstName": "first_name",
                "LastName": "last_name",
                "FullName": "full_name",
            }
        )
        integer_columns = [
            "weekend_year",
            "weekend_index",
            "session_index",
            "driver_id",
        ]
        for col in integer_columns:
            df_out[col] = df_out[col].astype(pd.Int64Dtype())
        df_out = df_out[
            [
                "weekend_year",
                "weekend_index",
                "session_index",
                "driver_id",
                "abbreviation",
                "broadcast_name",
                "first_name",
                "last_name",
                "full_name",
                "team_name",
                "team_color",
            ]
        ]
        return df_out


class RefinedRaceControl(TrustedToRefined):
    def __init__(self, year):
        self.year = year
        self.dataset_name = f"RaceControl_Y{year}"

    def transform(self):
        df_out = (
            self.read_from_trusted(f"RaceControl_Y{self.year}")
            .assign(ts_reference=lambda df: df["t0_date"].dt.tz_localize("utc"))
            .assign(
                session_reference_time=lambda df: df["ts_reference"],
                session_start_time=lambda df: df["ts_reference"]
                + df["session_start_time"],
                timestamp=lambda df: df["Time"].dt.tz_localize("utc"),
            )
            .rename(
                columns={
                    "year": "weekend_year",
                    "round_number": "weekend_index",
                    "session_type": "session_index",
                    "Category": "category",
                    "Message": "message",
                    "Status": "status",
                    "Flag": "flag",
                    "Scope": "scope",
                    "Sector": "sector",
                    "RacingNumber": "driver_id",
                }
            )
        )
        integer_columns = [
            "weekend_year",
            "weekend_index",
            "session_index",
            "driver_id",
        ]
        for col in integer_columns:
            df_out[col] = df_out[col].astype(pd.Int64Dtype())
        df_out = df_out[
            [
                "weekend_year",
                "weekend_index",
                "session_index",
                "session_reference_time",
                "session_start_time",
                "timestamp",
                "category",
                "message",
                "status",
                "flag",
                "scope",
                "sector",
                "driver_id",
            ]
        ]
        return df_out


class RefinedSessionStatus(TrustedToRefined):
    def __init__(self, year):
        self.year = year
        self.dataset_name = f"Status_Y{year}"

    def transform(self):
        df_out = (
            self.read_from_trusted(f"Status_Y{self.year}")
            .assign(ts_reference=lambda df: df["t0_date"].dt.tz_localize("utc"))
            .assign(
                session_reference_time=lambda df: df["ts_reference"],
                session_start_time=lambda df: df["ts_reference"]
                + df["session_start_time"],
                timestamp=lambda df: df["ts_reference"] + df["Time"],
            )
            .rename(
                columns={
                    "year": "weekend_year",
                    "round_number": "weekend_index",
                    "session_type": "session_index",
                    "Status": "status",
                }
            )
        )
        integer_columns = [
            "weekend_year",
            "weekend_index",
            "session_index",
        ]
        for col in integer_columns:
            df_out[col] = df_out[col].astype(pd.Int64Dtype())
        df_out = df_out[
            [
                "weekend_year",
                "weekend_index",
                "session_index",
                "session_reference_time",
                "session_start_time",
                "timestamp",
                "status",
            ]
        ]
        return df_out


class RefinedSessionWeather(TrustedToRefined):
    def __init__(self, year):
        self.year = year
        self.dataset_name = f"Weather_Y{year}"

    def transform(self):
        df_out = (
            self.read_from_trusted(f"Weather_Y{self.year}")
            .assign(ts_reference=lambda df: df["t0_date"].dt.tz_localize("utc"))
            .assign(
                session_reference_time=lambda df: df["ts_reference"],
                session_start_time=lambda df: df["ts_reference"]
                + df["session_start_time"],
                timestamp=lambda df: df["ts_reference"] + df["Time"],
            )
            .rename(
                columns={
                    "year": "weekend_year",
                    "round_number": "weekend_index",
                    "session_type": "session_index",
                    "AirTemp": "air_temperature",
                    "Humidity": "humidity",
                    "Pressure": "pressure",
                    "Rainfall": "rainfall",
                    "TrackTemp": "track_temperature",
                    "WindDirection": "wind_direction",
                    "WindSpeed": "wind_speed",
                }
            )
        )
        integer_columns = [
            "weekend_year",
            "weekend_index",
            "session_index",
        ]
        for col in integer_columns:
            df_out[col] = df_out[col].astype(pd.Int64Dtype())
        df_out = df_out[
            [
                "weekend_year",
                "weekend_index",
                "session_index",
                "session_reference_time",
                "session_start_time",
                "timestamp",
                "air_temperature",
                "track_temperature",
                "wind_speed",
                "wind_direction",
                "humidity",
                "pressure",
                "rainfall",
            ]
        ]
        return df_out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lap_timing", action="store_true")
    parser.add_argument("--drivers", action="store_true")
    parser.add_argument("--race_control", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--weather", action="store_true")
    parser.add_argument("--year", type=int)
    args = parser.parse_args()
    if args.lap_timing:
        RefinedTiming(args.year).run()
    if args.drivers:
        RefinedDrivers(args.year).run()
    if args.race_control:
        RefinedRaceControl(args.year).run()
    if args.status:
        RefinedSessionStatus(args.year).run()
    if args.weather:
        RefinedSessionWeather(args.year).run()


if __name__ == "__main__":
    main()
