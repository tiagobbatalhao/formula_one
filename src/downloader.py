import fastf1
from pathlib import Path
import pandas as pd
from datetime import datetime
import argparse
from loguru import logger


class FastF1Downloader:
    data_folder = Path("~/localdatalake/formula_one").expanduser() / "data" / "raw"
    cache_folder = Path("~/localdatalake/formula_one").expanduser() / "cache"

    def run(self):
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.cache_folder.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(self.cache_folder)
        path = self.data_folder / (self.get_filename() + ".parquet")
        if not path.exists():
            df = self.download()
            df_save = self.add_input_info(pd.DataFrame(df))
            df_save["created_at"] = datetime.utcnow()
            df_save.to_parquet(path)
        return self


class YearSchedule(FastF1Downloader):
    def __init__(self, year):
        self.year = year

    def download(self):
        schedule = fastf1.get_event_schedule(self.year)
        schedule["is_testing"] = schedule.is_testing()
        return schedule

    def add_input_info(self, df):
        df["year"] = self.year
        return df

    def get_filename(self):
        return "Schedule_Y{:04d}".format(int(self.year))


class SessionDownloader(FastF1Downloader):
    def __init__(self, year, round_number, session_type):
        self.year = year
        self.round_number = round_number
        self.session_type = session_type

    def add_input_info(self, df):
        df["year"] = self.year
        df["round_number"] = self.round_number
        df["session_type"] = self.session_type
        df["session_start_time"] = self.session.session_start_time
        df["t0_date"] = self.session.t0_date
        return df

    def get_filename(self, base_name):
        return "{}_Y{:04d}R{:02d}S{:s}".format(
            base_name,
            self.year,
            self.round_number,
            str(self.session_type),
        )

    def download(self):
        self.session = fastf1.get_session(
            self.year, self.round_number, self.session_type
        )
        self.session.load()
        return self.session


class SessionDriversDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Drivers")

    def download(self):
        session = super().download()
        info = [pd.DataFrame(session.get_driver(dr)) for dr in session.drivers]
        df = pd.concat(info, axis=1).T
        return df


class SessionLapsDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Laps")

    def download(self):
        session = super().download()
        df = pd.DataFrame(session.laps)
        return df


class SessionWeatherDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Weather")

    def download(self):
        session = super().download()
        df = pd.DataFrame(session.weather_data)
        return df


class SessionRaceControlDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("RaceControl")

    def download(self):
        session = super().download()
        df = pd.DataFrame(session.race_control_messages)
        return df


class SessionResultsDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Results")

    def download(self):
        session = super().download()
        df = pd.DataFrame(session.results)
        return df


class SessionStatusDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Status")

    def download(self):
        session = super().download()
        df = pd.DataFrame(session.session_status)
        return df


class SessionTelemetryDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Telemetry")

    def download(self):
        session = super().download()
        dfs = []
        for dr in session.drivers:
            try:
                this = self._get_by_driver(session, dr)
                if this is None:
                    continue
                this = this.assign(DriverNumber=dr)
            except (KeyError, ValueError) as e:
                continue
            dfs.append(this)
        df = pd.concat(dfs)
        return df

    def _get_by_driver(self, session, driver_number):
        dfls = []
        for _, lap in session.laps.pick_driver(driver_number).iterlaps():
            df = lap.get_car_data().add_differential_distance()
            df["LapNumber"] = lap["LapNumber"]
            dfls.append(df)
        if len(dfls) > 0:
            df = pd.concat(dfls).sort_values(by=["Date"])
            return df


class SessionPositionDownloader(SessionDownloader):
    def get_filename(self):
        return super().get_filename("Position")

    def download(self):
        session = super().download()
        dfs = []
        for dr in session.drivers:
            try:
                this = self._get_by_driver(session, dr)
                if this is None:
                    continue
                this = this.assign(DriverNumber=dr)
            except (KeyError, ValueError) as e:
                continue
            dfs.append(this)
        df = pd.concat(dfs)
        return df

    def _get_by_driver(self, session, driver_number):
        dfls = []
        for _, lap in session.laps.pick_driver(driver_number).iterlaps():
            df = lap.get_pos_data()
            df["LapNumber"] = lap["LapNumber"]
            dfls.append(df)
        if len(dfls) > 0:
            df = pd.concat(dfls).sort_values(by=["Date"])
            return df


def download_schedule(year: int):
    YearSchedule(year=year).run()


def download_weekend(year: int, round_number: int):
    classes_ = [
        SessionDriversDownloader,
        SessionLapsDownloader,
        SessionWeatherDownloader,
        SessionRaceControlDownloader,
        SessionResultsDownloader,
        SessionStatusDownloader,
        SessionTelemetryDownloader,
        SessionPositionDownloader,
    ]
    for session_index in range(1, 6):
        for cls_ in classes_:
            try:
                cls_(
                    year=year,
                    round_number=round_number,
                    session_type=session_index,
                ).run()
            except ValueError as e:
                msg = (
                    "Issue with (year={}, round_number={}, session_index={}: {}".format(
                        year, round_number, session_index, e
                    )
                )
                logger.error(msg)
                continue


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", action="store_true")
    parser.add_argument("--weekend", action="store_true")
    parser.add_argument("--year", type=int)
    parser.add_argument("--round_number", type=int)
    args = parser.parse_args()
    if args.schedule:
        download_schedule(args.year)
    elif args.weekend:
        if isinstance(args.round_number, int):
            download_weekend(args.year, args.round_number)
        else:
            for round_number in range(1, 30):
                try:
                    download_weekend(args.year, round_number)
                except ValueError:
                    continue


if __name__ == "__main__":
    main()
