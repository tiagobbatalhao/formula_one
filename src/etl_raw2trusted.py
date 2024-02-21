from pathlib import Path
import pandas as pd
import argparse


class RawToTrusted:
    raw_folder = Path("~/localdatalake/formula_one").expanduser() / "data" / "raw"
    trusted_folder = (
        Path("~/localdatalake/formula_one").expanduser() / "data" / "trusted"
    )

    def run(self):
        self.raw_folder.mkdir(parents=True, exist_ok=True)
        self.trusted_folder.mkdir(parents=True, exist_ok=True)
        path = self.trusted_folder / (self.prefix + ".parquet")
        file_list = self.raw_folder.glob(self.prefix + "*.parquet")
        df_save = pd.concat([pd.read_parquet(fl) for fl in file_list])
        df_save = self.transform(df_save)
        df_save.to_parquet(path)

    def transform(self, df):
        return df


class RawToTrusted_Schedule(RawToTrusted):
    def __init__(self):
        self.prefix = f"Schedule"

    def transform(self, df):
        pkey = ["year", "RoundNumber"]
        for col in pkey:
            df[col] = df[col].astype(int)
        return df.sort_values(by=pkey)


class RawToTrusted_Drivers(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Drivers_Y{year}"


class RawToTrusted_Laps(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Laps_Y{year}"


class RawToTrusted_Weather(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Weather_Y{year}"


class RawToTrusted_RaceControl(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"RaceControl_Y{year}"


class RawToTrusted_Results(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Results_Y{year}"


class RawToTrusted_Status(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Status_Y{year}"


class RawToTrusted_Telemetry(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Telemetry_Y{year}"


class RawToTrusted_Position(RawToTrusted):
    def __init__(self, year):
        self.prefix = f"Position_Y{year}"


def etl_concat(year: int):
    classes_ = [
        RawToTrusted_Drivers,
        RawToTrusted_Laps,
        RawToTrusted_Weather,
        RawToTrusted_RaceControl,
        RawToTrusted_Results,
        RawToTrusted_Status,
        RawToTrusted_Telemetry,
        RawToTrusted_Position,
    ]
    RawToTrusted_Schedule().run()
    for cls_ in classes_:
        cls_(year=year).run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concat", action="store_true")
    parser.add_argument("--year", type=int)
    args = parser.parse_args()
    if args.concat:
        etl_concat(args.year)


if __name__ == "__main__":
    main()
