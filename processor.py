import argparse
import zipfile
import polars as pl
from zat.log_to_dataframe import LogToDataFrame

from src.analysis import ssh

parser = argparse.ArgumentParser(
    "Log-Processing", description="Processing of collected log-files"
)


parser.add_argument("-f", "--file")


def open_zip(file: str, target: str):
    with zipfile.ZipFile(file, "r") as zip_ref:
        zip_ref.extractall(target)


def main():
    log_to_df = LogToDataFrame()
    args = parser.parse_args()

    df = log_to_df.create_dataframe(args.file).reset_index()

    df = pl.from_pandas(df)

    # df preprocessing
    df = df.sort("ts")
    df = df.drop_nulls("auth_success")

    df_brute_force = ssh.brute_force_detection(df)

    print(df_brute_force)


if __name__ == "__main__":
    main()
