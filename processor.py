import argparse
import zipfile
import polars as pl

from src.analysis import ssh 
from src.source.zeek import ssh as ssh_source

parser = argparse.ArgumentParser(
    "Log-Processing", description="Processing of collected log-files"
)


parser.add_argument("-f", "--file")


def open_zip(file: str, target: str):
    with zipfile.ZipFile(file, "r") as zip_ref:
        zip_ref.extractall(target)


def main():
    args = parser.parse_args()

    # source
    df = ssh_source.open_log(args.file)

    # analysis
    df_brute_force = ssh.brute_force_detection(df)

    print(df_brute_force)


if __name__ == "__main__":
    main()
