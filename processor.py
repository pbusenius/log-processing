import argparse
import zipfile

from src.analysis import ssh
from src.source.os import ssh as ssh_os_source
from src.source.zeek import ssh as ssh_zeek_source

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
    df = ssh_zeek_source.open_log(args.file)
    print(df)

    os_df = ssh_os_source.open_log("data/auth.log")
    print(os_df)

    # analysis
    df_brute_force = ssh.brute_force_detection(df)
    print(df_brute_force)

    os_df_brute_force = ssh.brute_force_detection(os_df)
    print(os_df_brute_force)


if __name__ == "__main__":
    main()
