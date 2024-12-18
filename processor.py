import argparse
import zipfile

from src.analysis import ssh
from src.source.os import ssh as ssh_os_source
from src.source.os import http as http_os_source
from src.source.zeek import ssh as ssh_zeek_source
from src.source.velociraptor import ssh as ssh_velociraptor_source
from src.enrichment import ip
from src.visualization import map
from src.export import timesketch

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
    # zeek_df = ssh_zeek_source.open_log(args.file)
    # os_df = ssh_os_source.open_log("data/auth.log")
    # velo_df = ssh_velociraptor_source.open_log("data/auth_velociraptor.log")
    http_df = http_os_source.open_log(args.file)

    # print(zeek_df)
    # print(os_df)
    # print(velo_df)
    print(http_df)

    # analysis
    # df_brute_force = ssh.brute_force_detection(zeek_df)

    # enrichment
    # df_brute_force = ip.city_information(df_brute_force)
    # df_brute_force = ip.country_information(df_brute_force)
    # df_brute_force = ip.asn_information(df_brute_force)
    # df_brute_force = ip.location_information(df_brute_force)

    # print(df_brute_force)

    # visualization
    # map.points(df_brute_force)

    # export
    # timesketch.as_json(df_brute_force, "brute_force.jsonl")
    # timesketch.as_csv(df_brute_force, "brute_force.csv")


if __name__ == "__main__":
    main()
