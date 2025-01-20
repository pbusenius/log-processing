import argparse
import zipfile

from src.analysis import ssh
from src.analysis import http
from src.analysis import login
from src.source.os import ssh as ssh_os_source
from src.source.os import http as http_os_source
from src.source.zeek import ssh as ssh_zeek_source
from src.source.zeek import http as http_zeek_source
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
    zeek_df = ssh_zeek_source.open_log("data/ssh.log")
    # os_df = ssh_os_source.open_log("data/auth.log")
    # velo_df = ssh_velociraptor_source.open_log("data/auth_velociraptor.log")
    # http_df = http_os_source.open_log("data/access.log")
    # http_df = http_zeek_source.open_log("data/http.log")

    print(zeek_df)
    # print(os_df)
    # print(velo_df)
    # print(http_df)

    # print(http.get_periodic_connection_to_host(http_df))
    # print(http.get_periodic_connection_from_host(http_df))

    # analysis
    # df_brute_force = ssh.brute_force_detection(zeek_df)
    # df_common_domain = http.get_common_domains(http_df)
    # df_uncommon_domain = http.get_uncommon_domains(http_df)

    df = login.detect_impossible_travel(zeek_df, 100)
    print(df)

    # print(df_common_domain)
    # print(df_uncommon_domain)

    # enrichment
    # df_brute_force = ip.city_information(df_brute_force)
    # df_brute_force = ip.country_information(df_brute_force)
    # df_brute_force = ip.asn_information(df_brute_force)
    # df_brute_force = ip.location_information(df_brute_force)

    # print(df_brute_force)

    # visualization
    # m = map.points(df_brute_force)
    # map.add_line(df_brute_force, m)
    # map.open_in_browser(m)

    # export
    # timesketch.as_json(df_brute_force, "brute_force.jsonl")
    # timesketch.as_csv(df_brute_force, "brute_force.csv")


if __name__ == "__main__":
    main()
