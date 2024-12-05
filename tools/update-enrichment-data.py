import requests

from typing import List


TOR_NODES_URL = "https://www.dan.me.uk/torlist/"
TOR_EXIT_NODES_URL = "https://www.dan.me.uk/torlist/?exit"
MAXMINDDB_ASN_URL = (
    "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-ASN.mmdb"
)
MAXMINDDB_CITY_URL = (
    "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb"
)
MAXMINDDB_COUNTRY_URL = (
    "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb"
)


def get_geo_ip_file():
    pass


def get_asn_ip_file():
    pass


def get_tor_nodes_file() -> List[str]:
    ip_list = []
    r = requests.get(TOR_NODES_URL)

    if r.status_code == 200:
        ip_list = [i for i in r.text.split("\n")]

    return ip_list


def get_tor_exit_nodes_file() -> List[str]:
    ip_list = []
    r = requests.get(TOR_EXIT_NODES_URL)

    if r.status_code == 200:
        ip_list = [i for i in r.text.split("\n")]

    return ip_list


def main():
    print(get_tor_exit_nodes_file())


if __name__ == "__main__":
    main()
