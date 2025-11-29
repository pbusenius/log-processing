from log_processing.analysis import ssh
from log_processing.source.zeek import ssh as ssh_zeek_source
from log_processing.enrichment import ip
from log_processing.visualization import map


def main():
    # source
    zeek_df = ssh_zeek_source.open_log("data/ssh.log")

    # analysis
    df_brute_force = ssh.brute_force_detection(zeek_df)

    # enrichment
    df_brute_force = ip.city_information(df_brute_force)
    df_brute_force = ip.country_information(df_brute_force)
    df_brute_force = ip.asn_information(df_brute_force)
    df_brute_force = ip.location_information(df_brute_force)

    # visualization
    m = map.points(df_brute_force)
    map.add_line(df_brute_force, m)
    map.open_in_browser(m)


if __name__ == "__main__":
    main()
