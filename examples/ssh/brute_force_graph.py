from log_processing.analysis import ssh
from log_processing.source.zeek import ssh as ssh_zeek_source
from log_processing.enrichment import ip
from log_processing.visualization import graph


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
    graph.plot_network(df_brute_force)


if __name__ == "__main__":
    main()
