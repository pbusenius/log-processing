from log_processing.source.firewall import fortinet
from log_processing.enrichment import ip
from log_processing.visualization import timeline


def main():
    # source
    firewall_df = fortinet.open_log("data/disk-traffic-forward-2025-11-18_15-05_5.log")

    # enrichment
    firewall_df = ip.city_information(firewall_df)
    firewall_df = ip.country_information(firewall_df)
    firewall_df = ip.asn_information(firewall_df)
    firewall_df = ip.location_information(firewall_df)

    # visualization
    timeline.plot_conn_transfer_over_time(firewall_df, "firewall_traffic_timeline.png")


if __name__ == "__main__":
    main()

