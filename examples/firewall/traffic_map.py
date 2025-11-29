import polars as pl
from log_processing.source.firewall import iptables
from log_processing.enrichment import ip
from log_processing.visualization import map


def main():
    # source
    firewall_df = iptables.open_log("data/disk-traffic-forward-2025-11-18_15-05_5.log")

    # enrichment
    firewall_df = ip.city_information(firewall_df)
    firewall_df = ip.country_information(firewall_df)
    firewall_df = ip.asn_information(firewall_df)
    firewall_df = ip.location_information(firewall_df)

    # Filter out rows without location data for visualization
    firewall_df = firewall_df.filter(
        (pl.col("latitude").is_not_null()) & (pl.col("longitude").is_not_null())
    )

    # visualization
    m = map.points(firewall_df)
    map.add_line(firewall_df, m)
    map.open_in_browser(m)


if __name__ == "__main__":
    main()

