"""
Detect potential port scanning activity in firewall logs.
"""
from log_processing.analysis import firewall
from log_processing.source.firewall import fortinet
from log_processing.enrichment import ip


def main():
    # Load firewall logs
    df = fortinet.open_log("data/disk-traffic-forward-2025-11-18_15-05_5.log")
    
    # Enrich with IP information
    df = ip.city_information(df, ip_column="id.orig_h")
    df = ip.country_information(df, ip_column="id.orig_h")
    df = ip.asn_information(df, ip_column="id.orig_h")
    
    # Detect port scanning (focus on external -> internal scans hitting default ports)
    print("Potential port scanning activity (external -> internal, >= 10 ports, >= 1 default port per hour):")
    port_scans = firewall.port_scan_detection(
        df, 
        min_ports=10, 
        time_window="1h", 
        focus_external=True,
        min_default_ports=1  # Require at least 1 default port to reduce false positives
    )
    
    if len(port_scans) > 0:
        print(f"\nFound {len(port_scans)} potential port scanning events:")
        print(port_scans)
        
        # Show details for first few
        print("\nDetailed view of first 5 port scans:")
        for row in port_scans.head(5).iter_rows(named=True):
            print(f"\nSource IP: {row['id.orig_h']}")
            print(f"  Time window: {row['ts']}")
            print(f"  Unique ports scanned: {row['unique_ports']}")
            print(f"  Default ports scanned: {row['default_ports_scanned']}")
            print(f"  Default ports: {row['default_ports_list']}")
            print(f"  Unique destinations: {row['unique_destinations']}")
            print(f"  Total connections: {row['connection_count']}")
            print(f"  All ports: {row['ports_scanned'][:20]}...")  # Show first 20 ports
    else:
        print("No port scanning activity detected.")


if __name__ == "__main__":
    main()

