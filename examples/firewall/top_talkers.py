"""
Analyze top talkers in firewall logs - identify IPs with most traffic.
"""
from log_processing.analysis import firewall
from log_processing.source.firewall import fortinet
from log_processing.enrichment import ip


def main():
    # Load firewall logs
    df = fortinet.open_log("data/disk-traffic-forward-2025-11-18_15-05_5.log")
    
    # Analyze top external destinations by bytes (internal -> external traffic)
    print("Top 20 external destinations by traffic volume (internal -> external):")
    top_talkers = firewall.top_talkers(df, top_n=20, by="bytes", focus_external=True)
    
    # Enrich the results with IP information (after grouping, only public IPs remain)
    top_talkers = ip.city_information(top_talkers, ip_column="id.resp_h")
    top_talkers = ip.country_information(top_talkers, ip_column="id.resp_h")
    top_talkers = ip.asn_information(top_talkers, ip_column="id.resp_h")
    print(top_talkers)
    
    # Analyze top external destinations by connections
    print("\nTop 20 external destinations by connection count:")
    top_connections = firewall.top_talkers(df, top_n=20, by="connections", focus_external=True)
    top_connections = ip.city_information(top_connections, ip_column="id.resp_h")
    top_connections = ip.country_information(top_connections, ip_column="id.resp_h")
    top_connections = ip.asn_information(top_connections, ip_column="id.resp_h")
    print(top_connections)
    
    # Analyze top external destinations by sessions
    print("\nTop 20 external destinations by session count:")
    top_sessions = firewall.top_talkers(df, top_n=20, by="sessions", focus_external=True)
    top_sessions = ip.city_information(top_sessions, ip_column="id.resp_h")
    top_sessions = ip.country_information(top_sessions, ip_column="id.resp_h")
    top_sessions = ip.asn_information(top_sessions, ip_column="id.resp_h")
    print(top_sessions)


if __name__ == "__main__":
    main()

