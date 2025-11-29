"""
Analyze firewall policy usage and effectiveness.
"""
import polars as pl
from log_processing.analysis import firewall
from log_processing.source.firewall import fortinet


def main():
    # Load firewall logs
    df = fortinet.open_log("data/disk-traffic-forward-2025-11-18_15-05_5.log")
    
    # Policy analysis
    print("Firewall policy usage statistics:")
    policies = firewall.policy_analysis(df)
    print(f"\nTotal policies in use: {len(policies)}")
    print(policies)
    
    # Summary statistics
    print("\n\nPolicy Summary:")
    print(f"Total connections: {policies['connection_count'].sum()}")
    print(f"Total bytes: {policies['total_bytes'].sum():,}")
    
    # Most used policies
    print("\n\nTop 10 most used policies:")
    print(policies.head(10))
    
    # Denied connections by policy
    if "action" in policies.columns:
        denied = policies.filter(pl.col("action").is_in(["deny", "block", "drop"]))
        if len(denied) > 0:
            print("\n\nDenied connections by policy:")
            print(denied.sort("connection_count", descending=True))


if __name__ == "__main__":
    import polars as pl
    main()

