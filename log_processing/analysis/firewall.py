import polars as pl


def top_talkers(
    df: pl.DataFrame, 
    top_n: int = 20,
    by: str = "bytes",  # "bytes", "connections", or "sessions"
    use_nat_ip: bool = False,  # Use translated IP (transip) for outbound traffic
    focus_external: bool = True  # Focus on internal -> external traffic
) -> pl.DataFrame:
    """
    Identify top talkers by traffic volume, connection count, or session count.
    
    Args:
        df: Firewall log DataFrame
        top_n: Number of top talkers to return
        by: Metric to rank by ("bytes", "connections", or "sessions")
        use_nat_ip: If True, use transip (NAT'd IP) for outbound traffic instead of internal IP
        focus_external: If True, only analyze internal -> external traffic (lan -> wan)
                       and group by destination IP (external IPs receiving traffic)
    
    Returns:
        DataFrame with top talkers and their statistics
    """
    # Filter for internal -> external traffic if requested
    if focus_external and "srcintfrole" in df.columns and "dstintfrole" in df.columns:
        df_filtered = df.filter(
            (pl.col("srcintfrole") == "lan") & (pl.col("dstintfrole") == "wan")
        )
        # Group by destination IP (external IPs) instead of source IP
        group_col = "id.resp_h"
        source_info_col = "id.orig_h"  # Keep source for reference
    else:
        df_filtered = df
        # Create effective source IP column (use transip if available and use_nat_ip is True)
        if use_nat_ip and "transip" in df.columns:
            df_filtered = df_filtered.with_columns(
                pl.when(pl.col("transip").is_not_null())
                .then(pl.col("transip"))
                .otherwise(pl.col("id.orig_h"))
                .alias("effective_source_ip")
            )
            group_col = "effective_source_ip"
            source_info_col = "id.orig_h"
        else:
            group_col = "id.orig_h"
            source_info_col = None
    
    if by == "bytes":
        agg_cols = [
            pl.sum("orig_ip_bytes").alias("total_sent_bytes"),
            pl.sum("resp_ip_bytes").alias("total_received_bytes"),
            (pl.sum("orig_ip_bytes") + pl.sum("resp_ip_bytes")).alias("total_bytes"),
            pl.len().alias("connection_count"),
            pl.n_unique("sessionid").alias("session_count"),
            pl.col("id.resp_p").n_unique().alias("unique_dest_ports"),
        ]
        if focus_external:
            agg_cols.extend([
                pl.n_unique(source_info_col).alias("unique_sources"),
                pl.col(source_info_col).unique().head(10).alias("top_sources"),  # Top 10 internal IPs talking to this external IP
            ])
        else:
            agg_cols.append(pl.col("id.resp_h").n_unique().alias("unique_destinations"))
            if source_info_col:
                agg_cols.append(pl.col(source_info_col).first().alias("original_source_ip"))
        
        df_result = (
            df_filtered.group_by(group_col)
            .agg(agg_cols)
            .sort("total_bytes", descending=True)
            .head(top_n)
        )
    elif by == "connections":
        agg_cols = [
            pl.len().alias("connection_count"),
            (pl.sum("orig_ip_bytes") + pl.sum("resp_ip_bytes")).alias("total_bytes"),
            pl.n_unique("sessionid").alias("session_count"),
            pl.col("id.resp_p").n_unique().alias("unique_dest_ports"),
        ]
        if focus_external:
            agg_cols.extend([
                pl.n_unique(source_info_col).alias("unique_sources"),
                pl.col(source_info_col).unique().head(10).alias("top_sources"),
            ])
        else:
            agg_cols.append(pl.col("id.resp_h").n_unique().alias("unique_destinations"))
            if source_info_col:
                agg_cols.append(pl.col(source_info_col).first().alias("original_source_ip"))
        
        df_result = (
            df_filtered.group_by(group_col)
            .agg(agg_cols)
            .sort("connection_count", descending=True)
            .head(top_n)
        )
    else:  # by == "sessions"
        agg_cols = [
            pl.n_unique("sessionid").alias("session_count"),
            pl.len().alias("connection_count"),
            (pl.sum("orig_ip_bytes") + pl.sum("resp_ip_bytes")).alias("total_bytes"),
            pl.col("id.resp_p").n_unique().alias("unique_dest_ports"),
        ]
        if focus_external:
            agg_cols.extend([
                pl.n_unique(source_info_col).alias("unique_sources"),
                pl.col(source_info_col).unique().head(10).alias("top_sources"),
            ])
        else:
            agg_cols.append(pl.col("id.resp_h").n_unique().alias("unique_destinations"))
            if source_info_col:
                agg_cols.append(pl.col(source_info_col).first().alias("original_source_ip"))
        
        df_result = (
            df_filtered.group_by(group_col)
            .agg(agg_cols)
            .sort("session_count", descending=True)
            .head(top_n)
        )
    
    return df_result


def port_scan_detection(
    df: pl.DataFrame,
    min_ports: int = 10,
    time_window: str = "1h",
    focus_external: bool = True,  # Focus on external -> internal scans
    min_default_ports: int = 1  # Minimum number of default ports to flag (0 = no requirement)
) -> pl.DataFrame:
    """
    Detect potential port scanning activity, with focus on known default ports.
    
    Identifies source IPs that connect to many different destination ports
    within a time window, which may indicate scanning behavior.
    Prioritizes scans that hit known default/service ports.
    
    Args:
        df: Firewall log DataFrame
        min_ports: Minimum number of unique destination ports to flag
        time_window: Time window for grouping (e.g., "1h", "30m")
        focus_external: If True, only analyze external -> internal scans (wan -> lan)
                       If False, analyze all traffic including internal -> external
        min_default_ports: Minimum number of default ports scanned to flag
    
    Returns:
        DataFrame with potential port scanners, including default port metrics
    """
    # Common default/service ports that attackers typically scan
    DEFAULT_PORTS = {
        "21", "22", "23", "25", "53", "80", "110", "111", "135", "139", "143",
        "443", "445", "993", "995", "1433", "1521", "3306", "3389", "5432",
        "5900", "5985", "5986", "6379", "8080", "8443", "9200", "27017"
    }
    
    # Filter for external -> internal scans if requested
    if focus_external and "srcintfrole" in df.columns and "dstintfrole" in df.columns:
        df_filtered = df.filter(
            (pl.col("srcintfrole") == "wan") & (pl.col("dstintfrole") == "lan")
        )
    else:
        df_filtered = df
    
    # Convert port to string for comparison
    df_with_port_str = df_filtered.with_columns(
        pl.col("id.resp_p").cast(pl.String).alias("resp_p_str")
    )
    
    df_result = (
        df_with_port_str.sort("ts")
        .group_by_dynamic("ts", group_by="id.orig_h", every=time_window)
        .agg(
            pl.col("id.resp_p").n_unique().alias("unique_ports"),
            pl.col("id.resp_h").n_unique().alias("unique_destinations"),
            pl.len().alias("connection_count"),
            pl.col("id.resp_p").unique().sort().alias("ports_scanned"),
            pl.col("id.resp_h").unique().sort().alias("destinations"),
            pl.col("srcintfrole").first().alias("source_interface"),
            pl.col("dstintfrole").first().alias("dest_interface"),
        )
        .with_columns(
            # Count how many default ports were scanned
            pl.col("ports_scanned")
            .map_elements(
                lambda ports: len([p for p in ports if str(p) in DEFAULT_PORTS]),
                return_dtype=pl.Int64
            )
            .alias("default_ports_scanned"),
            # List which default ports were scanned
            pl.col("ports_scanned")
            .map_elements(
                lambda ports: [str(p) for p in ports if str(p) in DEFAULT_PORTS],
                return_dtype=pl.List(pl.String)
            )
            .alias("default_ports_list"),
        )
        .filter(pl.col("unique_ports") >= min_ports)
        .filter(
            (pl.col("default_ports_scanned") >= min_default_ports) if min_default_ports > 0
            else pl.lit(True)
        )
        .sort("default_ports_scanned", "unique_ports", descending=True)
    )
    
    return df_result


def high_risk_connections(
    df: pl.DataFrame,
    min_reputation: int = 3,
    include_denied: bool = True
) -> pl.DataFrame:
    """
    Identify high-risk connections based on reputation scores and denied actions.
    
    Args:
        df: Firewall log DataFrame
        min_reputation: Minimum reputation score to flag (lower = more risky)
        include_denied: Include denied/blocked connections
    
    Returns:
        DataFrame with high-risk connections
    """
    conditions = []
    
    # Filter by reputation if available
    if "dstreputation" in df.columns:
        conditions.append(pl.col("dstreputation").cast(pl.Int64, strict=False) <= min_reputation)
    
    # Include denied connections if requested
    if include_denied and "action" in df.columns:
        conditions.append(pl.col("action").is_in(["deny", "block", "drop"]))
    
    if not conditions:
        return df.limit(0)  # Return empty DataFrame if no conditions
    
    df_result = (
        df.filter(pl.any_horizontal(conditions))
        .sort("ts", descending=True)
        .select([
            "ts",
            "id.orig_h",
            "id.resp_h",
            "id.resp_p",
            "proto",
            "action",
            "dstreputation",
            "policyname",
            "app",
            "apprisk",
            "orig_ip_bytes",
            "resp_ip_bytes",
        ])
    )
    
    return df_result


def application_risk_analysis(
    df: pl.DataFrame,
    min_risk_level: str = "elevated"
) -> pl.DataFrame:
    """
    Analyze application usage and identify high-risk applications.
    
    Args:
        df: Firewall log DataFrame
        min_risk_level: Minimum risk level to include ("low", "medium", "elevated", "high")
    
    Returns:
        DataFrame with application risk statistics
    """
    risk_order = {"low": 1, "medium": 2, "elevated": 3, "high": 4}
    min_risk_value = risk_order.get(min_risk_level.lower(), 3)
    
    def risk_to_value(risk: str) -> int:
        """Convert risk level string to numeric value."""
        if risk is None:
            return 0
        return risk_order.get(risk.lower(), 0)
    
    df_result = (
        df.filter(
            pl.col("app").is_not_null() & 
            pl.col("apprisk").is_not_null()
        )
        .with_columns(
            pl.col("apprisk")
            .str.to_lowercase()
            .map_elements(risk_to_value, return_dtype=pl.Int64)
            .alias("risk_value")
        )
        .filter(pl.col("risk_value") >= min_risk_value)
        .group_by("app", "apprisk", "appcat")
        .agg(
            pl.count().alias("connection_count"),
            (pl.sum("orig_ip_bytes") + pl.sum("resp_ip_bytes")).alias("total_bytes"),
            pl.n_unique("id.orig_h").alias("unique_sources"),
            pl.n_unique("id.resp_h").alias("unique_destinations"),
            pl.col("id.orig_h").unique().head(10).alias("sample_sources"),
        )
        .sort("risk_value", "connection_count", descending=True)
    )
    
    return df_result


def policy_analysis(
    df: pl.DataFrame
) -> pl.DataFrame:
    """
    Analyze firewall policy usage and effectiveness.
    
    Args:
        df: Firewall log DataFrame
    
    Returns:
        DataFrame with policy statistics
    """
    df_result = (
        df.filter(pl.col("policyname").is_not_null())
        .group_by("policyname", "action", "policyid")
        .agg(
            pl.count().alias("connection_count"),
            (pl.sum("orig_ip_bytes") + pl.sum("resp_ip_bytes")).alias("total_bytes"),
            pl.n_unique("id.orig_h").alias("unique_sources"),
            pl.n_unique("id.resp_h").alias("unique_destinations"),
            pl.col("ts").min().alias("first_seen"),
            pl.col("ts").max().alias("last_seen"),
        )
        .sort("connection_count", descending=True)
    )
    
    return df_result


def anomalous_data_transfer(
    df: pl.DataFrame,
    percentile: float = 95.0
) -> pl.DataFrame:
    """
    Identify connections with anomalously high data transfer.
    
    Args:
        df: Firewall log DataFrame
        percentile: Percentile threshold for anomaly detection (default 95th)
    
    Returns:
        DataFrame with anomalous transfers
    """
    # Calculate total bytes per connection
    df_with_total = df.with_columns(
        (pl.col("orig_ip_bytes") + pl.col("resp_ip_bytes")).alias("total_bytes")
    )
    
    # Calculate threshold
    threshold = df_with_total.select(
        pl.col("total_bytes").quantile(percentile / 100.0)
    ).item()
    
    df_result = (
        df_with_total
        .filter(pl.col("total_bytes") >= threshold)
        .sort("total_bytes", descending=True)
        .select([
            "ts",
            "id.orig_h",
            "id.resp_h",
            "id.resp_p",
            "proto",
            "action",
            "total_bytes",
            "orig_ip_bytes",
            "resp_ip_bytes",
            "duration",
            "sessionid",
            "policyname",
            "app",
        ])
    )
    
    return df_result


def geographic_anomalies(
    df: pl.DataFrame,
    exclude_countries: list[str] = None
) -> pl.DataFrame:
    """
    Identify connections to unusual geographic locations.
    
    Requires IP enrichment with country information.
    
    Args:
        df: Firewall log DataFrame (must have country columns from enrichment)
        exclude_countries: List of expected/normal countries to exclude
    
    Returns:
        DataFrame with connections to unusual countries
    """
    if exclude_countries is None:
        exclude_countries = []
    
    # Check if enrichment columns exist
    if "dstcountry" not in df.columns:
        return df.limit(0)  # Return empty if not enriched
    
    df_result = (
        df.filter(
            pl.col("dstcountry").is_not_null() &
            ~pl.col("dstcountry").is_in(exclude_countries)
        )
        .group_by("dstcountry", "dstregion", "dstcity")
        .agg(
            pl.count().alias("connection_count"),
            (pl.sum("orig_ip_bytes") + pl.sum("resp_ip_bytes")).alias("total_bytes"),
            pl.n_unique("id.orig_h").alias("unique_sources"),
            pl.n_unique("id.resp_h").alias("unique_destinations"),
            pl.col("id.orig_h").unique().head(10).alias("sample_sources"),
        )
        .sort("connection_count", descending=True)
    )
    
    return df_result

