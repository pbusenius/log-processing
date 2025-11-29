import re
import polars as pl


# Regex pattern for Fortinet firewall logs in key-value format
# Example: date=2025-11-13 time=15:05:05 srcip=192.168.108.24 srcport=65194 dstip=8.8.8.8 dstport=53 proto=17 action="accept"
# This pattern extracts key-value pairs from the log line
def parse_key_value_line(line: str) -> dict:
    """Parse a key-value formatted log line into a dictionary."""
    result = {}
    # Match key=value or key="value" patterns
    pattern = r'(\w+)=(?:"([^"]*)"|(\S+))'
    matches = re.finditer(pattern, line)
    for match in matches:
        key = match.group(1)
        # Group 2 is quoted value, group 3 is unquoted value
        value = match.group(2) if match.group(2) is not None else match.group(3)
        result[key] = value
    return result


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to appropriate data types."""
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
        pl.col("id.orig_h").cast(pl.String),
        pl.col("id.resp_h").cast(pl.String),
        pl.col("id.resp_p").cast(pl.String),
        pl.col("orig_ip_bytes").cast(pl.Int64),
        pl.col("resp_ip_bytes").cast(pl.Int64),
    )


def open_log(file: str) -> pl.DataFrame:
    """
    Open and parse Fortinet firewall log file.
    
    Extracts all available fields from the log file and maps to unified schema:
    - srcip -> id.orig_h (source IP)
    - dstip -> id.resp_h (destination IP)
    - dstport -> id.resp_p (destination port)
    - date + time -> ts (timestamp)
    - sentbyte -> orig_ip_bytes (bytes sent from origin)
    - rcvdbyte -> resp_ip_bytes (bytes received by responder)
    - All other fields are preserved with their original names
    """
    rows = []

    with open(file, "r") as f:
        for line in f:
            parsed = parse_key_value_line(line)
            
            # Extract required fields for unified schema
            date = parsed.get("date", "")
            time = parsed.get("time", "")
            srcip = parsed.get("srcip", "")
            dstip = parsed.get("dstip", "")
            
            # Skip if missing essential fields
            if not srcip or not dstip:
                continue
            
            # Combine date and time for timestamp
            if date and time:
                ts = f"{date} {time}"
            else:
                continue
            
            # Build row with unified schema fields first
            row = {
                "ts": ts,
                "id.orig_h": srcip,
                "id.resp_h": dstip,
                "id.resp_p": parsed.get("dstport", ""),
                "proto": parsed.get("proto", ""),
                "action": parsed.get("action", ""),
            }
            
            # Convert bytes to int, default to 0 if missing or invalid
            try:
                row["orig_ip_bytes"] = int(parsed.get("sentbyte", "0") or "0")
            except (ValueError, TypeError):
                row["orig_ip_bytes"] = 0
            try:
                row["resp_ip_bytes"] = int(parsed.get("rcvdbyte", "0") or "0")
            except (ValueError, TypeError):
                row["resp_ip_bytes"] = 0
            
            # Add all other fields from the log (excluding already processed ones)
            skip_fields = {"date", "time", "srcip", "dstip", "dstport", "proto", "action", "sentbyte", "rcvdbyte"}
            for key, value in parsed.items():
                if key not in skip_fields:
                    row[key] = value
            
            rows.append(row)

    if not rows:
        # Return empty dataframe with at least the unified schema columns
        return pl.DataFrame({
            "ts": [],
            "id.orig_h": [],
            "id.resp_h": [],
            "id.resp_p": [],
            "proto": [],
            "action": [],
            "orig_ip_bytes": [],
            "resp_ip_bytes": [],
        })
    
    df = pl.DataFrame(rows)
    
    # Filter out rows with empty IPs (failed parsing)
    df = df.filter(
        (pl.col("id.orig_h") != "") & (pl.col("id.resp_h") != "")
    )
    
    # Sort by timestamp
    df = df.sort("ts")
    
    df = cast_columns(df)

    return df

