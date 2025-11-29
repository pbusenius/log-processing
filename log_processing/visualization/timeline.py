import matplotlib
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

matplotlib.rc("font", size=6)
plt.style.use("bmh")


BYTE_TO_GIGABYTE_MODIFIER = 1e-9

def plot_data_transfer_over_time(df: pl.DataFrame, out_file: str, y_lim):
    df = df.with_columns(
        pl.col("timestamp").dt.to_string("%d.%m-%H:%M").alias("timestamp_string")
    )

    ax = sns.lineplot(
        df,
        x="timestamp_string",
        y="tx",
    )

    tickMultiplier = round(len(df) / 7)

    ax.xaxis.set_major_locator(ticker.MultipleLocator(tickMultiplier))

    ax.set(ylabel="Datentransfer GB/h", xlabel="Zeitstempel")

    ax.set_ylim(y_lim)

    plt.savefig(out_file, dpi=1000)

    plt.clf()


def plot_conn_transfer_over_time(df: pl.DataFrame, out_file: str):
    """Plot comprehensive timeline visualization with multiple metrics."""
    # Aggregate basic metrics by minute
    df_agg = (
        df.sort("ts")
        .group_by_dynamic("ts", every="1m")
        .agg([
            (pl.col("orig_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER).alias("tx_gb"),
            (pl.col("resp_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER).alias("rx_gb"),
        ])
        .with_columns(
            pl.col("ts").dt.to_string("%d.%m-%H:%M").alias("timestamp_string")
        )
    )

    # Aggregate data transfer by protocol
    df_proto_data = (
        df.sort("ts")
        .group_by_dynamic("ts", every="1m")
        .agg([
            (pl.col("orig_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER).alias("tx_gb"),
            (pl.col("resp_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER).alias("rx_gb"),
        ])
        .with_columns(
            pl.col("ts").dt.to_string("%d.%m-%H:%M").alias("timestamp_string")
        )
    )
    
    # Get protocol breakdown with data transfer
    proto_data_list = []
    for proto in df["proto"].unique().to_list():
        df_proto = df.filter(pl.col("proto") == proto)
        df_proto_agg = (
            df_proto.sort("ts")
            .group_by_dynamic("ts", every="1m")
            .agg([
                (pl.col("orig_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER).alias("tx_gb"),
                (pl.col("resp_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER).alias("rx_gb"),
            ])
            .with_columns(
                pl.col("ts").dt.to_string("%d.%m-%H:%M").alias("timestamp_string"),
                pl.lit(proto).alias("proto")
            )
        )
        proto_data_list.append(df_proto_agg)
    
    df_proto_combined = pl.concat(proto_data_list) if proto_data_list else pl.DataFrame()

    # Create figure with subplots
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    fig.suptitle("Firewall Traffic Analysis Over Time", fontsize=14, fontweight="bold", y=0.995)
    fig.subplots_adjust(top=0.94, hspace=0.4)

    tickMultiplier = max(1, round(len(df_agg) / 7))
    df_agg_pd = df_agg.to_pandas()

    # Plot 1: Data Transfer (TX/RX)
    ax1 = axes[0]
    sns.lineplot(
        data=df_agg_pd,
        x="timestamp_string",
        y="tx_gb",
        label="TX (Outbound)",
        ax=ax1,
        color="blue",
    )
    sns.lineplot(
        data=df_agg_pd,
        x="timestamp_string",
        y="rx_gb",
        label="RX (Inbound)",
        ax=ax1,
        color="red",
    )
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(tickMultiplier))
    ax1.set(ylabel="Data Transfer (GB/min)", xlabel="")
    ax1.set_xlim(left=-0.5)  # Start from left edge
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Data Transfer Volume", fontsize=10)

    # Plot 2: Data Transfer by Protocol
    ax2 = axes[1]
    if not df_proto_combined.is_empty():
        # Protocol number to name mapping
        proto_names = {
            "1": "ICMP",
            "6": "TCP",
            "17": "UDP",
            "47": "GRE",
            "50": "ESP",
            "51": "AH",
        }
        
        df_proto_pd = df_proto_combined.to_pandas()
        
        # Plot TX (outbound) by protocol
        for proto in df_proto_pd["proto"].unique():
            proto_df = df_proto_pd[df_proto_pd["proto"] == proto]
            proto_name = proto_names.get(str(proto), f"Proto {proto}")
            sns.lineplot(
                data=proto_df,
                x="timestamp_string",
                y="tx_gb",
                label=f"{proto_name} (TX)",
                ax=ax2,
            )
        
        # Plot RX (inbound) by protocol with dashed lines
        for proto in df_proto_pd["proto"].unique():
            proto_df = df_proto_pd[df_proto_pd["proto"] == proto]
            proto_name = proto_names.get(str(proto), f"Proto {proto}")
            sns.lineplot(
                data=proto_df,
                x="timestamp_string",
                y="rx_gb",
                label=f"{proto_name} (RX)",
                ax=ax2,
                linestyle="--",
            )
        
        ax2.xaxis.set_major_locator(ticker.MultipleLocator(tickMultiplier))
        ax2.set(ylabel="Data Transfer (GB/min)", xlabel="Zeitstempel")
        ax2.set_xlim(left=-0.5)  # Start from left edge
        ax2.legend(title="Protocol", loc="upper right", ncol=2)
        ax2.grid(True, alpha=0.3)
    ax2.set_title("Data Transfer by Protocol", fontsize=10)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.subplots_adjust(left=0.08, right=0.95, top=0.94, bottom=0.08, hspace=0.4)
    plt.savefig(out_file, dpi=300, bbox_inches="tight")
    plt.show()
    plt.close()
