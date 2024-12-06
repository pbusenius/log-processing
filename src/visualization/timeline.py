import matplotlib
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

matplotlib.rc("font", size=6)
plt.style.use("bmh")


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
