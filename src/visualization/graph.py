import polars as pl
from jaal import Jaal

from jaal.datasets import load_got


def plot_network(df: pl.DataFrame = None):
    edge_df, node_df = load_got()
    # init Jaal and run server
    Jaal(edge_df, node_df).plot()
