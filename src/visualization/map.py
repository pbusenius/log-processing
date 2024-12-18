import folium
import polars as pl

from typing import Tuple


def compute_centeroid(df: pl.DataFrame) -> Tuple[float, float]:
    return df.select(pl.mean("latitude", "longitude")).row(0)


def add_marker(df: pl.DataFrame, m: folium.Map):
    for row in df.iter_rows(named=True):
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            tooltip="Information",
            popup=f"Country: {row["country_information"]}, City: {row["city_information"]}",
            icon=folium.Icon(color="green"),
        ).add_to(m)

    return m


def points(df: pl.DataFrame, name: str = "map.html"):
    # read test file for testing
    # df = pl.read_csv("data/test.csv")

    centeroid = compute_centeroid(df)

    m = folium.Map(location=centeroid)

    add_marker(df, m)

    m.save(name)
