import folium
import polars as pl

from typing import Tuple
from src.plugins import geodesic


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


def to_file(m: folium.Map, file: str):
    m.save(file)


def open_in_browser(m: folium.Map):
    m.show_in_browser()


def add_line(df: pl.DataFrame, m: folium.Map):
    for row in df.iter_rows(named=True):
        geodesic.Geodesic(
            latitude_a=52.3785,
            longitude_a=4.9000,
            latitude_b=row["latitude"],
            longitude_b=row["longitude"]
        ).add_to(m)



def points(df: pl.DataFrame, name: str = "map.html"):
    centeroid = compute_centeroid(df)

    m = folium.Map(location=centeroid, zoom_start=3)

    add_marker(df, m)

    return m