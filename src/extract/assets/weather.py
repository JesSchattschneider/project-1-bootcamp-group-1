import pandas as pd
from etl_project.connectors.weather_api import WeatherApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient


def extract_weather(
    weather_api_client: WeatherApiClient, city_reference_path: Path
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """
    df_cities = pd.read_csv(city_reference_path)
    weather_data = []
    for city_name in df_cities["city_name"]:
        weather_data.append(weather_api_client.get_city(city_name=city_name))

    df_weather = pd.json_normalize(weather_data)
    return df_weather


def extract_population(population_reference_path: Path) -> pd.DataFrame:
    """Extracts data from the population file"""
    df_population = pd.read_csv(population_reference_path)
    return df_population


def transform(df_weather: pd.DataFrame, df_population: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'
    # set city names to lowercase
    df_weather["city_name"] = df_weather["name"].str.lower()
    df_merged = pd.merge(left=df_weather, right=df_population, on=["city_name"])
    df_selected = df_merged[["dt", "id", "name", "main.temp", "population"]]
    df_selected["unique_id"] = df_selected["dt"].astype(str) + df_selected["id"].astype(
        str
    )
    # convert unix timestamp column to datetime
    df_selected["dt"] = pd.to_datetime(df_selected["dt"], unit="s")
    # rename colum names to more meaningful names
    df_selected = df_selected.rename(
        columns={"dt": "datetime", "main.temp": "temperature"}
    )
    df_selected = df_selected.set_index(["unique_id"])
    return df_selected


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to either a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
