import pandas as pd
from src.extract.connectors.findwork_api import FindWorkApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from src.extract.connectors.postgresql import PostgreSqlClient


def extract_jobs(
    findwork_api_client: FindWorkApiClient, search_query: str, location: str = None, page: int = 1
) -> pd.DataFrame:
    """
    Extract job listings based on the search query and location.
    """
    jobs_data = findwork_api_client.get_jobs(
        search_query=search_query, location=location, page=page)
    df_jobs = pd.json_normalize(jobs_data)
    return df_jobs


def extract_population(population_reference_path: Path) -> pd.DataFrame:
    """Extracts data from the population file"""
    df_population = pd.read_csv(population_reference_path)
    return df_population


def transform(df_jobs: pd.DataFrame, df_population: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'
    # set city names to lowercase
    df_jobs["city_name"] = df_jobs["location"].str.lower()
    df_merged = pd.merge(left=df_jobs, right=df_population, on=["city_name"])
    df_selected = df_merged[["id", "title",
                             "location", "description", "population"]]
    df_selected["unique_id"] = df_selected["id"].astype(str)
    df_selected = df_selected.rename(
        columns={"id": "job_id", "title": "job_title",
                 "location": "job_location", "description": "job_description"}
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
