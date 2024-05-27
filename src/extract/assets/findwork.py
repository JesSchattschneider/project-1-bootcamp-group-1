import sys
import pandas as pd
from src.extract.connectors.findwork_api import FindWorkApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from src.extract.connectors.postgresql import PostgreSqlClient


def extract_jobs(
    findwork_api_client: FindWorkApiClient, search_query: str = None, location: str = None, page: int = 1
) -> pd.DataFrame:
    """
    Extract job listings based on the search query and location.
    """
    jobs_data = findwork_api_client.get_jobs(
        search_query=search_query, location=location, page=page)

    # df_jobs = pd.json_normalize(jobs_data)
    # print(df_jobs.head())

    # Print the keys of the 'jobs' dictionary
    print("Keys in the 'jobs' dictionary:", jobs_data.keys())

    # print the keys in the results dictionary
    print("Keys in the 'results' dictionary:", jobs_data['results'][0].keys())

    # place the results in a dataframe
    df_jobs_results = pd.json_normalize(jobs_data['results'])
    print(df_jobs_results.head())

    # Print the number of job listings in the 'results' list
    print("Number of job listings in the 'results' list:",
          len(jobs_data['results']))

    return df_jobs_results


def extract_population(population_reference_path: Path) -> pd.DataFrame:
    """Extracts data from the population file"""
    df_population = pd.read_csv(population_reference_path)
    return df_population


def transform_jobs(df_jobs: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'

    # set to lowercase
    # df_jobs["location"] = df_jobs["location"].str.lower()

    # how many rows are in df_jobs
    print("Number of rows in df_jobs:", len(df_jobs))
    # list all columns in df_jobs
    print(df_jobs.columns)

    df_jobs_renamed = df_jobs.rename(
        columns={"id": "job_id",
                 "role": "job_title",
                 "company_name": "company_name",
                 "company_num_employees": "company_num_employees",
                 "employment_type": "employment_type",
                 "location": "job_location",
                 "remote": "remote",
                 "logo": "logo",
                 "url": "url",
                 "text": "job_description",
                 "date_posted": "date_posted",
                 "keywords": "keywords",
                 "source": "source",

                 }
    )

    df_jobs_renamed["job_id"] = df_jobs_renamed["job_id"].astype(str)
    # df_jobs_renamed = df_jobs_renamed.set_index(["job_id"])

    return df_jobs_renamed


def transform_population(df_population: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'

    # set to lowercase
    # df_population["location"] = df_population["location"].str.lower()
    # df_population["city"] = df_population["city"].str.lower()

    # # merge the two dataframes
    # df_merged = pd.merge(left=df_jobs, right=df_population,
    #                      left_on='location', right_on='city')

    # how many rows are in df_population
    # print("Number of rows in df_population:", len(df_population))
    # list all columns in df_population
    # print(df_population.columns)
    # list all city names in df_population
    # print(df_population["city"].unique())

    # # how many rows are in df_merged
    # print("Number of rows in df_merged:", len(df_merged))

    # print the columns
    # print(df_merged.columns)
    # Index(['id', 'role', 'company_name', 'company_num_employees',
    #    'employment_type', 'location', 'remote', 'logo', 'url', 'text',
    #    'date_posted', 'keywords', 'source', 'population', 'pop2024', 'pop2023',
    #    'city', 'country', 'growthRate', 'type', 'rank'],
    #   dtype='object')

    # df_selected = df_merged[["id",
    #                          "role",
    #                          "company_name",
    #                          "company_num_employees",
    #                          "employment_type",
    #                          "location",
    #                          "remote",
    #                          "logo",
    #                          "url",
    #                          "text",
    #                          "date_posted",
    #                          "keywords",
    #                          "source",
    #                          "population",
    #                          "pop2024",
    #                          "pop2023",
    #                          "city",
    #                          "country",
    #                          "growthRate",
    #                          "type",
    #                          "rank"
    #                          ]]

    # # how many rows are in df_selected
    # print("Number of rows in df_selected:", len(df_selected))

    return df_population


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
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
