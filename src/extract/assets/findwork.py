from jinja2 import Environment
import pandas as pd
from src.extract.connectors.findwork_api import FindWorkApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from src.extract.connectors.postgresql import PostgreSqlClient
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from graphlib import TopologicalSorter
import time
from typing import Tuple

def extract_jobs(
    findwork_api_client: FindWorkApiClient, search_query: str = None, location: str = None, page: int = 1
) -> Tuple[pd.DataFrame, bool]:
    """
    Extract job listings based on the search query and location.
    """
    # todo:
    # check if findwork_data already exists in the db, if so, get the last page number
    # if not, set page to list [1:60]

    jobs_data = findwork_api_client.get_jobs(
        search_query=search_query, location=location, page=page)
    
    # Print the keys of the 'jobs' dictionary
    print("Keys in the 'jobs' dictionary:", jobs_data.keys())

    # print the keys in the results dictionary
    print("Keys in the 'results' dictionary:", jobs_data['results'][0].keys())

    # place the results in a dataframe
    df_jobs_results = pd.json_normalize(jobs_data['results'])

    # Print the number of job listings in the 'results' list
    print("Number of job listings in the 'results' list:",
          len(jobs_data['results']))

    # print the value of jobs_data['next'] to see if there are more pages
    print("Value of jobs_data['next']:", jobs_data['next'])

    # if there are more pages, return True
    if jobs_data['next'] is not None:
        has_more = True
    else:
        has_more = False

    return df_jobs_results, has_more

def extract_population(population_reference_path: Path) -> pd.DataFrame:
    """Extracts data from the population file"""
    df_population = pd.read_csv(population_reference_path)
    return df_population

# Initialize geocoder with a longer timeout
geolocator = Nominatim(user_agent="job_location_parser", timeout=1)

def _parse_location(location):
    # Handle NA or None values
    if pd.isna(location) or location.upper() == "NA":
        return ("Unknown", "Unknown")

    # Handle remote jobs separately
    if "REMOTE" in location.upper():
        return ("Remote", "Remote")
    
    # Handle remote jobs with no location specified separately
    if location.upper() == "NONE":
        return ("Unknown", "Unknown")

    attempt = 0
    while attempt < 5:
        try:
            # Geocode the location
            location_geo = geolocator.geocode(location, language='en')
            if location_geo:
                # Split the address into components
                address_parts = location_geo.address.split(',')
                country = address_parts[-1].strip()
                # Assume the city is the second to last part
                city = address_parts[-3].strip() if len(address_parts) > 2 else address_parts[0].strip()
                return (city, country)
            else:
                return ("Unknown", "Unknown")
        except GeocoderTimedOut:
            attempt += 1
            print(f"Timeout occurred for {location}. Retrying... (Attempt {attempt})")
            time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            print(f"Exception: {e}")
            return ("Unknown", "Unknown")
    return ("Unknown", "Unknown")


def transform_jobs(df_jobs: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'

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

    # Set all columns to lowercase 
    df_jobs_renamed.columns = map(str.lower, df_jobs_renamed.columns)

    # Handle location column
    df_jobs_renamed[['city', 'country']] = df_jobs_renamed['job_location'].apply(lambda x: pd.Series(_parse_location(x)))

    # Set values in all columns to lowercase
    df_jobs_renamed = df_jobs_renamed.apply(lambda x: x.astype(str).str.lower())

    df_jobs_renamed.to_csv("jobs.csv", index=False)

    return df_jobs_renamed
class SqlTransform:
    def __init__(
        self,
        postgresql_client: PostgreSqlClient,
        environment: Environment,
        table_name: str,
    ):
        self.postgresql_client = postgresql_client
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.postgresql_client.execute_sql(exec_sql)

def transform(dag: TopologicalSorter):
    """
    Performs `create table as` on all nodes in the provided DAG.
    """
    dag_rendered = tuple(dag.static_order())
    for node in dag_rendered:
        node.create_table_as()

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


# we might not need the func below:
def transform_population(df_population: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'

    # Set all columns to lowercase 
    df_population.columns = map(str.lower, df_population.columns)

    # Handle location column
    df_population[['city', 'country']] = df_population['city'].apply(lambda x: pd.Series(_parse_location(x)))

    # Set values in all columns to lowercase
    df_jobs_renamed = df_population.apply(lambda x: x.astype(str).str.lower())
    print(df_jobs_renamed.head())

    df_jobs_renamed.to_csv("pop.csv", index=False)
    

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
