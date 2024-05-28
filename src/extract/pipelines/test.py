from dotenv import load_dotenv
import os
from src.extract.connectors.findwork_api import FindWorkApiClient
from src.extract.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from src.extract.assets.findwork import (
    extract_population,
    extract_jobs,
    transform,
    load,
)

import yaml
from pathlib import Path
import schedule
import time

   
if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")

    findwork_api_client = FindWorkApiClient(api_key=API_KEY)

    df_jobs = extract_jobs(
        findwork_api_client=findwork_api_client #,
        # city_reference_path=config.get("city_reference_path"),
    )
    print(df_jobs)
    df_population = extract_population(
        population_reference_path= "src\extract\data\world-city-listing-table.csv"
        # PROD: config.get("population_reference_path")
    
    )
   

