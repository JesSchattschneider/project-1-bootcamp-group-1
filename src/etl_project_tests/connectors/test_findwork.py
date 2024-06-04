from dotenv import load_dotenv
from src.extract.connectors.findwork_api import FindWorkApiClient
import os
import pytest
import pandas as pd
from src.extract.assets.findwork import extract_jobs
from typing import Tuple

@pytest.fixture
def setup():
    load_dotenv()

def test_get_jobs_by_page(setup):
    API_KEY = os.environ.get("API_KEY")
    findwork_api_client = FindWorkApiClient(api_key=API_KEY)

    # Define the search query
    search_query = "data engineer"
    page = 1
    location = None
    page = 1
    data = extract_jobs(findwork_api_client, search_query, location, page)
    
    assert type(data[0]) == pd.DataFrame
    assert type(data[1]) == bool
    assert type(data) == tuple
