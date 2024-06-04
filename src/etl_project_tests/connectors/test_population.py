from dotenv import load_dotenv
import pytest
import pandas as pd
from src.extract.assets.findwork import extract_population

@pytest.fixture
def setup():
    load_dotenv()

def test_get_population(setup):

    population_reference_path = "./src/extract/data/world-city-listing-table-geopy-clean.csv"
    data = extract_population(population_reference_path=population_reference_path)

    assert type(data) == pd.DataFrame
