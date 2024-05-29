{% set config = {
    "extract_type": "full",
    "source_table_name": "population_data"
} %}

select
population
, pop2024
, pop2023
, city
, country
, "growthRate" 
, type
, rank
from
    {{ config["source_table_name"] }}


