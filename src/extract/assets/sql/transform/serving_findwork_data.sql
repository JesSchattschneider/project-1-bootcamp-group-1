{% set config = {
    "extract_type": "full",
    "incremental_column": "date_posted",
    "source_table_name": "findwork_data_transformed"
} %}

select
date_posted
, job_id
, remote
, city_geopy AS city
, country_geopy AS country
, company_name
, employment_type
, job_title
, job_description
, job_location
, keywords
from
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}
