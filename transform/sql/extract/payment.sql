{% set config = {
    "extract_type": "full",
    "incremental_column": "payment_date",
    "source_table_name": "payment"
} %}

select
job_id
, remote
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
