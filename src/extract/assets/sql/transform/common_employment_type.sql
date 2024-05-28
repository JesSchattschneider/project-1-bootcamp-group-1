select 
 extract(year from date_posted) as job_post_year
, extract(month from date_posted) as job_post_month
, employment_type
, job_id
, job_title
from findwork_data_clean
order by date_posted