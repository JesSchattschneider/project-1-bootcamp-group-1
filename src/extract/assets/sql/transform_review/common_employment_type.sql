-- To answer: What are the most common employment types over years?
select 
 extract(year from datetime) as job_post_year
, extract(month from datetime) as job_post_month
, employment_type
, job_id
, job_title
, count(*) over (
        partition by concat(job_post_year, "-", job_post_month) 
        order by datetime asc
        rows between unbounded preceding and current row -- (optional) this is optional because this is the default behaviour
    ) as cumulative_of_employment_type
from findwork_data
order by datetime;

