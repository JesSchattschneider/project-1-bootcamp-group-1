-- To answer: What are the most common employment types over years?
with job_population as (
select 
job_id
, date_posted::timestamp
, job_title
, job_description
, job_location
, employment_type
, population
from findwork_data f
join population_data p
on f.job_location = p.city),

raw_job as (
select 
job_id
, date_posted
, extract(year from date_posted) as job_post_year
, extract(month from date_posted) as job_post_month
, job_title
, job_description
, job_location
, employment_type
, population
from job_population)

select 
date_posted
, extract(year from date_posted) as job_post_year
, extract(month from date_posted) as job_post_month
, employment_type
, job_id
, job_title
, count(*) over (
        partition by concat(job_post_year, '-', job_post_month) 
        order by date_posted asc
        rows between unbounded preceding and current row 
    ) as cumulative_of_employment_type
from raw_job
order by date_posted