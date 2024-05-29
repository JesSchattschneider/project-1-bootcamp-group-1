-- To answer: What are the most common employment types over years?
with job_population as (
select 
job_id
, date_posted::timestamp
, job_title
, job_description
, job_location
, employment_type
from findwork_data f
),

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
from job_population),

raw_employment_type as(
select 
 employment_type
, count(*) over (
        partition by employment_type
        order by date_posted asc
        rows between unbounded preceding and current row 
    ) as cumulative_of_employment_type
from raw_job
order by employment_type)

select
employment_type
, max(cumulative_of_employment_type)
from raw_employment_type
group by employment_type