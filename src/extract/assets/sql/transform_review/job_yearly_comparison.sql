-- To answer: Whether there are more job opportunities this year than last year.
with raw_job as (
select 
job_id
, datetime
, extract(year from datetime) as job_post_year
, extract(month from datetime) as job_post_month
, concat(job_post_year, '-', job_post_month) as job_year_month
, job_title
, job_description
, job_location
, population
from findwork_data f
join population_data p
on f.job_location = p,city),

job as (
select
job_id
, datetime
, extract(year from datetime) as job_post_year
, extract(month from datetime) as job_post_month
, concat(job_post_year, '-', job_post_month) as job_year_month
, job_title
, job_description
, job_location
, population
, case when job_year_month >= "2023-06" and job_year_month <= "2024-06" then "current_year"
       when job_year_month >= "2022-06" and job_year_month <= "2023-06" then "last_year"
       else "null"
    end as yr
from
raw_job 
)

select 
job_post_year
, job_post_month
, count(*) over (partition by yr) as num_of_yearly_job
from job
order by job_post_year;


