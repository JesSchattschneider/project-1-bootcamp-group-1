-- To answer: Whether there are more job opportunities this year than last year.
with job_population as (
select 
job_id
, date_posted::timestamp
, job_title
, job_description
, job_location
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
, population
from job_population),

raw_job2 as (
select
job_id
, date_posted
, job_post_year
, job_post_month
, concat(job_post_year, '-', job_post_month) as job_year_month
, job_title
, job_description
, job_location
, population
from
raw_job
),

job as (
select
job_id
, date_posted
, job_post_year
, job_post_month
, job_year_month
, job_title
, job_description
, job_location
, population
, case when job_year_month >= '2023-6' and job_year_month <= '2024-6' then 'current_year'
       when job_year_month >= '2022-6' and job_year_month <= '2023-6' then 'last_year'
       else 'null'
    end as yr
from
raw_job2 
)

select 
job_post_year
, job_post_month
, count(*) over (partition by yr) as num_of_yearly_job
from job
order by job_post_year;