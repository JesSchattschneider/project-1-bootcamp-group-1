-- To answer: What is the population of the top 10 cities with more job opportunities?
with raw_job as (
select 
job_id
, date_posted
, extract(year from date_posted) as job_post_year
, extract(month from date_posted) as job_post_month
, concat(job_post_year, "-", job_post_month) as job_year_month
, job_title
, job_description
, job_location
, population
from findwork_data f
join population_data p
on f.job_location = p,city),

city_population as (
select
job_location as city
, population
from
raw_job
)

select 
job_location as city
, count(*) as num_of_job
from raw_job j
inner join city_population c
on c.city = j.job_location
group by job_location
order by count(*) desc;



