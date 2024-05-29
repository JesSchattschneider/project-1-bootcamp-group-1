with raw_job as (
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
order by count(*) desc

