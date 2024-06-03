with raw_job as (
select 
job_id
, date_posted::timestamp
, job_title
, job_description
, job_location
, population
, rank
from findwork_data_transformed f
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
, rank
, count(*) as num_of_job
from raw_job j
inner join city_population c
on c.city = j.job_location
where rank <= 10
group by job_location, rank
order by count(*) desc

