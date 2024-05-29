-- To answer: What is the population of the top 10 cities with more job opportunities?
SELECT
    p.population,
    p.pop2024,
    p.pop2023,
    f.city,
    f.country,
    p.growthRate,
    p.type,
    p.rank,
    COUNT(f.job_id) AS num_of_jobs
FROM
    findwork_data_clean f
LEFT JOIN
    population_data_clean p ON f.city_geopy = p.city_geopy AND f.country_geopy = p.country_geopy
GROUP BY
    p.population,
    p.pop2024,
    p.pop2023,
    f.city,
    f.country,
    p.growthRate,
    p.type,
    p.rank
ORDER BY
    num_of_jobs DESC;

-- with raw_job as (
-- select 
-- job_id
-- , date_posted
-- , extract(year from date_posted) as job_post_year
-- , extract(month from date_posted) as job_post_month
-- , concat(job_post_year, "-", job_post_month) as job_year_month
-- , job_title
-- , job_description
-- , job_location
-- , population
-- from findwork_data f
-- join population_data p
-- on f.job_location = p,city),

-- city_population as (
-- select
-- job_location as city
-- , population
-- from
-- raw_job
-- )

-- select 
-- job_location as city
-- , count(*) as num_of_job
-- from raw_job j
-- inner join city_population c
-- on c.city = j.job_location
-- group by job_location
-- order by count(*) desc;



