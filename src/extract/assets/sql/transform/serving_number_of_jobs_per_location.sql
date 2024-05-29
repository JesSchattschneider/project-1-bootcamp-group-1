{% set config = {
    "extract_type": "full",
    "incremental_column": "date_posted",
    "source_table_name_1": "serving_findwork_data",
    "source_table_name_2": "serving_population_data"} %}

SELECT
    p.population,
    p.pop2024,
    p.pop2023,
    f.city,
    f.country,
    p.growthrate,
    p.type,
    p.rank,
    COUNT(f.job_id) AS num_of_jobs
FROM
    {{ config["source_table_name_1"] }} f
LEFT JOIN
    {{ config["source_table_name_2"] }} p ON f.city = p.city AND f.country = p.country
WHERE 
	p.population IS NOT NULL
GROUP BY
    p.population,
    p.pop2024,
    p.pop2023,
    f.city,
    f.country,
    p.growthrate,
    p.type,
    p.rank

ORDER BY
    population, num_of_jobs DESC