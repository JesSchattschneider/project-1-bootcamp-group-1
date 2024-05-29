select
 case when remote ='true' then 'remote work'
       when remote ='false' then 'office work'
       end as work_location
, count(job_id) num_of_job
from findwork_data 
group by work_location