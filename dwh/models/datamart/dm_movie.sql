
/*
    Incrementally select movies from the ODS and insert them into the
    movie dimension table generating any necessary surrogate key
*/

{{ config(
	materialized='incremental', unique_key=['key'], alias='movie',
    pre_hook="CREATE SEQUENCE IF NOT EXISTS movie_seq;"
) }}


SELECT
    IFNULL(target.key, NEXTVAL('movie_seq')) AS key,
	m.id AS movie_id,
	m.title AS title,
	d.name AS distribution_company,
	p_producer.name AS producer,
	p_director.name AS director,
	m.release_date AS release_date,
	IFNULL(m.estimated_income, 0) AS total_income,
	IFNULL(m.estimated_budget, 0) AS budget
FROM {{ source("ods", "movie") }} AS m
	INNER JOIN {{ source("ods", "distribution_company") }} AS d ON d.id = m.distribution_company_id
	INNER JOIN {{ source("ods", "role") }} AS r_producer ON r_producer.movie_id = m.id
	INNER JOIN {{ source("ods", "person") }} AS p_producer ON r_producer.person_id = p_producer.id
	INNER JOIN {{ source("ods", "role") }} AS r_director ON r_director.movie_id = m.id
	INNER JOIN {{ source("ods", "person") }} AS p_director ON r_director.person_id = p_director.id
    LEFT JOIN {{ this }} AS target ON m.id = target.movie_id
WHERE
	r_producer.role_type = 'producer' AND r_director.role_type = 'director'
	
{% if is_incremental() %}
    AND GREATEST(m.update_time, GREATEST(d.update_time, GREATEST(r_producer.update_time, GREATEST(p_producer.update_time, GREATEST(r_director.update_time, p_director.update_time))))) > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}
