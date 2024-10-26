
/*
    Incrementally select screens from the ODS and insert into the
    screen dimension table generating any necessary surrogate key
*/

{{ config(materialized='incremental', unique_key=['key'], alias='screen') }}
{% set initialize %}
    -- Create a sequence to generate incremental surrogate keys
    CREATE SEQUENCE IF NOT EXISTS screen_seq;
{% endset %}

{% do run_query(initialize) %}

-- 1.	Convert natural keys of cinemas into the corresponding surrogate keys. The cinema
--		dimension table is a type 2 SCD, thus there can be multiple surrogate keys for
--		every natural key: in this case, only consider the greatest, as it's the key of
--		the most up to date record
-- 2.	Alignes the schema to the one of the data mart dimension table
-- 3.	Lookup the surrogate key for each screen and generated new ones for screens to be
--		inserted into the dimension table
SELECT
	IFNULL(target.key, NEXTVAL('screen_seq')) AS key,
	s.id AS screen_id,
    s.seats AS screen_seats,
    s.name AS screen_name,
    MAX(c.key) as cinema_key
FROM {{ source("ods", "screen") }} AS s
	INNER JOIN cinema AS c ON c.code = s.cinema_code

{% if is_incremental() %}
WHERE s.update_time > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}

GROUP BY s.id, s.seats, s.name
