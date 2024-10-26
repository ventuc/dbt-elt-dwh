
/*
    Incrementally select cinemas from the ODS and insert into the
    cinema dimension table generating any necessary surrogate key
*/

{{ config(materialized='incremental', unique_key=['key'], alias='cinema') }}
{% set initialize %}
    -- Create a sequence to generate incremental surrogate keys
    CREATE SEQUENCE IF NOT EXISTS cinema_seq;
{% endset %}

{% do run_query(initialize) %}

SELECT
    IFNULL(target.key, NEXTVAL('cinema_seq')) AS key,
	c.code, c.name, c.city, c.province, c.region
FROM {{ source("ods", "cinema") }} AS c
	LEFT JOIN {{ this }} AS target ON c.code = target.code
	
{% if is_incremental() %}
WHERE c.update_time > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}
