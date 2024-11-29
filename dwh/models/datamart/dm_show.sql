
/*
    Incrementally select projections from the ODS and insert them into the
    show dimension table generating any necessary surrogate key
*/

{{ config(
	materialized='incremental', unique_key=['key'], alias='show',
	pre_hook="CREATE SEQUENCE IF NOT EXISTS show_seq;"
) }}


SELECT
    IFNULL(target.key, NEXTVAL('show_seq')) AS key,
	p.show_nr AS code,
	CAST(time AS DATE) AS show_date,
	CAST(p.time AS TIME) AS start_time,
	(CAST(p.time AS TIME) + INTERVAL (p.interval_duration) MINUTE + INTERVAL (p.advertising_duration) MINUTE) AS end_time,
	p.language,
	(p.interval_duration > 0) AS has_interval,
	p.is_3d_projection AS is_3d
FROM {{ source("ods", "projection") }} AS p
    LEFT JOIN {{ this }} AS target ON p.show_nr = target.code
	
{% if is_incremental() %}
WHERE p.update_time > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}
