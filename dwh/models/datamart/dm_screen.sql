
/*
    Incrementally select screens loaded from the ODS and insert them into
    the screen dimension table generating any necessary surrogate key
*/

{{ config(
    materialized='incremental', unique_key=['key'], alias='screen',
    pre_hook="CREATE SEQUENCE IF NOT EXISTS screen_seq;"
) }}


SELECT
	IFNULL(target.key, NEXTVAL('screen_seq')) AS key, -- If a surrogate key is not found, generate a new one
	s.screen_id,
    s.screen_seats,
    s.screen_name,
    s.cinema_key
FROM {{ ref("dm_screen_fk_lookup") }} AS s
    -- Lookup the surrogate key for each screen
    LEFT JOIN {{ this }} AS target ON s.screen_id = target.screen_id

-- Select from the ODS only records that have been inserted/modified after the last execution of the process
{% if is_incremental() %}
WHERE s.update_time > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}
