
/*
    Incrementally select point of sales from the ODS and insert them into the
    sale channel dimension table generating any necessary surrogate key
*/

{{ config(
    materialized='incremental', unique_key=['key'], alias='sale_channel',
    pre_hook="CREATE SEQUENCE IF NOT EXISTS sale_channel_seq;"
) }}


SELECT
    IFNULL(target.key, NEXTVAL('sale_channel_seq')) AS key,
	p.id AS channel_id,
	type AS channel_type
FROM {{ source("ods", "point_of_sale") }} AS p
    LEFT JOIN {{ this }} AS target ON p.id = target.channel_id
	
{% if is_incremental() %}
WHERE p.update_time > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}
