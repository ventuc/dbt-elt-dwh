
/*
    Incrementally select ticket sales from the ODS and insert them into the
	sale fact table looking up the surrogate keys of the dimension tables
*/

{{ config(materialized='incremental', unique_key=['sale_date', 'screen_key', 'sale_channel_key', 'movie_key', 'show_key', 'actors_cast_key'], alias='sale') }}


SELECT
	sale_date,
	screen_key,
	sale_channel_key,
	movie_key,
	show_key,
	actors_cast_key,
	SUM(price) AS amount,
	COUNT(*) AS quantity
FROM {{ ref("dm_sale_fk_lookup") }}
{% if is_incremental() %}
WHERE sale_date > (SELECT COALESCE(MAX(time), '1900-01-01 00:00:00') FROM last_execution_times WHERE target_table = '{{ this.identifier }}')
{% endif %}
GROUP BY sale_date, screen_key, sale_channel_key, movie_key, show_key, actors_cast_key
