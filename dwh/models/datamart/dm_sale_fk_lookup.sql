
/*
    Incrementally select ticket sales from the ODS and insert them into the
	sale fact table looking up the surrogate keys of the dimension tables
*/

{{ config(materialized='ephemeral') }}


SELECT
	e.serial_nr,
	DATE_TRUNC('day', e.issue_time) as sale_date,
	MAX(dt_screen.key) AS screen_key,
	dt_channel.key AS sale_channel_key,
	dt_movie.key AS movie_key,
	dt_show.key AS show_key,
	0 AS actors_cast_key,
	e.price
FROM {{ ref("dm_sale_extract") }} AS e
	INNER JOIN {{ this.schema}}."show" AS dt_show ON dt_show.code = e.show_nr
	INNER JOIN {{ ref("dm_screen") }} AS dt_screen ON dt_screen.screen_id = e.screen_id
	INNER JOIN {{ ref("dm_movie") }} AS dt_movie ON dt_movie.movie_id = e.movie_id
	INNER JOIN {{ this.schema}}.sale_channel AS dt_channel ON dt_channel.channel_id = e.point_of_sale_id
GROUP BY e.serial_nr, sale_date, sale_channel_key, movie_key, show_key, e.price
