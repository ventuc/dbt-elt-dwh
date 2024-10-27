
/*
    Incrementally select ticket sales from the ODS and insert them into the
	sale fact table looking up the surrogate keys of the dimension tables
*/

{{ config(materialized='ephemeral') }}


SELECT
	t.serial_nr,
	t.show_nr,
	t.issue_time,
	t.point_of_sale_id,
	p.screen_id,
	p.movie_id,
	t.price
FROM {{ source("ods", "ticket") }} AS t
	INNER JOIN {{ source("ods", "projection") }} AS p ON t.show_nr = p.show_nr
