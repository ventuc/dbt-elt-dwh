
/*
    Transform records of screens from the ODS by looking up in
    the datamart the cinema to which each of them is related
*/

{{ config(materialized='ephemeral') }}


SELECT
    -- 1. Alignes the schema to the one of the data mart dimension table
	s.id AS screen_id,
    s.seats AS screen_seats,
    s.name AS screen_name,
    MAX(c.key) as cinema_key,
    s.update_time -- preserve the update timestamp to allow incremental loading in the next model
FROM {{ source("ods", "screen") }} AS s
    -- 2.	Convert natural keys of cinemas into the corresponding surrogate keys. The cinema
    --		dimension table is a type 2 SCD, thus there can be multiple surrogate keys for
    --		every natural key: in this case, only consider the greatest, as it's the key of
    --		the most up to date record
	INNER JOIN {{ ref("dm_cinema") }} AS c ON c.code = s.cinema_code
GROUP BY s.id, s.seats, s.name, s.update_time
