
/*
    Enrichment of movies from the Scheduling System using IMDB data
*/

{{ config(materialized='ephemeral') }}

SELECT
    s.id,
    i.code as imdb_code,
    i.release_date,
    s.title,
    s.duration,
    i.distributor_id as distribution_company_id,
    i.estimated_budget,
    i.estimated_income,
    s.insert_time,
    s.update_time
FROM {{ source('scheduling_system', 'movie') }} s
    LEFT JOIN {{ ref('imdb-movies') }} i ON i.code = s.imdb_code
