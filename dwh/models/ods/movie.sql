
/*
    Transformation of movies
*/

{{ config(materialized='incremental', unique_key=['id']) }}

SELECT *
FROM {{ ref('movie_enriched') }}

{% if is_incremental() %}
WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}
