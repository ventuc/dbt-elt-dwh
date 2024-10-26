/*
    Transformation of projections
*/
-- depends_on: {{ ref('ods_movie') }}

{{ config(materialized='incremental', unique_key=['show_nr'], alias="projection") }}

SELECT *
FROM {{ source('scheduling_system', 'projection') }}

{% if is_incremental() %}
WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}
