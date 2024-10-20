/*
    Transformation of projections
*/
-- depends_on: {{ ref('movie') }}

{{ config(materialized='incremental', unique_key=['show_nr']) }}

SELECT *
FROM {{ source('scheduling_system', 'projection') }}

{% if is_incremental() %}
WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}
