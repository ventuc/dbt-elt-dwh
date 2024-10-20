
/*
    Transformation of tickets
*/
-- depends_on: {{ ref('projection') }}

{{ config(materialized='incremental') }}

SELECT *
FROM {{ ref('ticket_transform') }}

{% if is_incremental() %}
WHERE issue_time > (SELECT IFNULL(MAX(issue_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}
