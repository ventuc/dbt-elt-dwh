
/*
    Transformation of tickets
*/
-- depends_on: {{ ref('ods_projection') }}

{{ config(materialized='incremental', alias='ticket') }}

SELECT *
FROM {{ ref('ods_ticket_transform') }}

{% if is_incremental() %}
WHERE issue_time > (SELECT IFNULL(MAX(issue_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}
