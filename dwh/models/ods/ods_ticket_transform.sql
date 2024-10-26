
/*
    Transform tickets loaded from the Booking System to align the schema to that of the ODS
*/

{{ config(materialized='ephemeral', alias='ticket_transform') }}

SELECT
    t.serial_nr,
    t.show as show_nr,
    t.issue_time,
    i.point_of_sale_id,
    t.price,
    CURRENT_TIMESTAMP as insert_time,
    CURRENT_TIMESTAMP as update_time
FROM {{ source('booking_system', 'ticket') }} t
    JOIN {{ source('booking_system', 'invoice') }} i ON t.invoice_number = i.number
