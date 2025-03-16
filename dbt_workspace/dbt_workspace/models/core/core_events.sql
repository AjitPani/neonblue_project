{{ config(
    materialized='incremental',  -- Set the model as incremental
    unique_key='event_id',        -- The column used to uniquely identify each row
    incremental_strategy='delete+insert', -- The strategy to delete and insert records
) }}

WITH raw_events AS (
    SELECT
        RAW_EVENTS:"event_id"::STRING AS event_id,
        CASE
            WHEN RAW_EVENTS:"account_id"::STRING IS NULL THEN
            CONCAT('DEAFULT','-',HIRING.SCRATCH.AJIT_ACCOUNT_ID_SEQ.NEXTVAL)
            ELSE RAW_EVENTS:"account_id"::STRING
        END AS cov_account_id,

        CASE
            WHEN RAW_EVENTS:"device_id"::STRING IS NULL THEN
            CONCAT('DEAFULT','-',HIRING.SCRATCH.AJIT_DEVICE_ID_SEQ.NEXTVAL)
            ELSE RAW_EVENTS:"device_id"::STRING
        END AS con_device_id,

        CONCAT(cov_account_id, '-', con_device_id) AS person_id,
        RAW_EVENTS:"created_at"::TIMESTAMP_LTZ AS created_at
    FROM {{ source("RAW_EVENTS_AJIT") }}
)

-- Select the rows based on the incremental logic
SELECT
    event_id,
    cov_account_id,
    con_device_id,
    person_id,
    created_at
FROM raw_events

{% if is_incremental() %}
    -- Only include records with a created_at greater than the maximum created_at already in the target table
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}