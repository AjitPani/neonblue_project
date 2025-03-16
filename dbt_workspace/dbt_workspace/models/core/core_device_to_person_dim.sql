{{ config(
    materialized='incremental',  -- Set the model as incremental
    unique_key='device_id',      -- The column used to uniquely identify each row
    incremental_strategy='delete+insert', -- The strategy to delete and insert records
) }}

WITH device_to_person_rnk AS (
    SELECT
        con_device_id AS device_id,
        person_id,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY con_device_id ORDER BY created_at DESC) AS device_id_rnk
    FROM {{ ref('core_events') }}  -- Referencing the core_events table
    GROUP BY con_device_id, person_id, created_at  -- Include necessary fields for grouping
)

-- Select the rows based on the incremental logic
SELECT
    device_id,
    Person_id,
    created_at
FROM device_to_person_rnk
WHERE device_id_rnk = 1  -- Only keep the most recent row for each device_id

{% if is_incremental() %}
    -- Only include records with a created_at greater than the maximum created_at already in the target table
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
