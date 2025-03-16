{{ config(
    materialized='incremental',           -- Set the model as incremental
    unique_key='person_id',               -- The column used to uniquely identify each row
    incremental_strategy='delete+insert', -- The strategy to delete and insert records
) }}

WITH person_rnk AS (
    SELECT
        person_id,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY created_at DESC) AS person_id_rnk
    FROM {{ ref('core_events') }}  -- Referencing the core_events table
    GROUP BY person_id, created_at
)

-- Select the rows based on the incremental logic
SELECT
    person_id,
    created_at
FROM person_rnk
WHERE person_id_rnk = 1  -- Keep only the latest record for each person

{% if is_incremental() %}
    -- Only include records with a created_at greater than the maximum created_at already in the target table
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
