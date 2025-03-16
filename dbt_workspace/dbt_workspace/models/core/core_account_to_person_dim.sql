{{ config(
    materialized='incremental',  -- Set the model as incremental
    unique_key='account_id',     -- The column used to uniquely identify each row
    incremental_strategy='delete+insert', -- The strategy to delete and insert records
) }}

WITH account_to_person_rnk AS (
    SELECT
        cov_account_id AS account_id,
        person_id,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY cov_account_id ORDER BY created_at DESC) AS account_id_rnk
    FROM {{ ref('core_events') }}  -- Referencing the core_events table
    GROUP BY cov_account_id, person_id, created_at  -- Include all necessary fields in GROUP BY
)

-- Select the rows based on the incremental logic
SELECT
    account_id,
    person_id,
    created_at
FROM account_to_person_rnk
WHERE account_id_rnk = 1  -- Only keep the most recent row per account_id

{% if is_incremental() %}
    -- Only include records with a created_at greater than the maximum created_at already in the target table
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
