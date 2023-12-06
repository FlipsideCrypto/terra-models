{{ config(
    materialized = 'view',
    secure = true,
    enabled = false
) }}

WITH messages AS (

    SELECT
        *
    FROM
        {{ ref('silver__messages') }}
)
SELECT
    message_id,
    block_timestamp,
    block_id,
    tx_id,
    tx_succeeded,
    chain_id,
    message_index,
    message_type,
    message_value,
    attributes,
    COALESCE (
        messages_id,
        {{ dbt_utils.generate_surrogate_key(
            ['message_id']
        ) }}
    ) AS ez_messages_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    messages
