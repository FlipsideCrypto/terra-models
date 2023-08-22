{{ config(
    materialized = 'view',
    secure = true
) }}

WITH transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__transfers') }}
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    transfer_id,
    tx_succeeded,
    chain_id,
    message_value,
    message_type,
    message_index,
    amount,
    currency,
    sender,
    receiver,
    blockchain,
    transfer_type
FROM
    transfers
