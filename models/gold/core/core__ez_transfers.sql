{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    CONCAT(
        tx_id,
        '_',
        msg_index
    ) AS transfer_id,
    tx_succeeded,
    NULL AS chain_id,
    NULL AS message_value,
    NULL AS message_type,
    msg_index AS message_index,
    amount,
    currency,
    sender,
    receiver,
    'terra' blockchain,
    transfer_type
FROM
    {{ ref('silver__transfers') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    CONCAT(
        tx_id,
        '_',
        msg_index
    ) AS transfer_id,
    tx_succeeded,
    NULL AS chain_id,
    NULL AS message_value,
    NULL AS message_type,
    msg_index AS message_index,
    amount,
    currency,
    sender,
    receiver,
    'terra' blockchain,
    transfer_type
FROM
    {{ ref('silver__transfers_ibc') }}
