{{ config(
    materialized = 'view',
    tags = ['core']
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
    transfer_type,
    COALESCE (
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'msg_index']
        ) }}
    ) AS ez_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    transfer_type,
    COALESCE (
        transfers_ibc_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'msg_index']
        ) }}
    ) AS ez_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__transfers_ibc') }}
