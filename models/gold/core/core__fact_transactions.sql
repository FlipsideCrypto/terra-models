{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    tx_id,
    block_id,
    block_timestamp,
    auth_type,
    authorizer_public_key,
    tx_sender,
    gas_limit,
    gas_used,
    fee_raw / pow(
        10,
        6
    ) AS fee,
    fee_denom,
    memo,
    codespace,
    tx_code,
    tx_succeeded,
    tx,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__transactions') }}
