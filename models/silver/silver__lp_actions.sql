{{ config(
    materialized = "incremental",
    cluster_by = ["_inserted_timestamp"],
    unique_key = "action_id",
    enabled = false
) }}

WITH pools AS (

    SELECT
        *
    FROM
        {{ ref("core__dim_address_labels") }}
    WHERE
        label_subtype = 'pool'
),
prelim_table AS (
    SELECT
        block_id,
        block_timestamp,
        'terra' AS blockchain,
        tx_id,
        tx_succeeded,
        chain_id,
        message_value,
        message_type,
        message_index,
        NULLIF(
            message_value :contract,
            message_value :msg :send :contract
        ) :: STRING AS pool_address,
        message_value :sender :: STRING AS liquidity_provider_address,
        attributes,
        path,
        VALUE :: STRING AS obj_value,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref("silver__messages") }},
        LATERAL FLATTEN(
            input => attributes :wasm
        )
    WHERE
        attributes :wasm IS NOT NULL
        AND message_type = '/cosmwasm.wasm.v1.MsgExecuteContract'
        AND {{ incremental_load_filter("_inserted_timestamp") }}
),
intermediate_table AS (
    SELECT
        prelim_table.*,
        label
    FROM
        prelim_table
        JOIN pools
        ON prelim_table.pool_address = pools.address
),
final_table AS (
    SELECT
        block_id,
        block_timestamp,
        blockchain,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                _inserted_timestamp DESC
        ) AS action_index,
        CONCAT(
            tx_id,
            '-',
            action_index -1
        ) AS action_id,
        tx_id,
        tx_succeeded,
        chain_id,
        pool_address,
        liquidity_provider_address,
        CASE
            WHEN path = 'refund_assets' THEN 'withdraw_liquidity'
            WHEN path = 'assets' THEN 'provide_liquidity'
            WHEN path = 'withdrawn_share' THEN 'burn_lp_token'
            WHEN path = 'share' THEN 'mint_lp_token'
            ELSE NULL
        END AS action,
        REGEXP_SUBSTR(
            VALUE,
            '[0-9]+'
        ) :: bigint AS amount,
        IFF(path IN ('withdrawn_share', 'share'), label, REGEXP_SUBSTR(VALUE, '[^[:digit:]](.*)')) AS currency,
        NULL AS decimals,
        _ingested_at,
        _inserted_timestamp
    FROM
        intermediate_table,
        LATERAL SPLIT_TO_TABLE(
            intermediate_table.obj_value,
            ', '
        )
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                intermediate_table
            WHERE
                obj_value IN (
                    'provide_liquidity',
                    'withdraw_liquidity'
                )
        )
        AND path IN (
            'refund_assets',
            'withdrawn_share',
            'share',
            'assets'
        )
)
SELECT
    block_id,
    block_timestamp,
    action_id,
    tx_id,
    tx_succeeded,
    blockchain,
    chain_id,
    pool_address,
    liquidity_provider_address,
    action,
    amount,
    currency,
    decimals,
    _ingested_at,
    _inserted_timestamp
FROM
    final_table
