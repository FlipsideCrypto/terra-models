{{ config(
    materialized = "incremental",
    unique_key = "SWAP_ID",
    incremental_strategy = "delete+insert",
    cluster_by = ["block_timestamp::DATE", "_inserted_timestamp::DATE"],
) }}


WITH all_swaps AS( 

    SELECT
        block_id,
        block_timestamp,
        _inserted_timestamp,
        'Terra' AS blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        message_index AS msg_index,
        attributes :wasm AS swap_info
    FROM
        {{ ref("silver__messages") }}
    WHERE
        message_type ILIKE '%msgexecutecontract%'
        AND (message_value :msg :swap IS NOT NULL
            OR message_value :msg :execute_swap_operations IS NOT NULL)
        AND {{ incremental_load_filter('_inserted_timestamp') }}
),

flattened_data AS(     --- flattened allswaps table
    SELECT
        swap.*,
        flat.seq, 
        flat.key,
        flat.value
    FROM
        all_swaps swap,
        LATERAL FLATTEN(
            input => swap_info
        ) flat
),

offer_amount_pattern AS(    ---associated swaps columns excluding swap contract_addresses column
    SELECT
        *,
        len('offer_amount') AS offer_amtlen,
        RIGHT(key, len(key) - offer_amtlen) AS suffix_ch
    FROM
        flattened_data
    WHERE
        key ILIKE '%offer_amount%'),

action_pattern AS(    --- only swap contract_addresses column
    SELECT
        *,
        len('action') AS actlen,
        RIGHT(key, len(key) - actlen) AS suffix_ch
    FROM
        flattened_data
    WHERE
        key ILIKE '%action%'
        AND VALUE = 'swap'
),

processed_swaps AS(    --- Associated swap columns including the swap contract addresses column
    SELECT
        offer.block_id,
        offer.block_timestamp,
        offer._inserted_timestamp,
        offer.blockchain,
        offer.chain_id,
        offer.tx_id,
        offer.msg_index,
        offer.tx_succeeded,
        COALESCE(
            offer.swap_info [CONCAT('offer_amount',offer.suffix_ch)] :: INTEGER,
            offer.swap_info ['offer_amount'] :: INTEGER
        ) AS from_amount,
        COALESCE(
            offer.swap_info [CONCAT('offer_asset',offer.suffix_ch)] :: STRING,
            offer.swap_info ['offer_asset'] :: STRING
        ) AS from_currency,
        6 :: INTEGER AS from_decimal,
        COALESCE(
            offer.swap_info [CONCAT('return_amount',offer.suffix_ch)] :: INTEGER,
            offer.swap_info ['return_amount'] :: INTEGER
        ) AS to_amount,
        COALESCE(
            offer.swap_info [CONCAT('ask_asset',offer.suffix_ch)] :: STRING,
            offer.swap_info ['ask_asset'] :: STRING
        ) AS to_currency,
        6 :: INTEGER AS TO_DECIMAL,
        COALESCE(
            action.swap_info [CONCAT('_contract_address',action.suffix_ch)] :: STRING,
            action.swap_info ['_contract_address'] :: STRING
        ) AS contract_address
    FROM
        offer_amount_pattern offer
        FULL JOIN action_pattern action
        ON offer.tx_id = action.tx_id
        AND offer.seq = action.seq
),

transactions AS (
    SELECT
        tx_id,
        tx_sender
    FROM
        {{ ref ('silver__transactions') }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
),

final_table AS (
    SELECT
        DISTINCT concat_ws (
            '-',
            s.tx_id,
            msg_index,
            s.contract_address
        ) AS swap_id,
        s.block_id,
        s.block_timestamp,
        s._inserted_timestamp,
        s.blockchain,
        s.chain_id,
        s.tx_id,
        s.tx_succeeded,
        t.tx_sender AS trader,
        from_amount,
        from_currency,
        from_decimal,
        to_amount,
        to_currency,
        TO_DECIMAL,
        contract_address AS pool_id
    FROM
        processed_swaps s
        INNER JOIN transactions t
        ON s.tx_id = t.tx_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY swap_id 
        ORDER BY s.block_timestamp DESC
        ) = 1
)

SELECT
    *
FROM
    final_table
