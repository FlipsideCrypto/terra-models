{{ config (
    materialized = 'view'
) }}

WITH lq_base AS (

    SELECT
        block_number AS block_id,
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            "bronze",
            "lq_txs"
        ) }}
)
SELECT
    record_id,
    tx_id,
    tx_block_index,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx,
    ingested_at AS _ingested_at,
    _inserted_timestamp
FROM
    {{ source(
        "chainwalkers",
        "terra2_txs"
    ) }}
UNION ALL
SELECT
    NULL AS record_id,
    VALUE :hash :: STRING AS tx_id,
    INDEX AS tx_block_index,
    NULL AS offset_id,
    block_id,
    NULL AS block_timestamp,
    'mainnet' AS network,
    'terra2' AS chain_id,
    b.value AS tx,
    _inserted_timestamp AS _ingested_at,
    _inserted_timestamp
FROM
    lq_base A,
    LATERAL FLATTEN(
        input => A.data :result :txs
    ) AS b
