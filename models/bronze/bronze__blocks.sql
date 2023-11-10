{{ config (
    materialized = 'view',
    tags = ['core']
) }}

WITH lq_base AS (

    SELECT
        block_number AS block_id,
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            "bronze",
            "lq_blocks"
        ) }}
),
sline AS (
    SELECT
        block_number AS block_id,
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            "bronze",
            "lq_blocks"
        ) }}
)
SELECT
    record_id,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx_count,
    header,
    NULL :: variant AS last_commit,
    NULL :: variant AS evidence,
    ingested_at AS _ingested_at,
    _inserted_timestamp
FROM
    {{ source(
        "chainwalkers",
        "terra2_blocks"
    ) }}
UNION ALL
SELECT
    NULL AS record_id,
    NULL AS offset_id,
    block_id,
    b.value :header :time :: datetime AS block_timestamp,
    'mainnet' AS network,
    'terra2' AS chain_id,
    COALESCE(
        ARRAY_SIZE(
            b.value :data :txs
        ) :: NUMBER,
        ARRAY_SIZE(
            DATA :result :block :data :txs
        ) :: NUMBER,
        0
    ) AS tx_count,
    b.value :header AS header,
    b.value :last_commit AS last_commit,
    b.value :evidence AS evidence,
    _inserted_timestamp AS _ingested_at,
    _inserted_timestamp
FROM
    lq_base A,
    LATERAL FLATTEN(
        input => A.data :result
    ) AS b
WHERE
    key = 'block'
UNION ALL
SELECT
    NULL AS record_id,
    NULL AS offset_id,
    block_id,
    b.value :header :time :: datetime AS block_timestamp,
    'mainnet' AS network,
    'terra2' AS chain_id,
    COALESCE(
        ARRAY_SIZE(
            b.value :data :txs
        ) :: NUMBER,
        ARRAY_SIZE(
            DATA :result :block :data :txs
        ) :: NUMBER,
        0
    ) AS tx_count,
    b.value :header AS header,
    b.value :last_commit AS last_commit,
    b.value :evidence AS evidence,
    _inserted_timestamp AS _ingested_at,
    _inserted_timestamp
FROM
    lq_base A,
    LATERAL FLATTEN(
        input => A.data :result
    ) AS b
WHERE
    key = 'block'
