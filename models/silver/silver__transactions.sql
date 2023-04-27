{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id'
) }}

WITH bronze_txs AS (

    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_id
    ORDER BY
        _inserted_timestamp DESC
) = 1
),
silver_blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp :: DATE
            ) -3
        FROM
            {{ this }}
    )
{% endif %}
),
silver_txs AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        chain_id AS blockchain,
        CASE
            WHEN block_id <= 4711778 THEN object_keys(
                tx :auth_info :signer_infos [0] :mode_info
            ) [0] :: STRING
            ELSE NULL
        END AS auth_type,
        CASE
            WHEN block_id <= 4711778 THEN COALESCE(
                tx :auth_info :signer_infos [0] :public_key :key :: ARRAY,
                tx :auth_info :signer_infos [0] :public_key :public_keys :: ARRAY
            )
            ELSE NULL
        END AS authorizer_public_key,
        TRY_BASE64_DECODE_STRING(
            tx :tx_result :events [0] :attributes [0] :key
        ) AS msg0_key,
        TRY_BASE64_DECODE_STRING(
            tx :tx_result :events [0] :attributes [0] :value
        ) AS msg0_value,
        CASE
            WHEN block_id <= 4711778 THEN tx :body :messages [0] :grantee :: STRING
            ELSE NULL
        END AS tx_grantee,
        CASE
            WHEN block_id <= 4711778 THEN tx :auth_info :fee :granter :: STRING
            ELSE NULL
        END AS tx_granter,
        CASE
            WHEN block_id <= 4711778 THEN tx :auth_info :fee :payer :: STRING
            ELSE NULL
        END AS tx_payer,
        TRY_BASE64_DECODE_STRING(
            tx :tx_result :events [1] :attributes [0] :value
        ) AS acc_seq,
        CASE
            WHEN msg0_key = 'spender' THEN msg0_value
            WHEN msg0_key = 'granter' THEN tx_payer
            WHEN msg0_key = 'fee' THEN COALESCE(tx_grantee, SPLIT(acc_seq, '/') [0] :: STRING)
        END AS tx_sender,
        CASE
            WHEN block_id <= 4711778 THEN tx :auth_info :fee :gas_limit :: NUMBER
            ELSE NULL
        END AS gas_limit,
        tx :tx_result :gasUsed :: NUMBER AS gas_used,
        CASE
            WHEN block_id <= 4711778 THEN tx :auth_info :fee :amount [0] :amount :: NUMBER
            ELSE NULL
        END AS fee_raw,
        CASE
            WHEN block_id <= 4711778 THEN tx :auth_info :fee :amount [0] :denom :: STRING
            ELSE NULL
        END AS fee_denom,
        CASE
            WHEN block_id <= 4711778 THEN tx :body :memo :: STRING
            ELSE NULL
        END AS memo,
        tx :tx_result :code :: NUMBER AS tx_code,
        IFF(
            tx_code = 0,
            TRUE,
            FALSE
        ) AS tx_succeeded,
        tx :tx_result :codespace :: STRING AS codespace,
        tx,
        _ingested_at,
        _inserted_timestamp
    FROM
        bronze_txs
)
SELECT
    A.tx_id,
    A.block_id,
    COALESCE(
        A.block_timestamp,
        b.block_timestamp
    ) AS block_timestamp,
    A.auth_type,
    A.authorizer_public_key,
    A.tx_sender,
    A.gas_limit,
    A.gas_used,
    A.fee_raw,
    A.fee_denom,
    A.memo,
    A.codespace,
    A.tx_code,
    A.tx_succeeded,
    A.tx,
    A._ingested_at,
    A._inserted_timestamp
FROM
    silver_txs A
    JOIN silver_blocks b
    ON A.block_id = b.block_id
