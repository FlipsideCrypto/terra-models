{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
-- depends_on: {{ ref('bronze__streamline_FR_transactions') }}
WITH streamline_txs AS (

    SELECT
        NULL AS record_id,
        b.value :hash :: STRING AS tx_id,
        INDEX AS tx_block_index,
        NULL AS offset_id,
        block_number AS block_id,
        NULL AS block_timestamp,
        'mainnet' AS network,
        'terra2' AS chain_id,
        b.value AS tx,
        _inserted_timestamp AS _ingested_at,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}

A,
LATERAL FLATTEN(
    input => A.data :result :txs
) AS b
WHERE
    1 = 1 {# key = 'block' #}
    AND block_number = 8345886

{% if is_incremental() %}
AND {{ incremental_last_x_days(
    '_inserted_timestamp',
    3
) }}
{% endif %}
),
bronze_txs AS (
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
        _ingested_at,
        _inserted_timestamp
    FROM(
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
                _ingested_at,
                _inserted_timestamp
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
UNION ALL
SELECT
    *
FROM
    streamline_txs
) qualify ROW_NUMBER() over (
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
        COALESCE(
            TRY_BASE64_DECODE_STRING(
                tx :tx_result :events [0] :attributes [0] :key
            ),
            tx :tx_result :events [0] :attributes [0] :key
        ) AS msg0_key,
        COALESCE(
            TRY_BASE64_DECODE_STRING(
                tx :tx_result :events [0] :attributes [0] :value
            ),
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
        COALESCE(
            TRY_BASE64_DECODE_STRING(
                tx :tx_result :events [1] :attributes [0] :value
            ),
            tx :tx_result :events [1] :attributes [0] :value
        ) AS acc_seq,
        CASE
            WHEN msg0_key = 'spender'
            AND msg0_value IS NOT NULL THEN msg0_value
            WHEN msg0_key = 'granter'
            AND tx_payer IS NOT NULL THEN tx_payer
            WHEN msg0_key = 'fee'
            AND COALESCE(tx_grantee, SPLIT(acc_seq, '/') [0] :: STRING) IS NOT NULL THEN COALESCE(tx_grantee, SPLIT(acc_seq, '/') [0] :: STRING)
            ELSE msg0_value
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

{% if is_incremental() %}
{% else %}
    UNION ALL
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        NULL AS blockchain,
        auth_type,
        authorizer_public_key :: ARRAY AS authorizer_public_key,
        msg0_key,
        msg0_value,
        tx_grantee,
        tx_granter,
        tx_payer,
        acc_seq,
        tx_sender,
        gas_limit,
        gas_used,
        fee_raw,
        fee_denom,
        memo,
        NULL AS tx_code,
        tx_succeeded,
        codespace,
        tx,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver___manual_tx_lq') }}
    {% endif %}
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
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver_txs A
    JOIN silver_blocks b
    ON A.block_id = b.block_id
