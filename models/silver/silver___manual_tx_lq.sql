{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge'
) }}

SELECT
    DATA :data :tx_response :height :: INT AS block_id,
    DATA :data :tx_response :timestamp :: timestamp_ntz AS block_timestamp,
    DATA :data :tx_response :codespace :: STRING AS codespace,
    DATA :data :tx_response :gas_used :: INT AS gas_used,
    DATA :data :tx_response :gas_wanted :: INT AS gas_wanted,
    DATA :data :tx_response :txhash :: STRING AS tx_id,
    NULL AS auth_type,
    NULL AS authorizer_public_key,
    COALESCE(
        TRY_BASE64_DECODE_STRING(
            DATA :data :tx_response :events [0] :attributes [0] :key
        ),
        DATA :data :tx_response :events [0] :attributes [0] :key
    ) AS msg0_key,
    COALESCE(
        TRY_BASE64_DECODE_STRING(
            DATA :data :tx_response :events [0] :attributes [0] :value
        ),
        DATA :data :tx_response :events [0] :attributes [0] :value
    ) AS msg0_value,
    NULL AS tx_grantee,
    NULL AS tx_granter,
    NULL AS tx_payer,
    COALESCE(
        TRY_BASE64_DECODE_STRING(
            NULL
        ),
        NULL
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
    NULL AS gas_limit,
    NULL AS fee_raw,
    NULL AS fee_denom,
    NULL AS memo,
    DATA :data :tx_response AS tx,
    '' AS tx_code,
    NULL AS tx_succeeded,
    _inserted_timestamp AS _ingested_at,
    _inserted_timestamp
FROM
    {{ ref(
        'bronze_api__manual_tx_lq'
    ) }}
WHERE
    block_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC) = 1)
