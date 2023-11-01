{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH calls AS (

    SELECT
        'https://phoenix-lcd.terra.dev/cosmos/tx/v1beta1/txs/' || tx_id calls,
        tx_id
    FROM
        (
            SELECT
                tx_id
            FROM
                {{ ref(
                    'bronze__rpc_error_blocks_tx_ids'
                ) }}

{% if is_incremental() %}
EXCEPT
SELECT
    tx_id
FROM
    {{ this }}
WHERE
    DATA :data :tx_response :height IS NOT NULL
{% endif %}
LIMIT
    20
)
), results AS (
    SELECT
        tx_id,
        livequery_dev.live.udf_api(
            'GET',
            calls,{},{}
        ) DATA
    FROM
        calls
)
SELECT
    *,
    SYSDATE() AS _inserted_timestamp
FROM
    results
