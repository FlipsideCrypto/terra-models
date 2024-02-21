{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['noncore']
) }}

WITH msgs AS (

    SELECT
        tx_id,
        msg_index,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key IN ('acc_seq', 'fee')

{% if is_incremental() %}
DATE_TRUNC(
    'hour',
    _inserted_timestamp
) >= (
    SELECT
        MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
fee AS (
    SELECT
        tx_id,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS fee
    FROM
        msgs
    WHERE
        attribute_key = 'fee'
        AND TRY_CAST(
            fee AS INT
        ) IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
spender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_from
    FROM
        msgs
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
)
SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    MIN(block_id) AS block_number_min,
    MAX(block_id) AS block_number_max,
    COUNT(
        DISTINCT block_id
    ) AS block_count,
    COUNT(
        DISTINCT A.tx_id
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_succeeded THEN A.tx_id
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_succeeded THEN A.tx_id
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT C.tx_from
    ) AS unique_from_count,
    SUM(b.fee / pow(10, 6)) AS total_fees,
    MAX(_inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }} A
    LEFT JOIN fee b
    ON A.tx_id = b.tx_id
    LEFT JOIN spender C
    ON A.tx_id = C.tx_id
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', systimestamp())

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    _inserted_timestamp
) >= (
    SELECT
        MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
