{{ config(
    materialized = "incremental",
    cluster_by = ["_inserted_timestamp::DATE"],
    unique_key = "address",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH token_labels AS (

    SELECT
        block_timestamp,
        tx_id,
        message_value :msg :name :: STRING AS label,
        message_value :msg :symbol :: STRING AS symbol,
        IFF(
            attributes :instantiate :_contract_address IS NOT NULL,
            attributes :instantiate :_contract_address,
            attributes :reply :_contract_address
        ) :: STRING AS address,
        message_value :msg :decimals :: INT AS decimals,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref("silver__messages") }}
    WHERE
        message_value :msg :decimals IS NOT NULL
        AND {{ incremental_load_filter("_inserted_timestamp") }}
)
SELECT
    'terra' AS blockchain,
    block_timestamp,
    tx_id,
    label,
    symbol,
    address,
    decimals,
    _ingested_at,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS token_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_labels
