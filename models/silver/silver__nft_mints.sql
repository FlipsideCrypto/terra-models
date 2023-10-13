{{ config(
    materialized = "incremental",
    cluster_by = ["_inserted_timestamp"],
    unique_key = "mint_id",
    enabled = false
) }}

WITH nft_mints AS (

    SELECT
        block_id,
        block_timestamp,
        'terra' AS blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        CASE
            WHEN attributes :wasm :_contract_address IS NOT NULL THEN attributes :wasm :_contract_address :: STRING
            WHEN attributes :wasm :_contract_address_1 IS NOT NULL THEN attributes :wasm :_contract_address_1 :: STRING
            WHEN message_value :msg :mint :mint_request :nft_contract IS NOT NULL THEN message_value :msg :mint :mint_request :nft_contract :: STRING
            ELSE NULL
        END AS contract_address,
        message_value :msg :mint AS mint_obj,
        attributes,
        NULLIF(
            message_value :funds [0] :amount :: bigint,
            0
        ) AS mint_price,
        message_value :sender :: STRING AS minter,
        attributes :wasm :token_id :: STRING AS token_id,
        attributes :coin_spent :currency_0 :: STRING AS currency,
        NULL AS decimals,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                _inserted_timestamp DESC
        ) AS INDEX,
        CONCAT(
            tx_id,
            '-',
            INDEX
        ) AS mint_id,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver__messages') }}
    WHERE
        (
            message_value :msg :mint :extension IS NOT NULL
            OR message_value :msg :mint :metadata_uri IS NOT NULL
            OR message_value :msg :mint :mint_request IS NOT NULL
            OR message_value :msg :mint :metadata IS NOT NULL
        )
        AND message_type != '/cosmwasm.wasm.v1.MsgInstantiateContract'
        AND {{ incremental_load_filter("_inserted_timestamp") }}
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    contract_address,
    mint_price,
    minter,
    token_id,
    currency,
    decimals,
    mint_id,
    _ingested_at,
    _inserted_timestamp
FROM
    nft_mints
