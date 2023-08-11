{{ config(
    materialized = 'incremental',
    unique_key = 'blockchain || contract_address || token_id',
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'classic', 'nft_metadata']
) }}

WITH silver AS (
    -- THIS SECTION CURRENTLY PULLS GALACTIC PUNK METADATA ONLY
    -- UNION IN OTHER METADATA AS NEEDED
SELECT
    system_created_at,
    blockchain,
    commission_rate,
    contract_address,
    contract_name,
    created_at_block_id,
    created_at_timestamp,
    created_at_tx_id,
    creator_address,
    creator_name,
    image_url,
    project_name,
    token_id,
    token_metadata,
    token_metadata_uri,
    token_name
FROM
    {{ ref('bronze__classic_nft_metadata') }}
WHERE
    contract_name IS NOT NULL
    AND token_name IS NOT NULL
    AND image_url IS NOT NULL
    AND token_metadata IS NOT NULL

)
SELECT
    system_created_at,
    blockchain,
    commission_rate,
    contract_address,
    contract_name,
    created_at_block_id :: bigint created_at_block_id,
    created_at_timestamp,
    created_at_tx_id,
    creator_address,
    creator_name,
    image_url,
    project_name,
    token_id,
    token_metadata,
    token_metadata_uri,
    token_name
FROM
    silver qualify(ROW_NUMBER() over(PARTITION BY blockchain, contract_address, token_id
ORDER BY
    system_created_at DESC)) = 1
