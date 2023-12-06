{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        tx_count,
        chain_id,
        consensus_hash,
        data_hash,
        evidence,
        evidence_hash,
        block_height,
        last_block_id,
        last_commit,
        last_commit_hash,
        last_results_hash,
        next_validators_hash,
        proposer_address,
        validators_hash,
        validator_address_array,
        COALESCE (
            blocks_id,
            {{ dbt_utils.generate_surrogate_key(
                ['block_id']
            ) }}
        ) AS fact_blocks_id,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        blocks
)
SELECT
    *
FROM
    FINAL
