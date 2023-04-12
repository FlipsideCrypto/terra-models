{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
) }}

WITH base_blocks AS (

    SELECT
        record_id,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        {{ incremental_last_x_days(
            '_inserted_timestamp',
            3
        ) }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
validators_address_array AS (
    SELECT
        block_id,
        validator_address_array,
        _inserted_timestamp
    FROM
        {{ ref('silver__blocks_val_array') }}
    WHERE
        {{ incremental_last_x_days(
            '_inserted_timestamp',
            3
        ) }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
FINAL AS (
    SELECT
        base_blocks.block_id AS block_id,
        base_blocks.block_timestamp AS block_timestamp,
        base_blocks.tx_count AS tx_count,
        base_blocks.header :app_hash :: STRING AS block_hash,
        base_blocks.header :chain_id :: STRING AS chain_id,
        base_blocks.header :consensus_hash :: STRING AS consensus_hash,
        base_blocks.header :data_hash :: STRING AS data_hash,
        base_blocks.header :evidence AS evidence,
        base_blocks.header :evidence_hash :: STRING AS evidence_hash,
        base_blocks.header :height :: INTEGER AS block_height,
        base_blocks.header :last_block_id AS last_block_id,
        base_blocks.header :last_commit AS last_commit,
        base_blocks.header :last_commit_hash :: STRING AS last_commit_hash,
        base_blocks.header :last_results_hash :: STRING AS last_results_hash,
        base_blocks.header :next_validators_hash :: STRING AS next_validators_hash,
        base_blocks.header :proposer_address :: STRING AS proposer_address,
        base_blocks.header :validators_hash :: STRING AS validators_hash,
        base_blocks._ingested_at AS _ingested_at,
        base_blocks._inserted_timestamp AS _inserted_timestamp,
        validators_address_array.validator_address_array :: ARRAY AS validator_address_array
    FROM
        base_blocks
        LEFT JOIN validators_address_array
        ON validators_address_array.block_id = base_blocks.block_id
)
SELECT
    *
FROM
    FINAL
