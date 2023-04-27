{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH base_blocks AS (

    SELECT
        header,
        last_commit,
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
validator_signatures AS (
    SELECT
        header :last_commit :height AS block_id,
        header :last_commit :signatures AS signatures,
        _inserted_timestamp
    FROM
        base_blocks
    WHERE
        block_id <= 4711778
    UNION ALL
    SELECT
        last_commit :height AS block_id,
        last_commit :signatures AS signatures,
        _inserted_timestamp
    FROM
        base_blocks
    WHERE
        block_id > 4711778
),
validator_addresses AS (
    SELECT
        validator_signatures.block_id AS block_id,
        s0.value :validator_address AS validator_address,
        _inserted_timestamp
    FROM
        validator_signatures,
        LATERAL FLATTEN(
            input => validator_signatures.signatures
        ) AS s0
),
validators_address_array AS (
    SELECT
        CAST(validator_addresses.block_id AS NUMBER(38, 0)) AS block_id,
        ARRAY_AGG(
            DISTINCT validator_addresses.validator_address
        ) AS validator_address_array,
        _inserted_timestamp
    FROM
        validator_addresses
    GROUP BY
        validator_addresses.block_id,
        _inserted_timestamp
)
SELECT
    *
FROM
    validators_address_array
