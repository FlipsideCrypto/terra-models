{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['core']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}
-- depends_on: {{ ref('bronze__streamline_FR_blocks') }}
WITH streamline_blocks AS (

    SELECT
        NULL AS record_id,
        NULL AS offset_id,
        block_number AS block_id,
        b.value :header :time :: datetime AS block_timestamp,
        'mainnet' AS network,
        'terra2' AS chain_id,
        COALESCE(
            ARRAY_SIZE(
                b.value :data :txs
            ) :: NUMBER,
            ARRAY_SIZE(
                DATA :result :block :data :txs
            ) :: NUMBER,
            0
        ) AS tx_count,
        b.value :header AS header,
        b.value :last_commit AS last_commit,
        b.value :evidence AS evidence,
        _inserted_timestamp AS _ingested_at,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

A,
LATERAL FLATTEN(
    input => A.data :result
) AS b
WHERE
    key = 'block'

{% if is_incremental() %}
AND {{ incremental_last_x_days(
    '_inserted_timestamp',
    3
) }}
{% endif %}
),
other_blocks AS (
    SELECT
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        last_commit,
        evidence,
        _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        {{ incremental_last_x_days(
            '_inserted_timestamp',
            3
        ) }}
),
base_blocks AS (
    SELECT
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        last_commit,
        evidence,
        _inserted_timestamp
    FROM
        (
            SELECT
                block_id,
                block_timestamp,
                network,
                chain_id,
                tx_count,
                header,
                last_commit,
                evidence,
                _inserted_timestamp
            FROM
                other_blocks
            UNION ALL
            SELECT
                block_id,
                block_timestamp,
                network,
                chain_id,
                tx_count,
                header,
                last_commit,
                evidence,
                _inserted_timestamp
            FROM
                streamline_blocks
        ) qualify ROW_NUMBER() over (
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
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_val_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    validators_address_array
