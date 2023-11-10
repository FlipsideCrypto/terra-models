{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    tags = ['noncore']
) }}

WITH labels AS (

    SELECT
        blockchain,
        address,
        creator,
        label_type,
        label_subtype,
        address_name AS label,
        project_name,
        system_created_at AS _inserted_timestamp
    FROM
        {{ source(
            'crosschain_silver',
            'address_labels'
        ) }}
    WHERE
        blockchain = 'terra'
        AND delete_flag IS NULL
        AND {{ incremental_load_filter('_inserted_timestamp') }}
        qualify ROW_NUMBER() over (
            PARTITION BY address
            ORDER BY
                creator DESC
        ) = 1
),
tokens AS (
    SELECT
        blockchain,
        'token_deployment' AS creator,
        tx_id,
        label,
        symbol,
        address,
        decimals,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_labels') }}
),
FINAL AS (
    SELECT
        COALESCE(
            t.blockchain,
            l.blockchain
        ) AS blockchain,
        COALESCE(
            t.address,
            l.address
        ) AS address,
        COALESCE(
            t.creator,
            l.creator
        ) AS creator,
        IFF(
            l.label_type IS NOT NULL,
            l.label_type,
            'token'
        ) AS label_type,
        IFF(
            l.label_subtype IS NOT NULL,
            l.label_subtype,
            'token_contract'
        ) AS label_subtype,
        COALESCE(
            t.symbol,
            l.label
        ) AS label,
        COALESCE(
            t.label,
            l.project_name
        ) AS project_name,
        t.decimals,
        t.tx_id AS deployment_tx_id,
        GREATEST(
            l._inserted_timestamp,
            t._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        labels l full
        JOIN tokens t USING (
            blockchain,
            address
        )
)
SELECT
    *
FROM
    FINAL
