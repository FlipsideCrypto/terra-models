{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    decimals,
    deployment_tx_id,
    COALESCE (
        address_labels_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_address_labels_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__address_labels') }}
WHERE
    address IS NOT NULL
