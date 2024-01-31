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
    address_name AS label,
    project_name,
    NULL AS decimals,
    NULL AS deployment_tx_id,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__address_labels') }}
