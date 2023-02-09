{{ config(
    materialized = 'view',
    secure = true
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
    deployment_tx_id
FROM
    {{ ref('silver__address_labels') }}
