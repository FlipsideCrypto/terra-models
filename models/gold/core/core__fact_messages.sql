{{ config(
    materialized = 'view',
    enabled = false
) }}

SELECT
    *
FROM
    {{ ref('core__ez_messages') }}
