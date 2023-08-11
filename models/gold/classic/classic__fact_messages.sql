{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic']
) }}

SELECT
    *
FROM
    {{ ref('terra_sv__msgs') }}
