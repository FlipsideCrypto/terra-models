{{ config(
    materialized = 'view',
    tags = ['classic']
) }}

SELECT
  blockchain, 
  address,
  creator,
  l1_label as label_type,
  l2_label as label_subtype,
  project_name as label, 
  address_name as address_name
FROM {{ref('silver_classic__address_labels')}}
WHERE blockchain = 'terra'
