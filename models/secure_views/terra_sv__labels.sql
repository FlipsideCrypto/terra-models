{{ config(
      materialized='view',
      secure = 'true',
      tags=['snowflake', 'classic', 'labels', 'terra_labels', 'secure_views', 'address_labels']  
    ) 
}}

SELECT
  blockchain, 
  address,
  l1_label as label_type,
  l2_label as label_subtype,
  project_name as label, 
  address_name as address_name
FROM {{ref('silver_classic__address_labels')}}
WHERE blockchain = 'terra'