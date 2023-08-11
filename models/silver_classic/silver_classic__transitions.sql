{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', chain_id, block_id, transition_type, index, event)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
  tags = ['snowflake', 'classic', 'terra_transitions']
) }}

SELECT
  *
FROM
  {{ ref('bronze__classic_transitions') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id, INDEX, transition_type, event
ORDER BY
  system_created_at DESC)) = 1
