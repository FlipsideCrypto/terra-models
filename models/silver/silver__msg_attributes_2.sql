{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
  tags = ['core']
) }}

SELECT
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  msg_group,
  msg_sub_group,
  msg_index,
  msg_type,
  b.index AS attribute_index,
  COALESCE(
    TRY_BASE64_DECODE_STRING(
      b.value :key :: STRING
    ),
    b.value :key :: STRING
  ) AS attribute_key,
  COALESCE(
    TRY_BASE64_DECODE_STRING(
      b.value :value :: STRING
    ),
    b.value :value :: STRING
  ) AS attribute_value,
  _inserted_timestamp,
  concat_ws(
    '-',
    tx_id,
    msg_index,
    attribute_index
  ) AS _unique_key,
  {{ dbt_utils.generate_surrogate_key(
    ['_unique_key']
  ) }} AS msg_attributes_2_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('silver__msgs_2') }} A,
  LATERAL FLATTEN(
    input => A.msg,
    path => 'attributes'
  ) b

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      )
    FROM
      {{ this }}
  )
{% endif %}
