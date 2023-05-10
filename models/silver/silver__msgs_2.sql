{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH b AS (

  SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    INDEX AS msg_index,
    VALUE :type :: STRING AS msg_type,
    VALUE AS msg,
    IFF(
      TRY_BASE64_DECODE_STRING(
        msg :attributes [0] :key :: STRING
      ) = 'action',
      TRUE,
      FALSE
    ) AS is_action,
    IFF(
      TRY_BASE64_DECODE_STRING(
        msg :attributes [0] :key :: STRING
      ) = 'module',
      TRUE,
      FALSE
    ) AS is_module,
    TRY_BASE64_DECODE_STRING(
      msg :attributes [0] :key :: STRING
    ) attribute_key,
    TRY_BASE64_DECODE_STRING(
      msg :attributes [0] :value :: STRING
    ) attribute_value,
    _inserted_timestamp
  FROM
    {{ ref('silver__transactions') }} A,
    LATERAL FLATTEN(
      input => A.tx :tx_result :events
    )

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
),
prefinal AS (
  SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    NULLIF(
      (conditional_true_event(is_action) over (PARTITION BY tx_id
      ORDER BY
        msg_index) -1),
        -1
    ) AS msg_group,
    msg_index,
    msg_type,
    LAST_VALUE(
      msg_type
    ) over(
      PARTITION BY tx_id
      ORDER BY
        msg_index
    ) AS last_msg_type,
    msg,
    is_module,
    attribute_key,
    attribute_value,
    _inserted_timestamp
  FROM
    b
),
exec_actions AS (
  SELECT
    DISTINCT tx_id,
    msg_group
  FROM
    prefinal
  WHERE
    msg_type = 'message'
    AND attribute_key = 'action'
    AND (LOWER(attribute_value) LIKE '%exec%'
    OR attribute_value IN ('/ibc.core.channel.v1.MsgRecvPacket', '/ibc.applications.transfer.v1.MsgTransfer'))
),
grp AS (
  SELECT
    A.tx_id,
    A.msg_index,
    is_module,
    RANK() over(
      PARTITION BY A.tx_id,
      A.msg_group
      ORDER BY
        A.msg_index
    ) -1 msg_sub_group
  FROM
    prefinal A
    JOIN exec_actions b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
  WHERE
    A.is_module = TRUE
    AND A.msg_type = 'message'
)
SELECT
  block_id,
  block_timestamp,
  A.tx_id,
  tx_succeeded,
  msg_group,
  COALESCE(
    CASE
      WHEN msg_group IS NULL THEN NULL
      ELSE LAST_VALUE(
        b.msg_sub_group ignore nulls
      ) over(
        PARTITION BY A.tx_id,
        msg_group
        ORDER BY
          A.msg_index DESC rows unbounded preceding
      )
    END,
    CASE
      WHEN msg_group IS NOT NULL
      AND last_msg_type <> 'tx' THEN 0
    END
  ) AS msg_sub_group,
  A.msg_index,
  msg_type,
  msg,
  _inserted_timestamp,
  concat_ws(
    '-',
    A.tx_id,
    A.msg_index
  ) AS _unique_key
FROM
  prefinal A
  LEFT JOIN grp b
  ON A.tx_id = b.tx_id
  AND A.msg_index = b.msg_index
