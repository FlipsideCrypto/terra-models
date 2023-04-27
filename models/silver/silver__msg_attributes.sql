{{ config(
  materialized = "incremental",
  cluster_by = ["_inserted_timestamp"],
  unique_key = "message_id",
  incremental_strategy = 'delete+insert'
) }}

WITH txs AS (

  SELECT
    *
  FROM
    {{ ref("silver__transactions") }}
  WHERE
    {{ incremental_load_filter("_inserted_timestamp") }}
),
flatten_txs AS (
  SELECT
    tx_id,
    block_timestamp,
    'terra' AS blockchain,
    block_id,
    tx,
    tx_succeeded,
    VALUE :events AS logs,
    VALUE :msg_index :: NUMBER AS msg_index,
    _ingested_at,
    _inserted_timestamp
  FROM
    txs,
    LATERAL FLATTEN(
      input => TRY_PARSE_JSON(
        tx :tx_result :log
      )
    )
),
block_table AS (
  SELECT
    block_id,
    chain_id
  FROM
    {{ ref("silver__blocks") }}
  WHERE
    {{ incremental_load_filter("_inserted_timestamp") }}
),
msg_table AS (
  SELECT
    flatten_txs.block_id,
    flatten_txs.block_timestamp,
    flatten_txs.blockchain,
    flatten_txs.tx_id,
    flatten_txs.tx_succeeded,
    flatten_log.value AS msg,
    msg_index,
    msg :type :: STRING AS msg_type,
    IFF(
      msg :attributes [0] :key :: STRING = 'action',
      TRUE,
      FALSE
    ) AS is_action,
    NULLIF(
      (conditional_true_event(is_action) over (PARTITION BY tx_id
      ORDER BY
        msg_index ASC) -1),
        -1
    ) AS msg_group,
    IFF(
      msg :attributes [0] :key :: STRING = 'module',
      TRUE,
      FALSE
    ) AS is_module,
    msg :attributes [0] :key :: STRING AS attribute_key,
    msg :attributes [0] :value :: STRING AS attribute_value,
    _ingested_at,
    _inserted_timestamp
  FROM
    flatten_txs,
    LATERAL FLATTEN(
      input => logs
    ) AS flatten_log
),
exec_actions AS (
  SELECT
    DISTINCT tx_id,
    msg_group
  FROM
    msg_table
  WHERE
    msg_type = 'message'
    AND attribute_key = 'action'
    AND LOWER(attribute_value) LIKE '%exec%'
),
combined AS (
  SELECT
    msg_table.tx_id,
    msg_table.msg_index,
    RANK() over(
      PARTITION BY msg_table.tx_id,
      msg_table.msg_group
      ORDER BY
        msg_table.msg_index
    ) -1 AS msg_sub_group
  FROM
    msg_table
    INNER JOIN exec_actions AS exec_action
    ON msg_table.tx_id = exec_action.tx_id
    AND msg_table.msg_group = exec_action.msg_group
  WHERE
    msg_table.is_module = 'TRUE'
    AND msg_table.msg_type = 'message'
),
add_chain_id AS (
  SELECT
    msg_t.block_id,
    block_timestamp,
    blockchain,
    chain_id,
    msg_t.tx_id,
    tx_succeeded,
    msg_group,
    CASE
      WHEN msg_group IS NULL THEN NULL
      ELSE COALESCE(
        LAST_VALUE(
          comb.msg_sub_group ignore nulls
        ) over(
          PARTITION BY msg_t.tx_id,
          msg_group
          ORDER BY
            msg_t.msg_index DESC rows unbounded preceding
        ),
        0
      )
    END AS msg_sub_group,
    msg_t.msg_index,
    msg_type,
    msg,
    _ingested_at,
    _inserted_timestamp
  FROM
    msg_table AS msg_t
    LEFT JOIN combined AS comb
    ON msg_t.tx_id = comb.tx_id
    AND msg_t.msg_index = comb.msg_index
    JOIN block_table AS blk
    ON msg_t.block_id = blk.block_id
),
final_msg_table AS (
  SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    concat_ws(
      ':',
      msg_group,
      msg_sub_group
    ) AS msg_group,
    msg_index,
    msg_type,
    msg,
    _ingested_at,
    _inserted_timestamp
  FROM
    add_chain_id
),
msg_attribute AS (
  SELECT
    ROW_NUMBER() over (
      PARTITION BY tx_id
      ORDER BY
        tx_id
    ) AS unique_number,
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_index,
    msg_type,
    attr.index AS attribute_index,
    attr.value :key :: STRING AS attribute_key,
    attr.value :value :: STRING AS attribute_value,
    concat_ws(
      '-',
      tx_id,
      msg_index,
      attribute_index
    ) AS message_id,
    _ingested_at,
    _inserted_timestamp
  FROM
    final_msg_table AS fmt,
    LATERAL FLATTEN(
      input => fmt.msg,
      path => 'attributes'
    ) AS attr
),
FINAL AS (
  SELECT
    concat_ws(
      '-',
      tx_id,
      msg_index,
      attribute_index,
      unique_number
    ) AS message_id,
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_index,
    msg_type,
    attribute_index,
    attribute_key,
    attribute_value,
    _ingested_at,
    _inserted_timestamp
  FROM
    msg_attribute
)
SELECT
  *
FROM
  FINAL
