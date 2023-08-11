{% macro get_blockchain_api() %}
  {% set query %}
  CREATE schema if NOT EXISTS bronze_api;
{% endset %}
  {% do run_query(query) %}
  {% set query %}
  CREATE TABLE if NOT EXISTS bronze_api.blockchain(
    call ARRAY,
    DATA variant,
    _inserted_timestamp timestamp_ntz
  );
{% endset %}
  {% do run_query(query) %}
  {% set query %}
INSERT INTO
  bronze_api.blockchain(
    call,
    DATA,
    _inserted_timestamp
  ) WITH base AS (
    SELECT
      *
    FROM
      (
        SELECT
          *,
          conditional_true_event(
            CASE
              WHEN rn_mod_out = 1 THEN TRUE
              ELSE FALSE
            END
          ) over (
            ORDER BY
              min_block
          ) groupID_out
        FROM
          (
            SELECT
              *,
              MOD(ROW_NUMBER() over(
            ORDER BY
              min_block), 200) rn_mod_out
            FROM
              (
                SELECT
                  MIN(block_id) min_block,
                  MAX(block_id) max_block,
                  ARRAY_AGG(block_id) blocks
                FROM
                  (
                    SELECT
                      conditional_true_event(
                        CASE
                          WHEN rn_mod = 1 THEN TRUE
                          ELSE FALSE
                        END
                      ) over (
                        ORDER BY
                          block_ID
                      ) groupID,
                      block_id
                    FROM
                      (
                        SELECT
                          block_Id :: STRING block_Id,
                          MOD(ROW_NUMBER() over(
                        ORDER BY
                          block_id), 20) rn_mod
                        FROM
                          (
                            SELECT
                              DISTINCT block_id
                            FROM
                              silver.blocks {# EXCEPT
                            SELECT
                              block_id
                            FROM
                              silver.blockchain #}
                          )
                        ORDER BY
                          block_id
                      )
                  )
                GROUP BY
                  groupID
              )
          )
      )
    WHERE
      groupID_out < 11
  ),
  calls AS (
    SELECT
      groupid_out,
      ARRAY_AGG(
        { 'jsonrpc': '2.0',
        'id': min_block :: INT,
        'method': 'blockchain',
        'params': [min_block::STRING,max_block::STRING] }
      ) call
    FROM
      base
    GROUP BY
      groupid_out
  )
SELECT
  call,
  ethereum.streamline.udf_json_rpc_call(('https://terra-rpc.polkachu.com/'),{}, call) AS DATA,
  SYSDATE()
FROM
  calls;
{% endset %}
  {% do run_query(query) %}
{% endmacro %}
