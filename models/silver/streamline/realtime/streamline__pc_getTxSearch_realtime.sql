{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'pc_getTxSearch', 'sql_limit', {{var('sql_limit','40000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_id
    FROM
        {{ ref("streamline___max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_id DESC
        ) = 3
),
blocks AS (
    SELECT
        block_id :: STRING AS block_id
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_id >= (
                SELECT
                    block_id
                FROM
                    last_3_days
            )
        )
    EXCEPT
    SELECT
        block_number :: STRING
    FROM
        {{ ref("bronze__streamline_transactions") }}
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "tx_search", "params":["',
            'tx.height=',
            block_id :: INTEGER,
            '",true, "1" ,"100" ,"asc"],"id":"',
            block_id :: STRING,
            '"}'
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_id ASC
