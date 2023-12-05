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
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_id DESC
        ) = 3
),
gen AS (
    SELECT
        ROW_NUMBER() over (
            ORDER BY
                SEQ4()
        ) AS id
    FROM
        TABLE(GENERATOR(rowcount => 50))
),
possible_perms AS (
    SELECT
        id,
        (
            id * 100
        ) - 99 min_count,
        id * 100 max_count
    FROM
        gen
),
perms AS (
    SELECT
        block_id,
        id
    FROM
        (
            SELECT
                A.block_id,
                A.tx_count
            FROM
                {{ ref("silver__blocks") }} A
                JOIN last_3_days b
                ON A.block_id = b.block_id
            WHERE
                A.block_id > 4109598
                AND A.tx_count > 100
        ) A
        JOIN possible_perms
        ON CEIL(
            tx_count,
            -2
        ) >= max_count
),
blocks AS (
    SELECT
        block_id,
        id
    FROM
        perms
    WHERE
        id > 1
    EXCEPT
    SELECT
        block_number :: STRING,
        metadata :request :params [2] :: INT AS id
    FROM
        {{ ref("bronze__streamline_transactions") }}
    WHERE
        metadata NOT LIKE '"1",%'
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "tx_search", "params":["',
            'tx.height=',
            block_id :: INTEGER,
            '",true, "',
            id :: INT :: STRING,
            '" ,"1000" ,"asc"],"id":"',
            block_id :: INT :: STRING,
            '"}'
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_id ASC
