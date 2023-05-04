{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        
    )
) }}

-- WITH last_3_days AS (

--     SELECT
--         block_number
--     FROM
--         {{ ref("_max_block_by_date") }}
--         qualify ROW_NUMBER() over (
--             ORDER BY
--                 block_number DESC
--         ) = 3
-- ),
-- blocks AS (
--     SELECT
--         block_number :: STRING AS block_number
--     FROM
--         {{ ref("streamline__blocks") }}
--     WHERE
--         (
--             block_number >= (
--                 SELECT
--                     block_number
--                 FROM
--                     last_3_days
--             )
--         )
--     -- EXCEPT
--     -- SELECT
--     --     block_number :: STRING
--     -- FROM
--     --     {{ ref("streamline__complete_qn_getBlockWithReceipts") }}
--     -- WHERE
--     --     (
--     --         block_number >= (
--     --             SELECT
--     --                 block_number
--     --             FROM
--     --                 last_3_days
--     --         )
--     --     )
-- )
-- SELECT
--     PARSE_JSON(
--         CONCAT(
--             '{"jsonrpc": "2.0",',
--             '"method": "block", "params":["',
--             REPLACE(
--                 concat_ws(
--                     '',
--                     '0x',
--                     to_char(
--                         block_number :: INTEGER,
--                         'XXXXXXXX'
--                     )
--                 ),
--                 ' ',
--                 ''
--             ),
--             '"],"id":"',
--             block_number :: STRING,
--             '"}'
--         )
--     ) AS request
-- FROM
--     blocks
-- ORDER BY
--     block_number ASC
