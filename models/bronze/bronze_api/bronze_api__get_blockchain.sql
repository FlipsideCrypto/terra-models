{{ config(
    materialized = 'incremental',
    full_refresh = false,
    tags = ['noncore']
) }}

WITH min_block AS (

    SELECT
        ethereum.streamline.udf_json_rpc_call(
            'https://terra-rpc.publicnode.com/',{},
            ARRAY_AGG(
                { 'jsonrpc': '2.0',
                'id': 0,
                'method': 'blockchain',
                'params': ['1','2'] }
            )
        ) DATA,
        REPLACE(
            DATA :data :error :data,
            'min height '
        ) rep,
        LEFT(rep, CHARINDEX('can', rep) -2) :: INT AS min_block
),
base AS (
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
                        min_block), 100) rn_mod_out
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
                                                        {{ ref('silver__blocks') }}
                                                    WHERE
                                                        block_id >= (
                                                            SELECT
                                                                min_block
                                                            FROM
                                                                min_block
                                                        )

{% if is_incremental() %}
EXCEPT
SELECT
    block_id
FROM
    silver.blockchain
{% endif %}
)
)
)
GROUP BY
    groupID
)
)
)
WHERE
    groupID_out < 6
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
    ethereum.streamline.udf_json_rpc_call(
        'https://terra-rpc.publicnode.com/',{},
        call
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
FROM
    calls
