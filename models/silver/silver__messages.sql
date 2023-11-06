{{ config(
    materialized = "incremental",
    cluster_by = ["_inserted_timestamp"],
    unique_key = "message_id",
    tags = ' ["run_now"]'
) }}

WITH txs AS (

    SELECT
        tx_id,
        block_timestamp,
        block_id,
        tx,
        tx_succeeded,
        VALUE :events AS logs,
        COALESCE(
            VALUE :msg_index :: NUMBER,
            0
        ) AS message_index,
        tx :body :messages [0] :"@type" :: STRING AS message_type,
        tx :body :messages [message_index] AS message_value,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref("silver__transactions") }},
        LATERAL FLATTEN(
            input => TRY_PARSE_JSON(
                tx :tx_result :log
            )
        )
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
),
blocks AS (
    SELECT
        block_id,
        chain_id
    FROM
        {{ ref("silver__blocks") }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
),
events AS (
    SELECT
        tx_id,
        tx,
        block_timestamp,
        block_id,
        message_index,
        tx_succeeded,
        message_value,
        message_type,
        VALUE AS logs,
        VALUE :attributes AS event_attributes,
        VALUE :type :: STRING AS event_type,
        INDEX AS event_index,
        _ingested_at,
        _inserted_timestamp
    FROM
        txs,
        LATERAL FLATTEN(
            input => logs
        )
),
attributes AS (
    SELECT
        tx_id,
        tx,
        block_timestamp,
        block_id,
        event_attributes,
        event_type,
        event_index,
        tx_succeeded,
        message_index,
        message_type,
        message_value,
        INDEX AS attribute_index,
        VALUE AS ATTRIBUTE,
        VALUE :key :: STRING AS attribute_key,
        IFF(
            VALUE :key = 'amount',
            REGEXP_SUBSTR(
                VALUE :value :: STRING,
                '[0-9]+'
            ),
            VALUE :value :: STRING
        ) AS attribute_value,
        IFF(
            VALUE :key = 'amount',
            REGEXP_SUBSTR(
                VALUE :value :: STRING,
                '[^[:digit:]](.*)'
            ),
            NULL
        ) AS currency,
        COUNT(attribute_key) over (
            PARTITION BY attribute_key,
            event_index,
            message_index,
            tx_id
        ) AS key_frequency,
        ROW_NUMBER() over (
            PARTITION BY attribute_key,
            event_index,
            message_index,
            tx_id
            ORDER BY
                attribute_key
        ) - 1 AS key_index,
        _ingested_at,
        _inserted_timestamp
    FROM
        events,
        LATERAL FLATTEN(
            input => event_attributes
        )
),
window_functions AS (
    SELECT
        tx_id,
        tx,
        event_type,
        event_attributes,
        event_index,
        message_index,
        message_type,
        message_value,
        tx_succeeded,
        attributes.block_id,
        chain_id,
        block_timestamp,
        IFF(
            key_frequency > 1,
            CONCAT(
                attribute_key,
                '_',
                key_index
            ),
            attribute_key
        ) AS unique_attribute_key,
        IFF(
            key_frequency > 1,
            CONCAT(
                'currency',
                '_',
                key_index
            ),
            'currency'
        ) AS unique_currency_key,
        currency,
        attribute_value,
        OBJECT_AGG(
            unique_attribute_key,
            attribute_value :: variant
        ) over (
            PARTITION BY tx_id,
            message_index,
            event_type
        ) AS attribute_obj,
        OBJECT_AGG(
            unique_currency_key,
            currency :: variant
        ) over (
            PARTITION BY tx_id,
            message_index,
            event_type
        ) AS currency_obj,
        json_merge(
            attribute_obj,
            currency_obj
        ) AS final_attrib_obj,
        {# NULL AS final_attrib_obj, #}
        _ingested_at,
        _inserted_timestamp
    FROM
        attributes
        JOIN blocks
        ON attributes.block_id = blocks.block_id
),
distinct_events_table AS (
    SELECT
        DISTINCT tx_id,
        message_index,
        event_type,
        chain_id,
        message_type,
        message_value,
        tx_succeeded,
        block_timestamp,
        block_id,
        final_attrib_obj,
        _ingested_at,
        _inserted_timestamp
    FROM
        window_functions
),
final_table AS (
    SELECT
        DISTINCT CONCAT(
            tx_id,
            '-',
            message_index
        ) AS message_id,
        block_timestamp,
        block_id,
        tx_id,
        tx_succeeded,
        chain_id,
        message_index,
        message_type,
        message_value,
        OBJECT_AGG(
            event_type,
            final_attrib_obj
        ) over (
            PARTITION BY tx_id,
            message_index
        ) AS attributes,
        _ingested_at,
        _inserted_timestamp
    FROM
        distinct_events_table
)
SELECT
    *
FROM
    final_table
