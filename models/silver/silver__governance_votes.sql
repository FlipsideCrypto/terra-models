{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert',
    enabled = false
) }}

WITH base_votes AS (

    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        chain_id,
        attributes,
        _inserted_timestamp
    FROM
        {{ ref('silver__messages') }}
    WHERE
        message_type = '/cosmos.gov.v1beta1.MsgVote'
        AND {{ incremental_load_filter('_inserted_timestamp') }}
),
parsed_votes AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        chain_id,
        attributes :message :sender :: text AS voter,
        attributes :proposal_vote :proposal_id :: NUMBER AS proposal_id,
        PARSE_JSON(
            attributes :proposal_vote :option
        ) AS parsed_vote_option,
        parsed_vote_option :option :: NUMBER AS vote_option,
        CASE
            vote_option
            WHEN 1 THEN 'Yes'
            WHEN 2 THEN 'Abstain'
            WHEN 3 THEN 'No'
            WHEN 4 THEN 'NoWithVeto'
        END AS vote_option_text,
        parsed_vote_option :weight :: NUMBER AS vote_weight,
        'terra' AS blockchain,
        _inserted_timestamp
    FROM
        base_votes
),
FINAL AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        voter,
        proposal_id,
        vote_option,
        vote_option_text,
        vote_weight,
        tx_succeeded,
        _inserted_timestamp
    FROM
        parsed_votes
)
SELECT
    *
FROM
    FINAL
