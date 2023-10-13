{{ config(
    materialized = 'view',
    enabled = false
) }}

WITH governance_votes AS (

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
        tx_succeeded
    FROM
        {{ ref('silver__governance_votes') }}
)
SELECT
    *
FROM
    governance_votes
