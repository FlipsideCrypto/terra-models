{{ config(
    materialized = 'view'
) }}

WITH governance_submit_proposal AS (

    SELECT
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        proposer,
        proposal_id,
        proposal_type
    FROM
        {{ ref('silver__governance_submit_proposal') }}
)
SELECT
    *
FROM
    governance_submit_proposal
