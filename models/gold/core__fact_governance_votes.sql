{{ config(
    materialized = 'view'
) }}

with governance_votes as (
    select
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
    from {{ ref('silver__governance_votes') }}
)

select * from governance_votes
