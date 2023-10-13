{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    enabled = false
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        'terra' AS blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        message_value :proposer :: STRING AS proposer,
        attributes :submit_proposal :proposal_id :: INTEGER AS proposal_id,
        attributes :submit_proposal :proposal_type :: STRING AS proposal_type,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver__messages') }}
    WHERE
        message_type ILIKE '%MsgSubmitProposal%'
        AND attributes :message :module :: STRING = 'governance'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    proposer,
    proposal_id,
    proposal_type,
    _ingested_at,
    _inserted_timestamp
FROM
    base
