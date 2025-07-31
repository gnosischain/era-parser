-- Migration 001: Initial beacon chain tables
-- Creates all the core tables for beacon chain data

CREATE TABLE IF NOT EXISTS {database}.blocks (
    slot UInt64,
    proposer_index UInt64 DEFAULT 0,
    parent_root String DEFAULT '',
    state_root String DEFAULT '',
    signature String DEFAULT '',
    version String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    randao_reveal String DEFAULT '',
    graffiti String DEFAULT '',
    eth1_deposit_root String DEFAULT '',
    eth1_deposit_count UInt64 DEFAULT 0,
    eth1_block_hash String DEFAULT '',
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, proposer_index);

CREATE TABLE IF NOT EXISTS {database}.sync_aggregates (
    slot UInt64,
    sync_committee_bits String DEFAULT '',
    sync_committee_signature String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    participating_validators UInt32 DEFAULT 0,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot);

CREATE TABLE IF NOT EXISTS {database}.execution_payloads (
    slot UInt64,
    parent_hash String DEFAULT '',
    fee_recipient String DEFAULT '',
    state_root String DEFAULT '',
    receipts_root String DEFAULT '',
    logs_bloom String DEFAULT '',
    prev_randao String DEFAULT '',
    block_number UInt64 DEFAULT 0,
    gas_limit UInt64 DEFAULT 0,
    gas_used UInt64 DEFAULT 0,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    base_fee_per_gas String DEFAULT '',
    block_hash String DEFAULT '',
    blob_gas_used UInt64 DEFAULT 0,
    excess_blob_gas UInt64 DEFAULT 0,
    extra_data String DEFAULT '',
    transactions_count UInt64 DEFAULT 0,
    withdrawals_count UInt64 DEFAULT 0,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, block_number);

CREATE TABLE IF NOT EXISTS {database}.transactions (
    slot UInt64,
    block_number UInt64 DEFAULT 0,
    block_hash String DEFAULT '',
    transaction_index UInt64,
    transaction_hash String,
    fee_recipient String DEFAULT '',
    gas_limit UInt64 DEFAULT 0,
    gas_used UInt64 DEFAULT 0,
    base_fee_per_gas String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, transaction_index, transaction_hash);

CREATE TABLE IF NOT EXISTS {database}.withdrawals (
    slot UInt64,
    block_number UInt64 DEFAULT 0,
    block_hash String DEFAULT '',
    withdrawal_index UInt64,
    validator_index UInt64,
    address String,
    amount UInt64,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, withdrawal_index, validator_index);

CREATE TABLE IF NOT EXISTS {database}.attestations (
    slot UInt64,
    attestation_index UInt64,
    aggregation_bits String DEFAULT '',
    signature String DEFAULT '',
    attestation_slot UInt64 DEFAULT 0,
    committee_index UInt64 DEFAULT 0,
    beacon_block_root String DEFAULT '',
    source_epoch UInt64 DEFAULT 0,
    source_root String DEFAULT '',
    target_epoch UInt64 DEFAULT 0,
    target_root String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, attestation_index, committee_index);

CREATE TABLE IF NOT EXISTS {database}.deposits (
    slot UInt64,
    deposit_index UInt64,
    pubkey String DEFAULT '',
    withdrawal_credentials String DEFAULT '',
    amount UInt64 DEFAULT 0,
    signature String DEFAULT '',
    proof String DEFAULT '[]',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, deposit_index, pubkey);

CREATE TABLE IF NOT EXISTS {database}.voluntary_exits (
    slot UInt64,
    exit_index UInt64,
    signature String DEFAULT '',
    epoch UInt64 DEFAULT 0,
    validator_index UInt64 DEFAULT 0,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, validator_index, epoch);

CREATE TABLE IF NOT EXISTS {database}.proposer_slashings (
    slot UInt64,
    slashing_index UInt64,
    header_1_slot UInt64 DEFAULT 0,
    header_1_proposer_index UInt64 DEFAULT 0,
    header_1_parent_root String DEFAULT '',
    header_1_state_root String DEFAULT '',
    header_1_body_root String DEFAULT '',
    header_1_signature String DEFAULT '',
    header_2_slot UInt64 DEFAULT 0,
    header_2_proposer_index UInt64 DEFAULT 0,
    header_2_parent_root String DEFAULT '',
    header_2_state_root String DEFAULT '',
    header_2_body_root String DEFAULT '',
    header_2_signature String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, slashing_index, header_1_proposer_index);

CREATE TABLE IF NOT EXISTS {database}.attester_slashings (
    slot UInt64,
    slashing_index UInt64,
    att_1_slot UInt64 DEFAULT 0,
    att_1_committee_index UInt64 DEFAULT 0,
    att_1_beacon_block_root String DEFAULT '',
    att_1_source_epoch UInt64 DEFAULT 0,
    att_1_source_root String DEFAULT '',
    att_1_target_epoch UInt64 DEFAULT 0,
    att_1_target_root String DEFAULT '',
    att_1_signature String DEFAULT '',
    att_1_attesting_indices String DEFAULT '[]',
    att_1_validator_count UInt32 DEFAULT 0,
    att_2_slot UInt64 DEFAULT 0,
    att_2_committee_index UInt64 DEFAULT 0,
    att_2_beacon_block_root String DEFAULT '',
    att_2_source_epoch UInt64 DEFAULT 0,
    att_2_source_root String DEFAULT '',
    att_2_target_epoch UInt64 DEFAULT 0,
    att_2_target_root String DEFAULT '',
    att_2_signature String DEFAULT '',
    att_2_attesting_indices String DEFAULT '[]',
    att_2_validator_count UInt32 DEFAULT 0,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    total_slashed_validators UInt32 DEFAULT 0,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, slashing_index, att_1_committee_index);

CREATE TABLE IF NOT EXISTS {database}.bls_changes (
    slot UInt64,
    change_index UInt64,
    signature String DEFAULT '',
    validator_index UInt64 DEFAULT 0,
    from_bls_pubkey String DEFAULT '',
    to_execution_address String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, change_index, validator_index);

CREATE TABLE IF NOT EXISTS {database}.blob_commitments (
    slot UInt64,
    commitment_index UInt64,
    commitment String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, commitment_index);


CREATE TABLE IF NOT EXISTS {database}.deposit_requests (
    slot UInt64,
    request_index UInt64,
    pubkey String,
    withdrawal_credentials String,
    amount UInt64,
    signature String,
    deposit_request_index UInt64,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, request_index, pubkey);

-- Withdrawal Requests (EIP-7002) - Validator withdrawal requests  
CREATE TABLE IF NOT EXISTS {database}.withdrawal_requests (
    slot UInt64,
    request_index UInt64,
    source_address String,
    validator_pubkey String,
    amount UInt64,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, request_index, source_address);

-- Consolidation Requests (EIP-7251) - Validator consolidation requests
CREATE TABLE IF NOT EXISTS {database}.consolidation_requests (
    slot UInt64,
    request_index UInt64,
    source_address String,
    source_pubkey String,
    target_pubkey String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, request_index, source_address);