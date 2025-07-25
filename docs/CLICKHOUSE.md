# ClickHouse Integration

This document provides comprehensive guidance for using Era Parser with ClickHouse, including setup, schema details, optimization strategies, and common queries.

## Quick Start

### Environment Setup
```bash
# Required ClickHouse connection settings
export CLICKHOUSE_HOST=your-clickhouse-host.com
export CLICKHOUSE_PASSWORD=your-password

# Optional settings (with defaults)
export CLICKHOUSE_PORT=8443
export CLICKHOUSE_USER=default
export CLICKHOUSE_DATABASE=beacon_chain
export CLICKHOUSE_SECURE=true
```

### Basic Usage
```bash
# Export era file to ClickHouse
era-parser gnosis-02607.era all-blocks --export clickhouse

# Process remote eras to ClickHouse
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Resume interrupted processing
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume
```

## Database Schema

### Core Tables

Era Parser creates normalized tables optimized for analytics:

```sql
-- Beacon chain block headers
CREATE TABLE beacon_chain.blocks (
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

-- Sync committee data (Altair+)
CREATE TABLE beacon_chain.sync_aggregates (
    slot UInt64,
    sync_committee_bits String DEFAULT '',
    sync_committee_signature String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    participating_validators UInt32 DEFAULT 0,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot);

-- Execution layer blocks (Bellatrix+)
CREATE TABLE beacon_chain.execution_payloads (
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

-- Individual transactions
CREATE TABLE beacon_chain.transactions (
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

-- Validator withdrawals (Capella+)
CREATE TABLE beacon_chain.withdrawals (
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

-- Validator attestations
CREATE TABLE beacon_chain.attestations (
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

-- Validator deposits
CREATE TABLE beacon_chain.deposits (
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

-- Voluntary exits
CREATE TABLE beacon_chain.voluntary_exits (
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

-- Proposer slashings
CREATE TABLE beacon_chain.proposer_slashings (
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

-- Attester slashings with full validator indices support
CREATE TABLE beacon_chain.attester_slashings (
    slot UInt64,
    slashing_index UInt64,
    
    -- Attestation 1 data
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
    
    -- Attestation 2 data  
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
    
    -- Metadata
    timestamp_utc DateTime DEFAULT toDateTime(0),
    total_slashed_validators UInt32 DEFAULT 0,
    
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, slashing_index, att_1_committee_index);

-- BLS to execution changes (Capella+)
CREATE TABLE beacon_chain.bls_changes (
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

-- Blob KZG commitments (Deneb+)
CREATE TABLE beacon_chain.blob_commitments (
    slot UInt64,
    commitment_index UInt64,
    commitment String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, commitment_index);

-- Execution requests (Electra+)
CREATE TABLE beacon_chain.execution_requests (
    slot UInt64,
    request_type String,
    request_index UInt64,
    pubkey String DEFAULT '',
    withdrawal_credentials String DEFAULT '',
    amount UInt64 DEFAULT 0,
    signature String DEFAULT '',
    deposit_request_index UInt64 DEFAULT 0,
    source_address String DEFAULT '',
    validator_pubkey String DEFAULT '',
    source_pubkey String DEFAULT '',
    target_pubkey String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, request_type, request_index);
```

### State Management Tables

Era Parser includes sophisticated state tracking:

```sql
-- Granular processing state per era and dataset
CREATE TABLE beacon_chain.era_processing_state (
    era_filename String,
    network String,
    era_number UInt32,
    dataset String,
    status String,
    worker_id String DEFAULT '',
    attempt_count UInt8 DEFAULT 0,
    created_at DateTime DEFAULT now(),
    completed_at Nullable(DateTime),
    rows_inserted Nullable(UInt64),
    file_hash String DEFAULT '',
    error_message Nullable(String),
    processing_duration_ms Nullable(UInt64),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9)),
    
    INDEX idx_status (status) TYPE minmax GRANULARITY 4,
    INDEX idx_network_dataset (network, dataset) TYPE minmax GRANULARITY 4,
    INDEX idx_era_number (era_number) TYPE minmax GRANULARITY 4
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY (network, toYYYYMM(created_at))
ORDER BY (era_filename, dataset)
SETTINGS index_granularity = 8192;

-- Era-level progress view
CREATE VIEW beacon_chain.era_processing_progress AS
SELECT 
    network,
    era_filename,
    era_number,
    countIf(status = 'completed') as completed_datasets,
    countIf(status = 'processing') as processing_datasets,
    countIf(status = 'failed') as failed_datasets,
    countIf(status = 'pending') as pending_datasets,
    count(*) as total_datasets,
    sum(rows_inserted) as total_rows_inserted,
    maxIf(completed_at, status = 'completed') as last_completed_at
FROM beacon_chain.era_processing_state
GROUP BY network, era_filename, era_number;

-- Dataset-level progress view
CREATE VIEW beacon_chain.dataset_processing_progress AS
SELECT
    network,
    dataset,
    countIf(status = 'completed') as completed_eras,
    countIf(status = 'processing') as processing_eras,
    countIf(status = 'failed') as failed_eras,
    countIf(status = 'pending') as pending_eras,
    count(*) as total_eras,
    sum(rows_inserted) as total_rows_inserted,
    maxIf(era_number, status = 'completed') as highest_completed_era
FROM beacon_chain.era_processing_state
GROUP BY network, dataset;
```

## Key Features

### Granular State Management
Era Parser tracks processing at the dataset level, enabling:
- **Smart Resume**: Only processes missing datasets
- **Parallel Processing**: Multiple workers can process different datasets
- **Error Handling**: Failed datasets don't block others
- **Progress Monitoring**: Detailed visibility into processing status

### Optimized Performance
- **Streaming Inserts**: Memory-efficient processing of large era files
- **Batch Processing**: Adaptive batch sizes for optimal throughput
- **Connection Resilience**: Automatic reconnection and retry logic
- **Cloud Optimization**: Settings tuned for ClickHouse Cloud

### Time-Based Partitioning
All tables are partitioned by `toStartOfMonth(timestamp_utc)` for:
- **Efficient Queries**: Time-range queries use partition pruning
- **Maintenance**: Easy to drop old data or manage storage
- **Performance**: Reduced query times on time-series data

## Configuration Options

### Connection Settings
```bash
# Basic connection (required)
CLICKHOUSE_HOST=your-host.com
CLICKHOUSE_PASSWORD=your-password

# Advanced settings (optional)
CLICKHOUSE_PORT=8443                    # Default: 8443
CLICKHOUSE_USER=default                 # Default: default
CLICKHOUSE_DATABASE=beacon_chain        # Default: beacon_chain
CLICKHOUSE_SECURE=true                  # Default: true

# Performance tuning
ERA_CLEANUP_AFTER_PROCESS=true          # Delete era files after processing
ERA_MAX_RETRIES=3                       # Retry attempts for failed operations
```

### Docker Configuration
```yaml
# docker-compose.yml
services:
  era-parser:
    image: era-parser:latest
    environment:
      - CLICKHOUSE_HOST=${CLICKHOUSE_HOST}
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
      - CLICKHOUSE_DATABASE=beacon_chain
    volumes:
      - ./era-files:/app/era-files:ro
      - ./output:/app/output
```

## Common Queries

### Block Analysis
```sql
-- Block production over time
SELECT 
    toStartOfHour(timestamp_utc) as hour,
    count() as blocks_produced,
    count(DISTINCT proposer_index) as unique_proposers
FROM blocks 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
GROUP BY hour
ORDER BY hour;

-- Top block proposers
SELECT 
    proposer_index,
    count() as blocks_proposed,
    min(timestamp_utc) as first_block,
    max(timestamp_utc) as last_block
FROM blocks 
WHERE timestamp_utc >= now() - INTERVAL 7 DAY
GROUP BY proposer_index
ORDER BY blocks_proposed DESC
LIMIT 10;

-- Block production patterns by day of week
SELECT 
    toDayOfWeek(timestamp_utc) as day_of_week,
    count() as blocks_produced,
    avg(blocks_produced) OVER () as avg_daily_blocks
FROM blocks 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY day_of_week
ORDER BY day_of_week;

-- Missed slots analysis
WITH slot_range AS (
    SELECT min(slot) as min_slot, max(slot) as max_slot
    FROM blocks 
    WHERE timestamp_utc >= now() - INTERVAL 1 DAY
),
expected_slots AS (
    SELECT number as expected_slot
    FROM numbers((SELECT max_slot - min_slot + 1 FROM slot_range))
    SETTINGS max_block_size = 100000
),
actual_slots AS (
    SELECT slot FROM blocks 
    WHERE timestamp_utc >= now() - INTERVAL 1 DAY
)
SELECT 
    count() as missed_slots,
    (SELECT count() FROM actual_slots) as produced_slots,
    round((count() * 100.0) / (count() + (SELECT count() FROM actual_slots)), 2) as miss_rate_percent
FROM expected_slots e
LEFT JOIN actual_slots a ON e.expected_slot + (SELECT min_slot FROM slot_range) = a.slot
WHERE a.slot IS NULL;
```

### Transaction Analysis
```sql
-- Transaction volume over time
SELECT 
    toStartOfDay(timestamp_utc) as day,
    count() as transaction_count,
    count(DISTINCT block_hash) as blocks_with_txs,
    round(count() / count(DISTINCT block_hash), 2) as avg_txs_per_block
FROM transactions 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY day
ORDER BY day;

-- Fee recipient analysis
SELECT 
    fee_recipient,
    count() as transactions,
    count(DISTINCT block_hash) as blocks,
    round(count() / count(DISTINCT block_hash), 2) as avg_txs_per_block
FROM transactions 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
GROUP BY fee_recipient
ORDER BY transactions DESC
LIMIT 20;

-- Transaction patterns by hour
SELECT 
    toHour(timestamp_utc) as hour,
    count() as transaction_count,
    avg(transaction_count) OVER () as avg_hourly_txs
FROM transactions 
WHERE timestamp_utc >= now() - INTERVAL 7 DAY
GROUP BY hour
ORDER BY hour;

-- Gas usage analysis
SELECT 
    b.slot,
    ep.gas_limit,
    ep.gas_used,
    round((ep.gas_used * 100.0) / ep.gas_limit, 2) as gas_utilization_percent,
    ep.base_fee_per_gas,
    ep.transactions_count
FROM blocks b
JOIN execution_payloads ep ON b.slot = ep.slot
WHERE b.timestamp_utc >= now() - INTERVAL 1 DAY
  AND ep.gas_used > 0
ORDER BY gas_utilization_percent DESC
LIMIT 100;
```

### Validator Analysis
```sql
-- Withdrawal patterns
SELECT 
    toStartOfDay(timestamp_utc) as day,
    count() as withdrawal_count,
    sum(amount) as total_withdrawn_gwei,
    avg(amount) as avg_withdrawal_gwei,
    count(DISTINCT validator_index) as unique_validators
FROM withdrawals 
WHERE timestamp_utc >= now() - INTERVAL 7 DAY
GROUP BY day
ORDER BY day;

-- Top validators by withdrawals
SELECT 
    validator_index,
    count() as withdrawal_count,
    sum(amount) as total_withdrawn_gwei,
    avg(amount) as avg_withdrawal_gwei,
    min(timestamp_utc) as first_withdrawal,
    max(timestamp_utc) as last_withdrawal
FROM withdrawals 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY validator_index
ORDER BY total_withdrawn_gwei DESC
LIMIT 50;

-- Attestation participation
SELECT 
    committee_index,
    count() as attestations,
    count(DISTINCT attestation_slot) as slots_attested,
    count(DISTINCT slot) as inclusion_slots,
    avg(slot - attestation_slot) as avg_inclusion_delay
FROM attestations 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
GROUP BY committee_index
ORDER BY attestations DESC;

-- Validator deposits analysis
SELECT 
    toStartOfDay(timestamp_utc) as day,
    count() as deposit_count,
    sum(amount) as total_deposited_gwei,
    count(DISTINCT pubkey) as unique_validators,
    avg(amount) as avg_deposit_gwei
FROM deposits 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY day
ORDER BY day;

-- Voluntary exits analysis
SELECT 
    toStartOfMonth(timestamp_utc) as month,
    count() as exit_count,
    count(DISTINCT validator_index) as unique_validators
FROM voluntary_exits 
WHERE timestamp_utc >= now() - INTERVAL 6 MONTH
GROUP BY month
ORDER BY month;
```

### Sync Committee Analysis
```sql
-- Sync committee participation rates
SELECT 
    toStartOfHour(timestamp_utc) as hour,
    avg(participating_validators) as avg_participation,
    min(participating_validators) as min_participation,
    max(participating_validators) as max_participation,
    count() as total_sync_aggregates
FROM sync_aggregates 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
GROUP BY hour
ORDER BY hour;

-- Sync committee efficiency over time
SELECT 
    toStartOfDay(timestamp_utc) as day,
    avg(participating_validators) as avg_participation,
    stddevPop(participating_validators) as participation_stddev,
    count() as sync_count
FROM sync_aggregates 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY day
ORDER BY day;
```

### Enhanced Slashing Analysis
```sql
-- Proposer slashing events
SELECT 
    slot,
    header_1_proposer_index as slashed_proposer,
    header_1_slot,
    header_2_slot,
    timestamp_utc
FROM proposer_slashings 
WHERE timestamp_utc >= now() - INTERVAL 1 YEAR
ORDER BY timestamp_utc DESC;

-- Attester slashing events with detailed validator analysis
SELECT 
    slot,
    att_1_committee_index,
    att_1_source_epoch,
    att_1_target_epoch,
    att_2_source_epoch,
    att_2_target_epoch,
    att_1_validator_count,
    att_2_validator_count,
    total_slashed_validators,
    timestamp_utc,
    CASE 
        WHEN att_1_target_epoch = att_2_target_epoch THEN 'Double Vote'
        WHEN att_1_source_epoch < att_2_source_epoch AND att_1_target_epoch > att_2_target_epoch THEN 'Surround Vote'
        ELSE 'Other'
    END as slashing_type
FROM attester_slashings 
WHERE timestamp_utc >= now() - INTERVAL 1 YEAR
ORDER BY timestamp_utc DESC;

-- Find attester slashings with most validators
SELECT 
    slot,
    total_slashed_validators,
    att_1_validator_count,
    att_2_validator_count,
    timestamp_utc
FROM attester_slashings 
ORDER BY total_slashed_validators DESC 
LIMIT 20;

-- Check if specific validator was slashed in attester slashing
SELECT 
    slot,
    slashing_index,
    att_1_validator_count,
    att_2_validator_count,
    total_slashed_validators,
    timestamp_utc
FROM attester_slashings 
WHERE has(JSONExtract(att_1_attesting_indices, 'Array(UInt64)'), 464190)
   OR has(JSONExtract(att_2_attesting_indices, 'Array(UInt64)'), 464190);

-- Attester slashing trends over time
SELECT 
    toStartOfMonth(timestamp_utc) as month,
    count() as slashing_events,
    sum(total_slashed_validators) as total_validators_slashed,
    avg(total_slashed_validators) as avg_validators_per_slashing
FROM attester_slashings 
WHERE timestamp_utc >= now() - INTERVAL 1 YEAR
GROUP BY month
ORDER BY month;
```

### BLS Changes Analysis (Capella+)
```sql
-- BLS to execution changes
SELECT 
    toStartOfDay(timestamp_utc) as day,
    count() as change_count,
    count(DISTINCT validator_index) as unique_validators
FROM bls_changes 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY day
ORDER BY day;

-- Most active addresses for BLS changes
SELECT 
    to_execution_address,
    count() as change_count,
    count(DISTINCT validator_index) as unique_validators
FROM bls_changes 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY to_execution_address
ORDER BY change_count DESC
LIMIT 20;
```

### Blob Analysis (Deneb+)
```sql
-- Blob commitment patterns
SELECT 
    toStartOfDay(timestamp_utc) as day,
    count() as commitment_count,
    count(DISTINCT slot) as slots_with_blobs,
    round(count() / count(DISTINCT slot), 2) as avg_commitments_per_slot
FROM blob_commitments 
WHERE timestamp_utc >= now() - INTERVAL 7 DAY
GROUP BY day
ORDER BY day;

-- Blob gas usage
SELECT 
    toStartOfHour(timestamp_utc) as hour,
    avg(blob_gas_used) as avg_blob_gas,
    max(blob_gas_used) as max_blob_gas,
    avg(excess_blob_gas) as avg_excess_blob_gas,
    count() as blocks_with_blobs
FROM execution_payloads 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
  AND blob_gas_used > 0
GROUP BY hour
ORDER BY hour;
```

### State Management Queries
```sql
-- Processing status overview
SELECT 
    network,
    count(DISTINCT era_number) as total_eras,
    countIf(completed_datasets = total_datasets) as fully_completed,
    countIf(processing_datasets > 0) as currently_processing,
    sum(total_rows_inserted) as total_rows
FROM era_processing_progress
GROUP BY network;

-- Failed processing attempts
SELECT 
    era_filename,
    dataset,
    error_message,
    attempt_count,
    created_at
FROM era_processing_state
WHERE status = 'failed'
ORDER BY created_at DESC
LIMIT 20;

-- Dataset completion status
SELECT 
    network,
    dataset,
    completed_eras,
    failed_eras,
    total_rows_inserted,
    highest_completed_era
FROM dataset_processing_progress
WHERE network = 'gnosis'
ORDER BY dataset;

-- Processing performance metrics
SELECT 
    dataset,
    avg(processing_duration_ms) as avg_duration_ms,
    min(processing_duration_ms) as min_duration_ms,
    max(processing_duration_ms) as max_duration_ms,
    avg(rows_inserted) as avg_rows_per_era,
    count() as completed_eras
FROM era_processing_state
WHERE status = 'completed'
  AND processing_duration_ms IS NOT NULL
  AND rows_inserted IS NOT NULL
GROUP BY dataset
ORDER BY avg_duration_ms DESC;

-- Recent processing activity
SELECT 
    toStartOfHour(created_at) as hour,
    count() as processing_attempts,
    countIf(status = 'completed') as completed,
    countIf(status = 'failed') as failed,
    sum(rows_inserted) as total_rows
FROM era_processing_state
WHERE created_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;
```

## Performance Optimization

### Query Optimization
```sql
-- Use partition pruning for time-range queries
SELECT count() 
FROM blocks 
WHERE timestamp_utc >= '2024-01-01' 
  AND timestamp_utc < '2024-02-01'
  AND proposer_index = 12345;

-- Efficient join patterns
SELECT 
    b.slot,
    b.proposer_index,
    ep.gas_used,
    ep.transactions_count
FROM blocks b
JOIN execution_payloads ep ON b.slot = ep.slot
WHERE b.timestamp_utc >= now() - INTERVAL 1 DAY;

-- Use proper data types for large numbers
SELECT 
    slot,
    toUInt64(base_fee_per_gas) as base_fee_numeric
FROM execution_payloads 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
  AND base_fee_per_gas != '';
```

### Index Recommendations
```sql
-- Additional indexes for common queries
ALTER TABLE blocks ADD INDEX idx_proposer_time (proposer_index, timestamp_utc) TYPE minmax;
ALTER TABLE transactions ADD INDEX idx_fee_recipient (fee_recipient) TYPE bloom_filter;
ALTER TABLE withdrawals ADD INDEX idx_validator_time (validator_index, timestamp_utc) TYPE minmax;
ALTER TABLE attestations ADD INDEX idx_committee_time (committee_index, timestamp_utc) TYPE minmax;
ALTER TABLE deposits ADD INDEX idx_pubkey (pubkey) TYPE bloom_filter;
ALTER TABLE attester_slashings ADD INDEX idx_validator_count (total_slashed_validators) TYPE minmax;
```

### Storage Optimization
```sql
-- Check compression ratios
SELECT 
    table,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) as compression_ratio
FROM system.parts 
WHERE database = 'beacon_chain'
GROUP BY table
ORDER BY compressed_size DESC;

-- Partition management
SELECT 
    table,
    partition,
    formatReadableSize(bytes_on_disk) as size,
    rows,
    min_date,
    max_date
FROM system.parts 
WHERE database = 'beacon_chain' 
  AND active = 1
ORDER BY table, min_date;

-- Table statistics
SELECT 
    table,
    count() as partitions,
    sum(rows) as total_rows,
    formatReadableSize(sum(bytes_on_disk)) as total_size,
    min(min_date) as earliest_data,
    max(max_date) as latest_data
FROM system.parts 
WHERE database = 'beacon_chain' 
  AND active = 1
GROUP BY table
ORDER BY total_rows DESC;
```

## Monitoring and Maintenance

### Health Checks
```sql
-- Check recent data ingestion
SELECT 
    table,
    max(timestamp_utc) as latest_data,
    count() as recent_records,
    now() - max(timestamp_utc) as data_lag
FROM (
    SELECT 'blocks' as table, timestamp_utc FROM blocks WHERE timestamp_utc >= now() - INTERVAL 1 HOUR
    UNION ALL
    SELECT 'transactions' as table, timestamp_utc FROM transactions WHERE timestamp_utc >= now() - INTERVAL 1 HOUR
    UNION ALL
    SELECT 'attestations' as table, timestamp_utc FROM attestations WHERE timestamp_utc >= now() - INTERVAL 1 HOUR
    UNION ALL
    SELECT 'withdrawals' as table, timestamp_utc FROM withdrawals WHERE timestamp_utc >= now() - INTERVAL 1 HOUR
    UNION ALL
    SELECT 'attester_slashings' as table, timestamp_utc FROM attester_slashings WHERE timestamp_utc >= now() - INTERVAL 1 HOUR
)
GROUP BY table
ORDER BY table;

-- Processing lag detection
SELECT 
    network,
    dataset,
    highest_completed_era,
    now() - max(completed_at) as time_since_last_completion
FROM dataset_processing_progress dp
JOIN era_processing_state eps ON dp.network = eps.network AND dp.dataset = eps.dataset
WHERE eps.status = 'completed'
GROUP BY network, dataset, highest_completed_era
HAVING time_since_last_completion > INTERVAL 1 HOUR;

-- Check for data quality issues
SELECT 
    'blocks_with_zero_timestamp' as issue,
    count() as count
FROM blocks 
WHERE timestamp_utc = toDateTime(0)
  AND slot > 0

UNION ALL

SELECT 
    'execution_payloads_without_blocks' as issue,
    count() as count
FROM execution_payloads ep
LEFT JOIN blocks b ON ep.slot = b.slot
WHERE b.slot IS NULL

UNION ALL

SELECT 
    'transactions_with_empty_hash' as issue,
    count() as count
FROM transactions 
WHERE transaction_hash = ''

UNION ALL

SELECT 
    'attestations_future_slots' as issue,
    count() as count
FROM attestations 
WHERE attestation_slot > slot

UNION ALL

SELECT 
    'attester_slashings_with_zero_validators' as issue,
    count() as count
FROM attester_slashings 
WHERE total_slashed_validators = 0;
```

### Performance Monitoring
```sql
-- Query performance analysis
SELECT 
    query,
    query_duration_ms,
    memory_usage,
    read_rows,
    read_bytes,
    written_rows,
    user
FROM system.query_log 
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND query_duration_ms > 5000  -- Queries taking more than 5 seconds
ORDER BY query_duration_ms DESC 
LIMIT 20;

-- Resource usage by table
SELECT 
    table,
    sum(rows) as total_rows,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
    round(sum(data_compressed_bytes) * 100.0 / 
          (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = 'beacon_chain'), 2) as size_percentage
FROM system.parts 
WHERE database = 'beacon_chain' 
  AND active = 1
GROUP BY table
ORDER BY sum(data_compressed_bytes) DESC;

-- Most active queries
SELECT 
    normalizeQuery(query) as normalized_query,
    count() as query_count,
    avg(query_duration_ms) as avg_duration_ms,
    sum(read_rows) as total_read_rows,
    sum(memory_usage) as total_memory_usage
FROM system.query_log 
WHERE event_time >= now() - INTERVAL 1 DAY
  AND type = 'QueryFinish'
GROUP BY normalized_query
HAVING query_count > 10
ORDER BY total_memory_usage DESC
LIMIT 20;
```

### Cleanup Operations
```sql
-- Remove old partitions (example: keep 1 year)
ALTER TABLE blocks DROP PARTITION '202301';  -- January 2023
ALTER TABLE transactions DROP PARTITION '202301';
ALTER TABLE attestations DROP PARTITION '202301';
ALTER TABLE attester_slashings DROP PARTITION '202301';

-- Clean up failed processing attempts older than 7 days
ALTER TABLE era_processing_state DELETE 
WHERE status = 'failed' 
  AND created_at < now() - INTERVAL 7 DAY;

-- Remove duplicate entries (if any)
OPTIMIZE TABLE blocks FINAL;
OPTIMIZE TABLE transactions FINAL;
OPTIMIZE TABLE attestations FINAL;
OPTIMIZE TABLE attester_slashings FINAL;

-- Check for orphaned records
SELECT 
    'transactions_without_execution_payload' as issue,
    count() as count
FROM transactions t
LEFT JOIN execution_payloads ep ON t.slot = ep.slot
WHERE ep.slot IS NULL

UNION ALL

SELECT 
    'withdrawals_without_execution_payload' as issue,
    count() as count
FROM withdrawals w
LEFT JOIN execution_payloads ep ON w.slot = ep.slot
WHERE ep.slot IS NULL

UNION ALL

SELECT 
    'attester_slashings_without_blocks' as issue,
    count() as count
FROM attester_slashings ats
LEFT JOIN blocks b ON ats.slot = b.slot
WHERE b.slot IS NULL;
```

## Troubleshooting

### Common Issues

**Connection Timeouts**:
```bash
# Increase timeout settings
export CLICKHOUSE_CONNECT_TIMEOUT=60
export CLICKHOUSE_SEND_RECEIVE_TIMEOUT=300

# Test connection
clickhouse-client --host $CLICKHOUSE_HOST --secure --password
```

**Memory Issues**:
```sql
-- Check memory usage
SELECT 
    query,
    memory_usage,
    formatReadableSize(memory_usage) as memory_usage_readable,
    peak_memory_usage,
    formatReadableSize(peak_memory_usage) as peak_memory_readable
FROM system.processes 
WHERE memory_usage > 1000000000;  -- > 1GB

-- Check system memory
SELECT 
    metric,
    value,
    formatReadableSize(value) as readable_value
FROM system.asynchronous_metrics 
WHERE metric LIKE 'Memory%'
ORDER BY metric;
```

**Slow Queries**:
```sql
-- Find slow queries
SELECT 
    query,
    query_duration_ms,
    read_rows,
    read_bytes,
    memory_usage,
    formatReadableSize(memory_usage) as memory_readable
FROM system.query_log 
WHERE query_duration_ms > 10000  -- > 10 seconds
  AND event_time >= now() - INTERVAL 1 HOUR
ORDER BY query_duration_ms DESC 
LIMIT 10;

-- Check for queries with high memory usage
SELECT 
    query,
    memory_usage,
    formatReadableSize(memory_usage) as memory_readable,
    query_duration_ms
FROM system.query_log 
WHERE memory_usage > 5000000000  -- > 5GB
  AND event_time >= now() - INTERVAL 1 HOUR
ORDER BY memory_usage DESC 
LIMIT 10;
```

**Data Inconsistency**:
```sql
-- Check for missing data relationships
SELECT 
    b.slot,
    b.timestamp_utc,
    CASE WHEN ep.slot IS NULL THEN 'Missing execution_payload' ELSE 'OK' END as execution_status,
    CASE WHEN sa.slot IS NULL THEN 'Missing sync_aggregate' ELSE 'OK' END as sync_status
FROM blocks b
LEFT JOIN execution_payloads ep ON b.slot = ep.slot
LEFT JOIN sync_aggregates sa ON b.slot = sa.slot
WHERE b.timestamp_utc >= now() - INTERVAL 1 DAY
  AND (ep.slot IS NULL OR sa.slot IS NULL)
ORDER BY b.slot DESC
LIMIT 100;
```

### Performance Tuning

**ClickHouse Settings**:
```sql
-- Optimize for Era Parser workloads
SET max_memory_usage = 10000000000; -- 10GB
SET max_threads = 8;
SET max_insert_block_size = 100000;
SET insert_quorum = 0;
SET insert_quorum_timeout = 0;
SET send_timeout = 300;
SET receive_timeout = 300;
```

**Connection Pool Settings**:
```python
# Era Parser automatically uses optimized settings
settings = {
    'max_insert_block_size': 100000,
    'insert_quorum': 0,
    'async_insert': 0,
    'max_execution_time': 300,
    'max_memory_usage': 10000000000,  # 10GB
}
```

## Advanced Features

### Custom Materialized Views
```sql
-- Hourly block statistics
CREATE MATERIALIZED VIEW beacon_chain.blocks_hourly_stats
ENGINE = SummingMergeTree()
ORDER BY (network, hour, proposer_index)
AS SELECT
    'gnosis' as network,
    toStartOfHour(timestamp_utc) as hour,
    proposer_index,
    count() as blocks_proposed,
    1 as total_blocks
FROM beacon_chain.blocks
GROUP BY hour, proposer_index;

-- Daily withdrawal summary
CREATE MATERIALIZED VIEW beacon_chain.withdrawals_daily_summary
ENGINE = SummingMergeTree()
ORDER BY (day, validator_index)
AS SELECT
    toDate(timestamp_utc) as day,
    validator_index,
    count() as withdrawal_count,
    sum(amount) as total_amount_gwei
FROM beacon_chain.withdrawals
GROUP BY day, validator_index;

-- Validator performance metrics
CREATE MATERIALIZED VIEW beacon_chain.validator_performance_daily
ENGINE = ReplacingMergeTree()
ORDER BY (day, validator_index)
AS SELECT
    toDate(b.timestamp_utc) as day,
    b.proposer_index as validator_index,
    count() as blocks_proposed,
    avg(ep.gas_used) as avg_gas_used,
    sum(ep.transactions_count) as total_transactions,
    avg(ep.transactions_count) as avg_transactions
FROM beacon_chain.blocks b
LEFT JOIN beacon_chain.execution_payloads ep ON b.slot = ep.slot
GROUP BY day, validator_index;

-- Daily slashing summary
CREATE MATERIALIZED VIEW beacon_chain.slashing_daily_summary
ENGINE = SummingMergeTree()
ORDER BY (day, slashing_type)
AS SELECT
    toDate(timestamp_utc) as day,
    'attester' as slashing_type,
    count() as slashing_events,
    sum(total_slashed_validators) as total_validators_slashed
FROM beacon_chain.attester_slashings
GROUP BY day
UNION ALL
SELECT
    toDate(timestamp_utc) as day,
    'proposer' as slashing_type,
    count() as slashing_events,
    count() as total_validators_slashed  -- Each proposer slashing affects 1 validator
FROM beacon_chain.proposer_slashings
GROUP BY day;
```

### Data Export
```sql
-- Export to files for external analysis
SELECT 
    slot,
    proposer_index,
    timestamp_utc,
    gas_used,
    transactions_count
FROM blocks b
JOIN execution_payloads ep ON b.slot = ep.slot
WHERE timestamp_utc >= '2024-01-01'
  AND timestamp_utc < '2024-02-01'
FORMAT CSVWithNames
INTO OUTFILE '/tmp/blocks_january_2024.csv';

-- Export validator performance data
SELECT 
    validator_index,
    day,
    blocks_proposed,
    total_transactions,
    avg_gas_used
FROM validator_performance_daily
WHERE day >= '2024-01-01'
FORMAT Parquet
INTO OUTFILE '/tmp/validator_performance_2024.parquet';

-- Export attester slashing details with validator indices
SELECT 
    slot,
    total_slashed_validators,
    att_1_attesting_indices,
    att_2_attesting_indices,
    timestamp_utc
FROM attester_slashings
WHERE timestamp_utc >= '2024-01-01'
FORMAT JSONEachRow
INTO OUTFILE '/tmp/attester_slashings_2024.jsonl';
```

### Integration with Other Tools

**Grafana Dashboard Queries**:
```sql
-- Block production rate over time
SELECT 
    $__timeGroup(timestamp_utc, $__interval) as time,
    count() as blocks_per_interval
FROM blocks 
WHERE $__timeFilter(timestamp_utc)
GROUP BY time
ORDER BY time;

-- Transaction volume
SELECT 
    $__timeGroup(timestamp_utc, $__interval) as time,
    count() as transaction_count
FROM transactions 
WHERE $__timeFilter(timestamp_utc)
GROUP BY time
ORDER BY time;

-- Gas utilization
SELECT 
    $__timeGroup(timestamp_utc, $__interval) as time,
    avg(gas_used * 100.0 / gas_limit) as avg_gas_utilization
FROM execution_payloads 
WHERE $__timeFilter(timestamp_utc)
  AND gas_limit > 0
GROUP BY time
ORDER BY time;

-- Slashing events over time
SELECT 
    $__timeGroup(timestamp_utc, $__interval) as time,
    count() as attester_slashings,
    sum(total_slashed_validators) as total_validators_slashed
FROM attester_slashings 
WHERE $__timeFilter(timestamp_utc)
GROUP BY time
ORDER BY time;
```

**Jupyter Notebook Integration**:
```python
import clickhouse_connect
import pandas as pd
import matplotlib.pyplot as plt
import json

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='your-host.com',
    password='your-password',
    database='beacon_chain'
)

# Query recent blocks with gas data
df = client.query_df("""
    SELECT 
        b.slot, 
        b.proposer_index, 
        b.timestamp_utc, 
        ep.gas_used,
        ep.gas_limit,
        ep.transactions_count
    FROM blocks b
    JOIN execution_payloads ep ON b.slot = ep.slot
    WHERE b.timestamp_utc >= now() - INTERVAL 1 DAY
    ORDER BY b.slot
""")

# Plot gas utilization over time
df['gas_utilization'] = (df['gas_used'] / df['gas_limit']) * 100
df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])

plt.figure(figsize=(12, 6))
plt.plot(df['timestamp_utc'], df['gas_utilization'])
plt.title('Gas Utilization Over Time')
plt.xlabel('Time')
plt.ylabel('Gas Utilization (%)')
plt.show()

# Analyze attester slashing data
slashing_df = client.query_df("""
    SELECT 
        slot,
        total_slashed_validators,
        att_1_attesting_indices,
        att_2_attesting_indices,
        timestamp_utc
    FROM attester_slashings
    WHERE timestamp_utc >= now() - INTERVAL 30 DAY
""")

# Parse JSON validator indices
def parse_indices(indices_json):
    try:
        return json.loads(indices_json)
    except:
        return []

slashing_df['att_1_indices'] = slashing_df['att_1_attesting_indices'].apply(parse_indices)
slashing_df['att_2_indices'] = slashing_df['att_2_attesting_indices'].apply(parse_indices)

# Analyze validator overlap in slashings
overlaps = []
for _, row in slashing_df.iterrows():
    att_1_set = set(row['att_1_indices'])
    att_2_set = set(row['att_2_indices'])
    overlap = len(att_1_set.intersection(att_2_set))
    overlaps.append(overlap)

slashing_df['validator_overlap'] = overlaps

print("Attester Slashing Analysis:")
print(f"Total slashing events: {len(slashing_df)}")
print(f"Total validators slashed: {slashing_df['total_slashed_validators'].sum()}")
print(f"Average validators per slashing: {slashing_df['total_slashed_validators'].mean():.2f}")
print(f"Average validator overlap: {slashing_df['validator_overlap'].mean():.2f}")
```

## Best Practices

### Data Loading
1. **Use Remote Processing**: More efficient than local file processing
2. **Enable Resume**: Prevents reprocessing on interruption
3. **Monitor State**: Check processing status regularly with `era-parser --era-status`
4. **Batch Appropriately**: Let Era Parser handle batch sizing automatically

### Query Patterns
1. **Use Time Partitions**: Always include time filters when possible
2. **Leverage Indexes**: Use indexed columns in WHERE clauses
3. **Join Efficiently**: Join on slot for best performance between tables
4. **Aggregate Wisely**: Use materialized views for frequently computed aggregations

### Maintenance
1. **Monitor Disk Usage**: ClickHouse can grow quickly with beacon chain data
2. **Clean Up Failures**: Regularly clean failed processing attempts
3. **Optimize Tables**: Run OPTIMIZE after large deletes or schema changes
4. **Backup State**: Era processing state is valuable for resume capability

### Security
1. **Use Secure Connections**: Always set CLICKHOUSE_SECURE=true for production
2. **Rotate Passwords**: Regularly update database credentials
3. **Limit Access**: Use dedicated database users with minimal required permissions
4. **Monitor Access**: Track queries and connection attempts in system logs

## Migration and Upgrades

### Schema Updates
When Era Parser adds new fields or tables:

```sql
-- Check current schema version
SHOW TABLES FROM beacon_chain;

-- Add new columns (example for future fork)
ALTER TABLE blocks ADD COLUMN new_fork_field String DEFAULT '';

-- Update existing data if needed
ALTER TABLE blocks UPDATE new_fork_field = 'default_value' WHERE new_fork_field = '';

-- Create new tables for new data types
CREATE TABLE beacon_chain.new_data_type (
    slot UInt64,
    new_field String,
    timestamp_utc DateTime,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot);
```

### Data Migration
```sql
-- Migrate data between clusters or databases
INSERT INTO new_cluster.beacon_chain.blocks 
SELECT * FROM beacon_chain.blocks 
WHERE timestamp_utc >= '2024-01-01';

-- Migrate specific time ranges
INSERT INTO backup_database.blocks
SELECT * FROM blocks
WHERE timestamp_utc >= '2023-01-01' 
  AND timestamp_utc < '2024-01-01';

-- Verify migration
SELECT 
    count() as total_rows,
    min(timestamp_utc) as earliest,
    max(timestamp_utc) as latest
FROM backup_database.blocks;
```

### Backup Strategies
```sql
-- Create backup tables
CREATE TABLE beacon_chain.blocks_backup AS beacon_chain.blocks;

-- Export critical data
SELECT * FROM era_processing_state 
WHERE network = 'gnosis' 
FORMAT Native 
INTO OUTFILE '/backup/era_processing_state_gnosis.native';

-- Restore from backup
INSERT INTO era_processing_state 
FROM INFILE '/backup/era_processing_state_gnosis.native' 
FORMAT Native;
```