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

# Process with force mode (clean and reprocess)
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --force
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

-- Transaction hashes (Bellatrix+)
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
ORDER BY (slot, transaction_index);

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
ORDER BY (slot, attestation_index);

-- Validator deposits
CREATE TABLE beacon_chain.deposits (
    slot UInt64,
    deposit_index UInt64,
    pubkey String,
    withdrawal_credentials String,
    amount UInt64,
    signature String,
    proof String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, deposit_index);

-- Voluntary exits
CREATE TABLE beacon_chain.voluntary_exits (
    slot UInt64,
    exit_index UInt64,
    signature String,
    epoch UInt64,
    validator_index UInt64,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, exit_index, validator_index);

-- Proposer slashings
CREATE TABLE beacon_chain.proposer_slashings (
    slot UInt64,
    slashing_index UInt64,
    header_1_slot UInt64,
    header_1_proposer_index UInt64,
    header_1_parent_root String,
    header_1_state_root String,
    header_1_body_root String,
    header_1_signature String,
    header_2_slot UInt64,
    header_2_proposer_index UInt64,
    header_2_parent_root String,
    header_2_state_root String,
    header_2_body_root String,
    header_2_signature String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, slashing_index);

-- Attester slashings with full data
CREATE TABLE beacon_chain.attester_slashings (
    slot UInt64,
    slashing_index UInt64,
    att_1_slot UInt64,
    att_1_committee_index UInt64,
    att_1_beacon_block_root String,
    att_1_source_epoch UInt64,
    att_1_source_root String,
    att_1_target_epoch UInt64,
    att_1_target_root String,
    att_1_signature String,
    att_1_attesting_indices String,  -- JSON array
    att_1_validator_count UInt64,
    att_2_slot UInt64,
    att_2_committee_index UInt64,
    att_2_beacon_block_root String,
    att_2_source_epoch UInt64,
    att_2_source_root String,
    att_2_target_epoch UInt64,
    att_2_target_root String,
    att_2_signature String,
    att_2_attesting_indices String,  -- JSON array
    att_2_validator_count UInt64,
    total_slashed_validators UInt64,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, slashing_index);

-- BLS to execution changes (Capella+)
CREATE TABLE beacon_chain.bls_changes (
    slot UInt64,
    change_index UInt64,
    validator_index UInt64,
    from_bls_pubkey String,
    to_execution_address String,
    signature String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, change_index, validator_index);

-- Blob KZG commitments (Deneb+)
CREATE TABLE beacon_chain.blob_kzg_commitments (
    slot UInt64,
    commitment_index UInt64,
    commitment String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, commitment_index);

-- Execution requests (Electra+)
CREATE TABLE beacon_chain.execution_requests (
    slot UInt64,
    request_index UInt64,
    request_type String,
    request_data String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, request_index);

-- Era processing state tracking
CREATE TABLE beacon_chain.era_completion (
    network String,
    era_number UInt32,
    status Enum('processing', 'completed', 'failed'),
    slot_start UInt64,
    slot_end UInt64,
    total_records UInt64,
    datasets_processed Array(String),
    processing_started_at DateTime,
    completed_at DateTime,
    error_message String,
    retry_count UInt8,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (network, era_number);
```

## Processing Modes

### Normal Mode (Default)
Processes all specified eras:
```bash
era-parser --remote gnosis 1000-1100 all-blocks --export clickhouse
```

### Force Mode
Cleans existing data first, then reprocesses everything:
```bash
era-parser --remote gnosis 1000-1100 all-blocks --export clickhouse --force
```

**Force Mode Process:**
1. Identifies eras with existing data
2. Cleans all data for those eras from all tables
3. Removes completion records  
4. Processes all eras from scratch

**Force Mode Use Cases:**
- Data corruption recovery
- Schema changes requiring reprocessing
- Testing with clean state
- Regenerating specific data ranges

## Era Completion Tracking

Monitor processing status with the `era_completion` table:

```sql
-- Check completion status
SELECT 
    status,
    count() as era_count,
    sum(total_records) as total_records
FROM era_completion 
WHERE network = 'gnosis'
GROUP BY status;

-- Recent processing activity
SELECT 
    era_number,
    status,
    total_records,
    completed_at,
    error_message
FROM era_completion 
WHERE network = 'gnosis' 
  AND processing_started_at >= now() - INTERVAL 1 DAY
ORDER BY era_number DESC;

-- Failed eras needing attention
SELECT 
    era_number,
    retry_count,
    error_message,
    completed_at
FROM era_completion 
WHERE network = 'gnosis' 
  AND status = 'failed'
ORDER BY era_number;
```

## Data Analysis Examples

### Block Analysis
```sql
-- Block production statistics
SELECT 
    proposer_index,
    count() as blocks_proposed,
    avg(attestation_count) as avg_attestations,
    min(timestamp_utc) as first_block,
    max(timestamp_utc) as last_block
FROM blocks 
WHERE timestamp_utc >= now() - INTERVAL 7 DAY
GROUP BY proposer_index
ORDER BY blocks_proposed DESC
LIMIT 20;

-- Block timing analysis
SELECT 
    toStartOfHour(timestamp_utc) as hour,
    count() as block_count,
    avg(attestation_count) as avg_attestations,
    avg(deposit_count) as avg_deposits
FROM blocks 
WHERE timestamp_utc >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;

-- Missing slots analysis
SELECT 
    slot,
    slot - lag(slot) OVER (ORDER BY slot) - 1 as missing_slots
FROM blocks 
WHERE timestamp_utc >= now() - INTERVAL 1 DAY
  AND missing_slots > 0
ORDER BY missing_slots DESC;
```

### Transaction Analysis  
```sql
-- Transaction volume by fee recipient
SELECT 
    fee_recipient,
    count() as transactions,
    count() / (SELECT count() FROM transactions WHERE timestamp_utc >= now() - INTERVAL 1 DAY) * 100 as percentage,
    avg(gas_limit) as avg_gas_limit,
    avg(gas_used) as avg_gas_used
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

-- Top validators by BLS changes
SELECT 
    validator_index,
    count() as change_count,
    groupArray(to_execution_address) as execution_addresses,
    min(timestamp_utc) as first_change,
    max(timestamp_utc) as last_change
FROM bls_changes 
WHERE timestamp_utc >= now() - INTERVAL 30 DAY
GROUP BY validator_index
HAVING change_count > 1
ORDER BY change_count DESC;
```

### Blob Analysis (Deneb+)
```sql
-- Blob commitment patterns
SELECT 
    toStartOfHour(timestamp_utc) as hour,
    count() as total_commitments,
    count(DISTINCT slot) as blocks_with_blobs,
    avg(count()) OVER () as avg_hourly_commitments
FROM blob_kzg_commitments 
WHERE timestamp_utc >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;

-- Blocks with most blob commitments
SELECT 
    slot,
    count() as commitment_count,
    timestamp_utc
FROM blob_kzg_commitments 
WHERE timestamp_utc >= now() - INTERVAL 7 DAY
GROUP BY slot, timestamp_utc
ORDER BY commitment_count DESC
LIMIT 20;
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
    count() as rows_today
FROM (
    SELECT 'blocks' as table, timestamp_utc FROM blocks WHERE timestamp_utc >= today()
    UNION ALL
    SELECT 'transactions' as table, timestamp_utc FROM transactions WHERE timestamp_utc >= today()
    UNION ALL
    SELECT 'attestations' as table, timestamp_utc FROM attestations WHERE timestamp_utc >= today()
    UNION ALL
    SELECT 'withdrawals' as table, timestamp_utc FROM withdrawals WHERE timestamp_utc >= today()
    UNION ALL
    SELECT 'attester_slashings' as table, timestamp_utc FROM attester_slashings WHERE timestamp_utc >= today()
) 
GROUP BY table
ORDER BY table;

-- Processing performance
SELECT 
    toStartOfHour(completed_at) as hour,
    count() as eras_completed,
    avg(retry_count) as avg_retries,
    countIf(status = 'failed') as failed,
    sum(total_records) as total_rows
FROM era_completion
WHERE completed_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;

-- Processing lag detection
SELECT 
    network,
    max(era_number) as highest_completed_era,
    now() - max(completed_at) as time_since_last_completion
FROM era_completion
WHERE status = 'completed'
GROUP BY network
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
DELETE FROM era_completion
WHERE status = 'failed' 
  AND completed_at < now() - INTERVAL 7 DAY;

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
    formatReadableSize(read_bytes) as read_bytes,
    formatReadableSize(memory_usage) as memory_usage
FROM system.query_log 
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND query_duration_ms > 10000  -- > 10 seconds
ORDER BY query_duration_ms DESC
LIMIT 10;

-- Query optimization recommendations
EXPLAIN SYNTAX 
SELECT * FROM blocks WHERE timestamp_utc >= now() - INTERVAL 1 DAY;

-- Check if indexes are being used
EXPLAIN indexes = 1
SELECT * FROM blocks WHERE proposer_index = 12345;
```

**Data Inconsistencies**:
```sql
-- Check for missing data
SELECT 
    'Missing execution payloads' as issue,
    count() as missing_count
FROM blocks b
LEFT JOIN execution_payloads ep ON b.slot = ep.slot
WHERE ep.slot IS NULL 
  AND b.timestamp_utc >= toDateTime('2022-09-15');  -- Post-merge only

-- Verify slot continuity
SELECT 
    slot,
    prev_slot,
    slot - prev_slot as gap
FROM (
    SELECT 
        slot,
        lag(slot) OVER (ORDER BY slot) as prev_slot
    FROM blocks 
    WHERE timestamp_utc >= now() - INTERVAL 1 DAY
)
WHERE gap > 1
ORDER BY gap DESC;
```

## Production Tips

### Data Pipeline Setup
```bash
# Initial historical load with force mode
era-parser --remote gnosis 0-2000 all-blocks --export clickhouse --force

# Incremental processing (normal mode)
era-parser --remote gnosis 2001+ all-blocks --export clickhouse

# Recovery from failures
era-parser --era-failed gnosis  # Check failures
era-parser --remote gnosis <failed-range> all-blocks --export clickhouse --force
```

### Monitoring Script
```bash
#!/bin/bash
# Check for failed eras and alert
failed_count=$(clickhouse-client --query "SELECT count() FROM era_completion WHERE status = 'failed'")
if [ "$failed_count" -gt 0 ]; then
    echo "WARNING: $failed_count failed eras detected"
    # Send alert
fi

# Check data freshness
latest_block=$(clickhouse-client --query "SELECT max(timestamp_utc) FROM blocks")
current_time=$(date +%s)
latest_block_ts=$(date -d "$latest_block" +%s)
lag_minutes=$(( (current_time - latest_block_ts) / 60 ))

if [ "$lag_minutes" -gt 60 ]; then
    echo "WARNING: Data is $lag_minutes minutes behind"
    # Send alert
fi
```

### Backup Strategy
```sql
-- Backup critical tables
BACKUP TABLE beacon_chain.era_completion TO S3('s3://backup-bucket/era_completion/');
BACKUP TABLE beacon_chain.blocks TO S3('s3://backup-bucket/blocks/');

-- Incremental backup
BACKUP TABLE beacon_chain.blocks 
WHERE timestamp_utc >= today() - INTERVAL 1 DAY
TO S3('s3://backup-bucket/blocks_incremental/');
```

### Performance Tuning
```sql
-- Optimize tables regularly
OPTIMIZE TABLE blocks FINAL;
OPTIMIZE TABLE transactions FINAL;
OPTIMIZE TABLE attestations FINAL;

-- Monitor partition sizes
SELECT 
    table,
    partition,
    formatReadableSize(bytes_on_disk) as size
FROM system.parts 
WHERE database = 'beacon_chain'
  AND bytes_on_disk > 1000000000  -- > 1GB
ORDER BY bytes_on_disk DESC;

-- Adjust merge settings for high-volume tables
ALTER TABLE blocks MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 161061273600;
```

---

For more details on era file formats and parsing, see [ERA_FILE_FORMAT.md](ERA_FILE_FORMAT.md) and [PARSED_FIELDS.md](PARSED_FIELDS.md).