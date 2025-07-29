# Remote Era Processing

This document covers Era Parser's advanced remote processing capabilities, including S3 integration, bulk discovery, unified state management, and optimization strategies.

## Overview

Remote processing allows Era Parser to automatically discover, download, and process era files from remote storage without manual intervention. This enables large-scale processing of historical beacon chain data with sophisticated state tracking and unified completion management.

## Quick Start

### Basic Setup
```bash
# Required environment variable
export ERA_BASE_URL=https://era-files.com

# Optional configuration
export ERA_DOWNLOAD_DIR=./temp_era_files
export ERA_CLEANUP_AFTER_PROCESS=true
export ERA_MAX_RETRIES=3
```

### Simple Examples
```bash
# Process single era
era-parser --remote gnosis 1082 all-blocks --export clickhouse

# Process era range
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Process from era until end
era-parser --remote gnosis 1082+ all-blocks --export clickhouse

# Force reprocess (clean and reprocess all)
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --force

# Download only (no processing)
era-parser --remote gnosis 1082-1100 --download-only
```

## Era Range Formats

| Format | Description | Example Usage |
|--------|-------------|---------------|
| `1082` | Single era | Process only era 1082 |
| `1082-1100` | Era range (inclusive) | Process eras 1082 through 1100 |
| `1082+` | Open range | Process from 1082 until no more files found |

## Configuration

### Environment Variables

**Required**:
- `ERA_BASE_URL`: Base URL for era files (S3 bucket or HTTP server)

**Optional**:
```bash
ERA_DOWNLOAD_DIR=./temp_era_files      # Download directory (default: system temp)
ERA_CLEANUP_AFTER_PROCESS=true         # Delete files after processing (default: true)
ERA_MAX_RETRIES=3                      # Download retry attempts (default: 3)
ERA_MAX_CONCURRENT_DOWNLOADS=10        # Parallel download limit (default: 10)
```

### Docker Environment
```yaml
# docker-compose.yml
services:
  era-parser:
    environment:
      - ERA_BASE_URL=${ERA_BASE_URL}
      - ERA_DOWNLOAD_DIR=/app/temp_era_files
      - ERA_CLEANUP_AFTER_PROCESS=true
      - CLICKHOUSE_HOST=${CLICKHOUSE_HOST}
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
```

## S3 Integration

### Optimized S3 Discovery

Era Parser automatically detects S3 URLs and uses optimized bulk listing:

```bash
# S3 URLs are automatically detected
export ERA_BASE_URL=https://era-files-bucket.s3.eu-central-1.amazonaws.com

# Bulk listing discovers thousands of files instantly
era-parser --remote gnosis 0+ all-blocks --export clickhouse
```

**S3 Optimization Features**:
- **Bulk Listing**: Single API call discovers 1000+ files
- **Pagination**: Handles buckets with 10,000+ era files
- **Prefix Filtering**: Only fetches files for specified network
- **Parallel Processing**: Concurrent downloads and processing

### S3 Performance
```
Traditional Discovery: 2000 files × 100ms = 200 seconds
S3 Bulk Listing: 2000 files ÷ 1000 per page = 2 seconds
```

## Discovery Process

### Phase 1: Fast Discovery
```bash
🚀 Fast discovery starting from era 0
📦 Using S3 bulk listing for ultra-fast discovery
   🔍 Fetching S3 bucket listing (page 1)...
   📊 Page 1: Found 1000 era files
   🔍 Fetching S3 bucket listing (page 2)...
   📊 Page 2: Found 1000 era files
   🔍 Fetching S3 bucket listing (page 3)...
   📊 Page 3: Found 613 era files
   🎯 Total found: 2613 era files across 3 pages
```

### Phase 2: Unified State Filtering
```bash
📋 Discovered 2613 available eras
🔍 Checking for completed eras...
✅ Found 138 completed eras for gnosis
📋 Skipping 138 completed eras, processing 2475 incomplete eras
🚀 Will process eras 0 to 2612
```

### Phase 3: Processing
```bash
📈 Processing era 1082 (1/2475)
   📥 Downloading era 1082
   🔧 Processing with command: 'all-blocks'
   🗄️  Export to: ClickHouse
   📊 Loading all data types to ClickHouse:
   - blocks: 8192 records
   - transactions: 45623 records
   - attestations: 16388 records
   ✅ Era 1082 completed: 70203 records, 13 datasets
```

## Unified State Management

### Completion Tracking

The new system uses `EraStateManager` for unified state tracking:

```python
# All state operations go through unified manager
state_manager = EraStateManager()

# Record processing start
state_manager.record_era_start(era_number, network)

# Record successful completion
state_manager.record_era_completion(era_number, network, datasets_processed, total_records)

# Record failure
state_manager.record_era_failure(era_number, network, error_message)
```

### State Tracking Commands
```bash
# Check processing status using unified state
era-parser --era-status gnosis

# View failed processing attempts
era-parser --era-failed gnosis

# Clean up failed entries
era-parser --clean-failed-eras gnosis

# Clean specific era range (force clean)
era-parser --remote --force-clean gnosis 1082-1100
```

### Era Completion Table

State is tracked in a simplified `era_completion` table:

```sql
CREATE TABLE era_completion (
    network String,
    era_number UInt32,
    status Enum8('processing' = 0, 'completed' = 1, 'failed' = 2),
    slot_start UInt32,
    slot_end UInt32,
    total_records UInt64,
    datasets_processed Array(String),
    processing_started_at DateTime,
    completed_at DateTime DEFAULT now(),
    error_message String DEFAULT '',
    retry_count UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY network
ORDER BY (network, era_number);
```

## Processing Modes

### Normal Mode (Default)
Processes eras not yet completed:
```bash
# Skips already completed eras
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse
```

**Process**:
1. Discover available eras
2. Check completion status using unified state manager
3. Process only incomplete eras
4. Record completion atomically

### Force Mode
Cleans and reprocesses everything:
```bash
# Clean and reprocess all eras in range
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --force
```

**Process**:
1. Discover available eras
2. Clean existing data for ALL eras in range
3. Process all eras from scratch
4. Record new completion status

## Data Cleanup in Force Mode

### Comprehensive Cleaning

Force mode performs complete data cleanup:

```python
def clean_era_completely(self, network: str, era_number: int) -> None:
    """Remove ALL data for an era's slot range"""
    start_slot, end_slot = self.get_era_slot_range(era_number, network)
    
    # Delete from all beacon chain tables
    for table in self.ALL_DATASETS:
        self.client.command(f"""
            DELETE FROM {self.database}.{table} 
            WHERE slot >= {start_slot} AND slot <= {end_slot}
        """)
    
    # Remove completion records
    self.client.command(f"""
        DELETE FROM {self.database}.era_completion 
        WHERE network = '{network}' AND era_number = {era_number}
    """)
```

**Tables Cleaned**:
- `blocks`, `sync_aggregates`, `execution_payloads`
- `transactions`, `withdrawals`, `attestations`
- `deposits`, `voluntary_exits`, `proposer_slashings`
- `attester_slashings`, `bls_changes`, `blob_commitments`
- `execution_requests`, `era_completion`

## Advanced Features

### Open-Ended Processing
```bash
# Process from era 1082 until no more files are found
era-parser --remote gnosis 1082+ all-blocks --export clickhouse

# Automatic detection of latest era
era-parser --remote mainnet 0+ all-blocks --export clickhouse
```

**Smart Termination**:
- Stops when 3 consecutive batches have <5 files each
- Handles gaps in era numbering
- Efficient for processing latest data

### Parallel Discovery
```bash
# For non-S3 URLs, uses parallel discovery
export ERA_BASE_URL=https://regular-http-server.com/era-files

# Checks multiple eras concurrently
era-parser --remote gnosis 1000-2000 all-blocks --export clickhouse
```

**Parallel Features**:
- 20 concurrent HTTP requests
- Exponential backoff on failures
- Automatic retry with connection reset

### Atomic Processing

Each era is processed atomically:

```python
def load_all_data_types(self, all_data: Dict[str, List[Dict[str, Any]]]):
    """Load all data types atomically using unified state management"""
    try:
        # 1. Clean FIRST using unified state manager
        self.state_manager.clean_era_data_if_needed(self.era_number, self.network)
        
        # 2. Mark as processing
        self.state_manager.record_era_start(self.era_number, self.network)
        
        # 3. Process all datasets
        datasets_processed = []
        total_records = 0
        
        for dataset, data_list in all_data.items():
            if data_list:
                records_loaded = self.load_data_to_table(data_list, dataset)
                datasets_processed.append(dataset)
                total_records += records_loaded
        
        # 4. Mark as completed
        self.state_manager.record_era_completion(
            self.era_number, self.network, datasets_processed, total_records
        )
        
    except Exception as e:
        # 5. Mark as failed
        self.state_manager.record_era_failure(self.era_number, self.network, str(e))
        raise
```

## Performance Optimization

### Unified Batch Processing

Single global batch size for optimal performance:

```python
# Single global batch size for all operations
GLOBAL_BATCH_SIZE = 100000

def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str) -> int:
    """Optimized bulk loading with single global batch size"""
    if total_records > self.GLOBAL_BATCH_SIZE:
        return self._streaming_bulk_insert(bulk_data, table_name, expected_columns)
    else:
        # Direct insert for small datasets
        self.client.insert(table_name, bulk_data, column_names=expected_columns)
```

### Single Timestamp Approach

All tables use unified timestamp for efficient partitioning:

```python
def _convert_to_datetime(self, value) -> datetime:
    """Robust datetime conversion for ClickHouse DateTime columns"""
    # Handle various timestamp formats and return consistent datetime
    # Single timestamp approach improves query performance
```

### Connection Optimization

Optimized settings for ClickHouse Cloud:

```python
client = clickhouse_connect.get_client(
    # Cloud-optimized settings
    settings={
        'max_insert_block_size': 100000,
        'async_insert': 0,  # Synchronous for predictable behavior
        'max_execution_time': 300,
        'max_memory_usage': 10000000000,  # 10GB
    },
    connect_timeout=60,
    send_receive_timeout=300,
    compress=True,  # Network efficiency
)
```

## Error Handling

### Automatic Recovery
```bash
# Connection failures are automatically retried
⚠️  Download attempt 1/3 failed: Connection timeout
⚠️  Download attempt 2/3 failed: Connection reset
✅ Download attempt 3/3 succeeded
```

### Graceful Degradation
```bash
# Individual era failures don't stop batch processing
❌ Era 1085 failed: Parse error
🧹 Cleaning era 1085 data (slots 8884736-8892927)
✅ Era 1086 completed: 70203 records, 13 datasets
✅ Era 1087 completed: 69874 records, 13 datasets
```

### State Persistence
```bash
# Completion status is atomic - era is either complete or not
✅ Era 1082 marked as 'completed' with 70203 records

# Failed eras are tracked with retry count
❌ Era 1085 marked as 'failed' (attempt 1): Connection timeout
```

## Monitoring and Debugging

### Progress Monitoring
```bash
# Real-time progress with unified state
📈 Processing era 1082 (1/2475)
   📥 Downloading (attempt 1/3)
   ✅ Downloaded: 15MB
   🔧 Processing with command: 'all-blocks'
   🔄 Processing era 1082 atomically
   📊 Loading all data types to ClickHouse:
   - blocks: 8192 records
   - sync_aggregates: 8192 records  
   - transactions: 45623 records
   - attestations: 16388 records
   ✅ Era 1082 completed: 70203 records, 13 datasets
```

### State Management Commands
```bash
# Comprehensive state checking
era-parser --era-status gnosis

# Output:
📊 Era Processing Summary (gnosis)
============================================================
✅ Completed eras: 138
❌ Failed eras: 3
📊 Total records: 9,645,234
```

### Database Optimization
```bash
# Optimize tables after processing
era-parser --remote --optimize

# Clean failed entries
era-parser --clean-failed-eras gnosis
```

## Common Patterns

### Research Workflows
```bash
# Extract all validator data for analysis period
era-parser --remote gnosis 1000-1100 attestations --export clickhouse
era-parser --remote gnosis 1000-1100 deposits --export clickhouse
era-parser --remote gnosis 1000-1100 withdrawals --export clickhouse

# Continuous monitoring of latest data
era-parser --remote gnosis 2500+ all-blocks --export clickhouse
```

### Data Recovery Workflows
```bash
# Force reprocess specific range (clean first)
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --force

# Clean and retry failed eras
era-parser --clean-failed-eras gnosis
era-parser --remote gnosis 1082+ all-blocks --export clickhouse
```

### Testing and Development
```bash
# Test with small range and force mode
era-parser --remote gnosis 1082-1085 all-blocks --export clickhouse --force

# Check state after processing
era-parser --era-status gnosis
era-parser --era-check gnosis 1082
```

## Troubleshooting

### Common Issues

**Configuration Errors**:
```bash
❌ Configuration error: ERA_BASE_URL environment variable is required
# Solution: Set ERA_BASE_URL
export ERA_BASE_URL=https://your-era-files.com
```

**State Management Issues**:
```bash
⏰ Timeout checking completed eras (30s), processing all eras as fallback
# Solution: Check ClickHouse connectivity
era-parser --era-status gnosis
```

**Force Mode Not Working**:
```bash
# Check if data was actually cleaned
era-parser --era-check gnosis 1082

# Verify force mode cleaned data
🧹 Cleaned era 1082 (5 tables had data)
```

### Performance Issues

**Slow Processing**:
```bash
# Check batch size and connection settings
# Single global batch size should be 100,000
📊 Progress: 12.5% (1,250,000 records)
```

**Memory Usage**:
```bash
# Era Parser uses constant memory per era
# Large batches are streamed automatically
🔍 Streaming insert 456,234 records into attestations with batch size 100000
```

### State Issues

**Incomplete State**:
```bash
# Clean failed eras and retry
era-parser --clean-failed-eras gnosis

# Force clean specific range
era-parser --remote --force-clean gnosis 1082-1100
```

**Migration Issues**:
```bash
# Check migration status
era-parser --migrate status

# Run pending migrations
era-parser --migrate run
```