# Remote Era Processing

This document covers Era Parser's advanced remote processing capabilities, including S3 integration, bulk discovery, state management, and optimization strategies.

## Overview

Remote processing allows Era Parser to automatically discover, download, and process era files from remote storage without manual intervention. This enables large-scale processing of historical beacon chain data with sophisticated resume capabilities and state tracking.

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
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume

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

### Phase 2: State Filtering
```bash
📋 Found 138 already processed eras in ClickHouse
📋 Era state filter: 2475 eras remaining after filtering
```

### Phase 3: Processing
```bash
📈 Processing era 1082 (1/2475)
   📥 Downloading era 1082
   🔧 Processing with command: 'all-blocks'
   🗄️  Export to: ClickHouse
   ✅ Successfully processed era 1082
```

## State Management

### Resume Capability

Remote processing integrates with era state management for intelligent resume:

```bash
# Initial run processes eras 1082-1090
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Resume processes remaining eras 1091-1100
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --resume
```

### State Tracking Commands
```bash
# Check processing status
era-parser --era-status gnosis

# View failed processing attempts
era-parser --era-failed gnosis

# Clean up stale processing entries
era-parser --era-cleanup 30

# Clear remote progress (file-based tracking)
era-parser --remote-clear gnosis
```

### Granular Dataset Tracking

Era Parser tracks processing at the dataset level:

```sql
-- Check dataset completion status
SELECT 
    network,
    dataset,
    completed_eras,
    failed_eras,
    highest_completed_era
FROM dataset_processing_progress
WHERE network = 'gnosis';
```

## Processing Modes

### ClickHouse Export (Recommended)
```bash
# Direct export to ClickHouse with state management
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume

# Export specific datasets
era-parser --remote gnosis 1082-1100 transactions --export clickhouse
era-parser --remote gnosis 1082-1100 attestations --export clickhouse
```

**Benefits**:
- Granular state tracking per dataset
- Automatic resume capability
- No local storage requirements
- Optimized for analytics

### File Export
```bash
# Export to separate files per data type
era-parser --remote gnosis 1082-1085 all-blocks analysis.parquet --separate

# Single file with all data
era-parser --remote gnosis 1082 all-blocks complete.json
```

**File Naming**:
```
output/
├── gnosis_analysis_era_01082_blocks.parquet
├── gnosis_analysis_era_01082_transactions.parquet
├── gnosis_analysis_era_01082_withdrawals.parquet
├── gnosis_analysis_era_01082_attestations.parquet
└── gnosis_analysis_era_01082_sync_aggregates.parquet
```

### Download Only
```bash
# Download without processing
era-parser --remote gnosis 1082-1100 --download-only

# Files are saved to ERA_DOWNLOAD_DIR
ls temp_era_files/
# gnosis-01082-fe3b60d1.era
# gnosis-01083-a1b2c3d4.era
# ...
```

## Advanced Features

### Open-Ended Processing
```bash
# Process from era 1082 until no more files are found
era-parser --remote gnosis 1082+ all-blocks --export clickhouse

# Automatic detection of latest era
era-parser --remote mainnet 0+ all-blocks --export clickhouse --resume
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

### Progress Tracking
```bash
# Show remote processing progress
era-parser --remote-progress gnosis

# Output:
📊 Remote Processing Progress (gnosis)
   Processed eras: 1250
   Failed eras: 3
   Last run: 2024-01-15 14:30:22
   Progress file: temp_era_files/.era_progress_gnosis.json
```

## Performance Optimization

### Batch Processing Strategy

Era Parser uses adaptive batch sizes based on data complexity:

```python
# Automatic batch sizing
if table == 'attestations':
    batch_size = 3000  # Complex data structure
elif table in ['transactions', 'withdrawals']:
    batch_size = 8000  # Medium complexity
else:
    batch_size = 15000  # Simple structures
```

### Download Optimization
```bash
# Concurrent downloads with connection pooling
ERA_MAX_CONCURRENT_DOWNLOADS=10

# Retry strategy with exponential backoff
ERA_MAX_RETRIES=5

# Compression for faster transfers
# (automatically enabled for supported servers)
```

### Memory Management
```bash
# Streaming processing keeps memory usage constant
# Processing 8192 blocks uses ~50MB regardless of era size

# Cleanup after processing saves disk space
ERA_CLEANUP_AFTER_PROCESS=true
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
❌ Failed to process era 1085: Parse error
✅ Successfully processed era 1086
✅ Successfully processed era 1087
```

### State Persistence
```bash
# Progress is saved after each era
# Interruption at any point allows resume from exact position
^C Operation cancelled by user

# Resume continues from interruption point
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume
```

## Monitoring and Debugging

### Progress Monitoring
```bash
# Real-time progress display
📈 Processing era 1082 (1/2475)
   📥 Downloading (attempt 1/3)
   ✅ Downloaded: 15MB
   🔧 Processing with command: 'all-blocks'
   📊 Loading all data types to ClickHouse:
   - blocks: 8192 records
   - sync_aggregates: 8192 records
   - attestations: 16388 records
   - deposits: 1 records
   ✅ Successfully processed era 1082
```

### Debug Information
```bash
# Enable verbose logging
export ERA_DEBUG=true

# Show detailed network operations
era-parser --remote gnosis 1082 all-blocks --export clickhouse
```

### Health Checks
```bash
# Test configuration without processing
era-parser --remote gnosis 1082 --download-only

# Verify ClickHouse connectivity
era-parser --era-status gnosis
```

## Common Patterns

### Research Workflows
```bash
# Extract all validator data for analysis period
era-parser --remote gnosis 1000-1100 attestations --export clickhouse
era-parser --remote gnosis 1000-1100 deposits --export clickhouse
era-parser --remote gnosis 1000-1100 withdrawals --export clickhouse

# Continuous monitoring of latest data
era-parser --remote gnosis 2500+ all-blocks --export clickhouse --resume
```

### Data Export Workflows
```bash
# Export historical data for external analysis
era-parser --remote mainnet 2000-2100 all-blocks data.parquet --separate

# Create time-series datasets
era-parser --remote gnosis 1500+ transactions tx_data.csv --resume
```

### Backup and Migration
```bash
# Download all era files for backup
era-parser --remote gnosis 0+ --download-only

# Process to multiple formats
era-parser --batch 'temp_era_files/gnosis-*.era' all-blocks --export clickhouse
era-parser --batch 'temp_era_files/gnosis-*.era' all-blocks backup.parquet --separate
```

## Troubleshooting

### Common Issues

**Configuration Errors**:
```bash
❌ Configuration error: ERA_BASE_URL environment variable is required
# Solution: Set ERA_BASE_URL
export ERA_BASE_URL=https://your-era-files.com
```

**Network Issues**:
```bash
❌ S3 listing failed: Connection timeout
# Solution: Increase retry count and timeout
export ERA_MAX_RETRIES=5
```

**Disk Space Issues**:
```bash
⚠️  Download failed: No space left on device
# Solution: Enable cleanup or change download directory
export ERA_CLEANUP_AFTER_PROCESS=true
export ERA_DOWNLOAD_DIR=/path/with/more/space
```

### Performance Issues

**Slow Discovery**:
```bash
# For non-S3 URLs, discovery may be slow
# Consider using S3-compatible storage for better performance
⚡ Using parallel discovery
   📋 Checking 1000 eras in total
```

**Memory Usage**:
```bash
# Era Parser uses constant memory regardless of era size
# If experiencing issues, check system resources:
htop
df -h
```

### State Issues

**Stale Processing**:
```bash
# Clean up entries stuck in "processing" state
era-parser --era-cleanup 30

# Check what was reset
era-parser --era-status gnosis
```

**Resume Not Working**:
```bash
# Check current status
era-parser --era-status gnosis

# Force retry failed datasets
era-parser --era-failed gnosis
```
