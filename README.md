# Era Parser - Beacon Chain Era File Parser

A modular, extensible parser for Gnosis/Ethereum beacon chain era files supporting multiple networks, forks, and export formats with **remote era file processing** and **granular ClickHouse state management**.

## Features

- 🌐 **Multi-Network Support**: Mainnet, Gnosis, Sepolia
- 🔄 **All Fork Support**: Phase 0, Altair, Bellatrix, Capella, Deneb, Electra, Fulu (example)
- 📊 **Multiple Export Formats**: JSON, JSONL, CSV, Parquet, ClickHouse
- 🎯 **Selective Data Extraction**: Extract specific data types
- 📁 **Flexible Output**: Single files or separate files per data type
- 🚀 **Batch Processing**: Process multiple era files at once with wildcards
- 🌍 **Remote Era Processing**: Download and process era files from remote URLs
- 🗄️ **ClickHouse Integration**: Direct export to ClickHouse with granular dataset tracking
- 📈 **Era State Management**: Granular tracking of processing status per dataset
- 🐳 **Docker Support**: Easy containerized deployment and execution
- 🧩 **Modular Architecture**: Easy to extend with new forks and networks
- ⚡ **High Performance**: Memory-efficient streaming processing
- 🔍 **Fork Auto-Detection**: Automatically detects and uses correct parser

## Installation

### Native Installation

```bash
# Clone the repository  
git clone https://github.com/your-org/era-parser.git
cd era-parser

# Create virtual environment
python -m venv era_parser_env
source era_parser_env/bin/activate  # On Windows: era_parser_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install as package
pip install -e .
```

#### System Dependencies

**macOS:**
```bash
brew install snappy
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libsnappy-dev
```

### Docker Installation

```bash
# Clone the repository
git clone https://github.com/your-org/era-parser.git
cd era-parser

# Copy and configure environment
cp .env.example .env
# Edit .env with your ERA_BASE_URL and ClickHouse settings

# Build the Docker image
docker build -t era-parser:latest .

# Create directories
mkdir -p output era-files
```

## Quick Start

### Native Usage

#### Local File Processing

```bash
# Show era file statistics
era-parser gnosis-02607-fe3b60d1.era stats

# Parse single block
era-parser gnosis-02607-fe3b60d1.era block 21348352

# Export all blocks to JSON
era-parser gnosis-02607-fe3b60d1.era all-blocks blocks.json

# Export to multiple CSV files
era-parser gnosis-02607-fe3b60d1.era all-blocks data.csv --separate

# Extract only transactions
era-parser gnosis-02607-fe3b60d1.era transactions txs.parquet
```

#### ClickHouse Export

```bash
# Export all data types to ClickHouse (automatically separate tables)
era-parser gnosis-02607-fe3b60d1.era all-blocks --export clickhouse

# Export specific data type to ClickHouse
era-parser gnosis-02607-fe3b60d1.era transactions --export clickhouse

# Export sync aggregates to ClickHouse
era-parser gnosis-02607-fe3b60d1.era sync_aggregates --export clickhouse
```

#### Remote Era Processing

Set up remote processing by configuring the base URL:

```bash
# Set the remote era files base URL
export ERA_BASE_URL=https://era-files.com

# Process a range of remote eras to ClickHouse
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Process from era 1082 until no more files found
era-parser --remote gnosis 1082+ transactions --export clickhouse

# Process to files with separate data types
era-parser --remote gnosis 1082-1100 all-blocks gnosis_data.parquet --separate

# Download only (no processing)
era-parser --remote gnosis 1082-1100 --download-only

# Resume previous processing
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume
```

#### Era State Management

```bash
# Check era processing status
era-parser --era-status gnosis
era-parser --era-status all

# View failed datasets
era-parser --era-failed gnosis
era-parser --era-failed all 50

# Clean up stale processing entries
era-parser --era-cleanup 30

# Check specific era file status
era-parser --era-check gnosis-02607-fe3b60d1.era
```

#### Batch Processing (Local Files)

```bash
# Process all Gnosis era files in current directory to ClickHouse
era-parser --batch 'gnosis-*.era' all-blocks --export clickhouse

# Process files in a specific directory
era-parser --batch /data/era_files/ transactions --export clickhouse

# Process era range with separate files per data type
era-parser --batch 'mainnet-026*.era' all-blocks era_26xx.parquet --separate
```

### Docker Usage

#### Local File Processing

```bash
# Place your era file in era-files/
cp gnosis-02607-fe3b60d1.era ./era-files/

# Show era file statistics
docker-compose run --rm era-parser /app/era-files/gnosis-02607-fe3b60d1.era stats

# Export all blocks to ClickHouse
docker-compose run --rm era-parser /app/era-files/gnosis-02607-fe3b60d1.era all-blocks --export clickhouse

# Export with separate files per data type
docker-compose run --rm era-parser /app/era-files/gnosis-02607-fe3b60d1.era all-blocks data.csv --separate

# Extract only transactions to ClickHouse
docker-compose run --rm era-parser /app/era-files/gnosis-02607-fe3b60d1.era transactions --export clickhouse
```

#### Remote Processing

```bash
# Process era range to ClickHouse with granular state tracking
docker-compose run --rm era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Process from era until end with resume
docker-compose run --rm era-parser --remote gnosis 1082+ transactions --export clickhouse --resume

# Process single era to files
docker-compose run --rm era-parser --remote gnosis 1082 all-blocks single_era.json

# Download only (no processing)
docker-compose run --rm era-parser --remote gnosis 1082-1100 --download-only
```

#### Era State Management

```bash
# Show era processing status
docker-compose run --rm era-parser --era-status gnosis

# Show failed datasets
docker-compose run --rm era-parser --era-failed gnosis

# Clean up stale processing
docker-compose run --rm era-parser --era-cleanup

# Check specific era status
docker-compose run --rm era-parser --era-check /app/era-files/your-file.era
```

#### Interactive Shell

```bash
# Launch shell for any custom command
docker-compose run --rm shell

# Inside the shell:
era-parser --help
era-parser /app/era-files/your-file.era stats
era-parser --remote mainnet 2600 transactions --export clickhouse
era-parser --era-status all
```

## Usage

### Command Syntax

```bash
# Local file processing
era-parser <era_file> <command> [options]

# Local batch processing
era-parser --batch <pattern> <command> <base_output> [--separate] [--export clickhouse]

# Remote era processing
era-parser --remote <network> <era_range> <command> [<output>] [flags]

# Era state management
era-parser --era-status <network|all>
era-parser --era-failed <network|all> [limit]
era-parser --era-cleanup [timeout_minutes]
era-parser --era-check <era_file>

# Remote utility commands
era-parser --remote-progress <network>
era-parser --remote-clear <network>
```

### ClickHouse Configuration

Set up ClickHouse connection in your environment:

```bash
# Required ClickHouse settings
export CLICKHOUSE_HOST=your-clickhouse-host.com
export CLICKHOUSE_PASSWORD=your-password

# Optional ClickHouse settings
export CLICKHOUSE_PORT=8443
export CLICKHOUSE_USER=default
export CLICKHOUSE_DATABASE=beacon_chain
export CLICKHOUSE_SECURE=true
```

Or create a `.env` file:

```bash
# .env file
CLICKHOUSE_HOST=your-clickhouse-host.com
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=true

# Remote era processing
ERA_BASE_URL=https://era-files.com
ERA_CLEANUP_AFTER_PROCESS=true
ERA_MAX_RETRIES=3
```

### Remote Era Processing Commands

#### Process Remote Eras
```bash
era-parser --remote <network> <era_range> <command> [<output>] [flags]
```

**Parameters:**
- `<network>`: Network name (`gnosis`, `mainnet`, `sepolia`)
- `<era_range>`: Era range specification (see formats below)
- `<command>`: Processing command (`all-blocks`, `transactions`, `withdrawals`, `attestations`, `sync_aggregates`)
- `<output>`: Output filename/path (not used for ClickHouse)
- `[flags]`: Optional flags (`--separate`, `--resume`, `--export clickhouse`)

#### Era Range Formats

| Format | Description | Example |
|--------|-------------|---------|
| `1082` | Single era | Process only era 1082 |
| `1082-1100` | Era range (inclusive) | Process eras 1082 through 1100 |
| `1082+` | Open range | Process from 1082 until no more files |

#### Remote Commands

| Command | Description | Example |
|---------|-------------|---------|
| **Process to ClickHouse** | Download and process to database | `era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse` |
| **Process to files** | Download and process to files | `era-parser --remote gnosis 1082-1100 all-blocks data.parquet --separate` |
| **Download only** | Download without processing | `era-parser --remote gnosis 1082-1100 --download-only` |
| **Resume processing** | Continue from where left off | `era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume` |

### Era State Management Commands

| Command | Description | Example |
|---------|-------------|---------|
| **Show status** | Display processing progress | `era-parser --era-status gnosis` |
| **Show failures** | List failed datasets | `era-parser --era-failed gnosis 20` |
| **Cleanup stale** | Reset stuck processing entries | `era-parser --era-cleanup 30` |
| **Check era** | Status of specific era file | `era-parser --era-check era-file.era` |

### Environment Configuration

Create a `.env` file in your project root:

```bash
# Required: ClickHouse connection
CLICKHOUSE_HOST=your-clickhouse-host.com
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=true

# Required: Base URL for remote era files
ERA_BASE_URL=https://era-files.com

# Optional: Directory for temporary downloads (default: system temp)
ERA_DOWNLOAD_DIR=./temp_era_files

# Optional: Whether to delete files after processing (default: true)
ERA_CLEANUP_AFTER_PROCESS=true

# Optional: Maximum download retries (default: 3)
ERA_MAX_RETRIES=3
```

Or set environment variables directly:

```bash
export CLICKHOUSE_HOST=your-clickhouse-host.com
export CLICKHOUSE_PASSWORD=your-password
export ERA_BASE_URL=https://era-files.com
export ERA_DOWNLOAD_DIR=./temp_era_files
export ERA_CLEANUP_AFTER_PROCESS=true
```

### Local Commands

| Command | Description | Single File Example | Batch Example |
|---------|-------------|---------------------|---------------|
| `stats` | Show era file statistics | `era-parser era.era stats` | N/A |
| `block <slot>` | Parse specific block | `era-parser era.era block 21348352` | N/A |
| `all-blocks` | Export all block data | `era-parser era.era all-blocks data.json` | `era-parser --batch *.era all-blocks --export clickhouse` |
| `transactions` | Extract only transactions | `era-parser era.era transactions --export clickhouse` | `era-parser --batch gnosis-*.era transactions --export clickhouse` |
| `withdrawals` | Extract only withdrawals | `era-parser era.era withdrawals w.parquet` | `era-parser --batch mainnet-*.era withdrawals --export clickhouse` |
| `attestations` | Extract only attestations | `era-parser era.era attestations --export clickhouse` | `era-parser --batch *.era attestations --export clickhouse` |
| `sync_aggregates` | Extract only sync aggregates | `era-parser era.era sync_aggregates --export clickhouse` | `era-parser --batch *.era sync_aggregates --export clickhouse` |

## ClickHouse Integration

### Database Schema

The parser creates normalized tables in ClickHouse:

- **`blocks`** - Beacon chain block headers and metadata
- **`sync_aggregates`** - Sync committee data (Altair+ forks)
- **`execution_payloads`** - Execution layer block data (Bellatrix+ forks)
- **`transactions`** - Individual transaction records
- **`withdrawals`** - Validator withdrawal records
- **`attestations`** - Validator attestation data
- **`deposits`** - Validator deposit data
- **`voluntary_exits`** - Validator exit requests
- **`proposer_slashings`** - Proposer slashing events
- **`attester_slashings`** - Attester slashing events
- **`bls_changes`** - BLS to execution address changes
- **`blob_commitments`** - Blob KZG commitments (Deneb+ forks)
- **`execution_requests`** - Execution layer requests (Electra+ forks)

### Era State Tracking

The parser maintains granular processing state:

- **`era_processing_state`** - Detailed tracking per era file and dataset
- **`era_processing_progress`** - View showing era-level completion status
- **`dataset_processing_progress`** - View showing dataset-level progress

### Granular Processing Benefits

1. **Partial Era Processing**: If one dataset fails, others can still succeed
2. **Smart Resume**: Only processes datasets that haven't been completed
3. **Dataset-Specific Retries**: Retry only failed datasets, not entire eras
4. **Progress Visibility**: See exactly which datasets are complete/failed/processing
5. **Automatic Filtering**: Remote processing skips already-completed eras

## Docker Configuration

The Docker setup provides a clean, isolated environment for running Era Parser with all dependencies included.

### Directory Structure

```
era-parser/
├── Dockerfile
├── docker-compose.yml  
├── .env                # Environment configuration
├── output/             # All output files
└── era-files/          # Local era files (optional)
    └── your-file.era
```

### Environment Variables for Docker

Set in `.env` file:
- `CLICKHOUSE_HOST`: ClickHouse server hostname
- `CLICKHOUSE_PASSWORD`: ClickHouse password
- `ERA_BASE_URL`: Required for remote processing
- `ERA_CLEANUP_AFTER_PROCESS`: Delete downloaded files after processing (default: true)
- `ERA_MAX_RETRIES`: Maximum download retries (default: 3)

All output files are saved to `./output/` on your host machine.

## Remote Processing Examples

### Basic Remote Processing to ClickHouse
```bash
# Set environment
export CLICKHOUSE_HOST=your-clickhouse-host.com
export CLICKHOUSE_PASSWORD=your-password
export ERA_BASE_URL=https://era-files.com

# Process era range with granular dataset tracking
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse
```

**Results in ClickHouse:**
- Separate tables for each dataset type
- Granular tracking of processing status
- Automatic resume capability if interrupted

### Advanced ClickHouse Workflows

#### Research Use Cases
```bash
# Extract all transactions for MEV analysis
era-parser --remote mainnet 2500+ transactions --export clickhouse --resume

# Get comprehensive validator data
era-parser --remote gnosis 1000-1100 all-blocks --export clickhouse

# Process specific datasets only
era-parser --remote gnosis 1082-1090 sync_aggregates --export clickhouse
era-parser --remote gnosis 1082-1090 attestations --export clickhouse
```

#### Monitoring and Management
```bash
# Check processing status
era-parser --era-status gnosis

# View failed datasets for retry
era-parser --era-failed gnosis

# Clean up stale processing entries
era-parser --era-cleanup 60

# Resume failed datasets only
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume
```

### Open-Ended Processing with Resume
```bash
# Start processing from era 1082 until no more files
era-parser --remote gnosis 1082+ all-blocks --export clickhouse

# If interrupted, resume with:
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume

# Check progress
era-parser --era-status gnosis
```

### Download First, Process Later
```bash
# Download eras without processing
era-parser --remote gnosis 1082-1090 --download-only

# Files are downloaded to ERA_DOWNLOAD_DIR
# Process later using local commands:
for era in temp_era_files/gnosis-*.era; do
    era-parser "$era" all-blocks --export clickhouse
done
```

### Mixed File and ClickHouse Processing
```bash
# Process some eras to files for analysis
era-parser --remote gnosis 1082-1085 all-blocks analysis.parquet --separate

# Process rest to ClickHouse for long-term storage  
era-parser --remote gnosis 1086+ all-blocks --export clickhouse --resume
```

## Output Formats

Files are auto-detected by extension:

| Format | Extension | Description | Best For |
|--------|-----------|-------------|----------|
| **JSON** | `.json` | Complete nested structure | API integration, small datasets |
| **JSON Lines** | `.jsonl` | One JSON per line | Streaming, large datasets |
| **CSV** | `.csv` | Flattened tabular data | Excel, pandas, analysis |
| **Parquet** | `.parquet` | Compressed columnar | Big data, analytics, ML |
| **ClickHouse** | `--export clickhouse` | Normalized database tables | Production, analytics, queries |

### ClickHouse Output Structure

When using `--export clickhouse`:

```sql
-- Era-level progress tracking
SELECT * FROM era_processing_progress WHERE network = 'gnosis';

-- Dataset-level progress
SELECT * FROM dataset_processing_progress WHERE network = 'gnosis';

-- Query actual data
SELECT COUNT(*) FROM blocks WHERE timestamp_utc >= '2024-01-01';
SELECT fee_recipient, COUNT(*) FROM transactions GROUP BY fee_recipient LIMIT 10;
SELECT COUNT(*) FROM sync_aggregates WHERE slot >= 1000000;
```

### Remote Processing Output Structure

When using `--separate` with remote processing:

```
output/
├── gnosis_analysis_era_01082_blocks.parquet
├── gnosis_analysis_era_01082_transactions.parquet
├── gnosis_analysis_era_01082_withdrawals.parquet
├── gnosis_analysis_era_01082_attestations.parquet
├── gnosis_analysis_era_01082_sync_aggregates.parquet
├── gnosis_analysis_era_01083_blocks.parquet
├── gnosis_analysis_era_01083_transactions.parquet
├── ...
└── progress files and summaries
```

### Data Output Examples

**Transactions Table (`transactions`):**
```sql
SELECT slot, block_number, transaction_hash, fee_recipient, gas_used 
FROM transactions 
WHERE slot >= 21348352 
LIMIT 5;
```

**Withdrawals Table (`withdrawals`):**
```sql
SELECT slot, validator_index, address, amount, timestamp_utc 
FROM withdrawals 
WHERE amount > 1000000000 
LIMIT 5;
```

**Sync Aggregates Table (`sync_aggregates`):**
```sql
SELECT slot, length(sync_committee_bits), timestamp_utc 
FROM sync_aggregates 
WHERE slot >= 21348352 
LIMIT 5;
```

**Attestations Table (`attestations`):**
```sql
SELECT slot, committee_index, source_epoch, target_epoch 
FROM attestations 
WHERE slot >= 21348352 
LIMIT 5;
```

## Error Handling and Troubleshooting

### ClickHouse Connection Issues

#### Missing Environment Variables
```
❌ Configuration error: CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD must be set
```

**Solution:**
```bash
export CLICKHOUSE_HOST=your-clickhouse-host.com
export CLICKHOUSE_PASSWORD=your-password
```

#### Connection Timeout
```bash
# Test ClickHouse connection
era-parser --era-status all
```

If this fails, check:
- Network connectivity to ClickHouse server
- Correct host and port
- Valid credentials
- Firewall settings

### Era State Management Issues

#### Stale Processing Entries
```bash
# Clean up entries stuck in "processing" state
era-parser --era-cleanup 30

# Check what was reset
era-parser --era-status gnosis
```

#### Resume Not Working
```bash
# Check current status
era-parser --era-status gnosis

# Check specific era
era-parser --era-check gnosis-02607-fe3b60d1.era

# Force retry failed datasets
era-parser --era-failed gnosis
```

### Remote Processing Issues

#### Missing Environment Variable
```
❌ Configuration error: ERA_BASE_URL environment variable is required
```

**Solution:**
```bash
export ERA_BASE_URL=https://era-files.com
```

#### Network Connection Issues
```bash
# Increase retry count for unstable connections
export ERA_MAX_RETRIES=5

# Use custom download directory with more space
export ERA_DOWNLOAD_DIR=/data/era_temp
```

#### Resume Interrupted Processing
```bash
# Check what's been processed in ClickHouse
era-parser --era-status gnosis

# Resume from where you left off
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume

# Clear file progress if needed (keeps ClickHouse state)
era-parser --remote-clear gnosis
```

### Local Processing Issues

**Missing output directory:**
```bash
mkdir -p output
```

**Pattern not matching files:**
```bash
# Use quotes if shell expansion is problematic
era-parser --batch 'gnosis-*.era' all-blocks --export clickhouse
```

**Memory issues:**
```bash
# Use ClickHouse export to reduce memory usage
era-parser --batch era-*.era all-blocks --export clickhouse
```

### Docker Issues

**Permission issues:**
```bash
sudo chown -R $USER:$USER ./output
```

**Network issues:**
```bash
# Increase retry count in .env
echo "ERA_MAX_RETRIES=10" >> .env
```

**ClickHouse connection from Docker:**
```bash
# Make sure ClickHouse host is accessible from container
# Use host networking if needed
docker-compose run --network host era-parser --era-status all
```

## Performance Tips

### For ClickHouse Processing
- ClickHouse export is the most efficient for large datasets
- Granular state tracking prevents reprocessing completed work
- Use `--resume` for long-running jobs to handle interruptions
- Process in reasonable chunks rather than enormous ranges

### For Remote Processing
- Use ClickHouse export for production workloads
- Use `--separate` files for analysis workflows
- Use `--resume` for long-running jobs to handle interruptions
- Process in reasonable chunks rather than enormous ranges

### For Local Processing
- Process era ranges in batches for very large datasets
- Use ClickHouse export for multiple data types
- Monitor disk space when processing many eras

### For Docker Usage
- Mount specific directories only when needed
- Use the shell service for interactive exploration
- Keep the .env file properly configured for your environment

## Architecture

The parser supports both local and remote era file processing through a modular architecture:

```
era-parser/
├── config/          # Network and fork configurations
│   ├── networks.py  # Network-specific settings (Mainnet, Gnosis, Sepolia)
│   └── forks.py     # Fork definitions and detection logic
├── ingestion/       # Era file reading and remote downloading
│   ├── era_reader.py        # Local era file reader
│   ├── remote_downloader.py # Remote era file downloader
│   └── compression.py       # Snappy decompression utilities
├── parsing/         # Block parsing with fork-specific logic
│   ├── block_parser.py  # Main parsing coordinator
│   ├── ssz_utils.py     # SSZ parsing utilities
│   └── forks/       # Individual fork parsers
│       ├── base.py      # Common parsing functionality
│       ├── phase0.py    # Phase 0 parser
│       ├── altair.py    # Altair parser
│       ├── bellatrix.py # Bellatrix parser
│       ├── capella.py   # Capella parser
│       ├── deneb.py     # Deneb parser
│       ├── electra.py   # Electra parser
│       └── fulu.py      # Fulu parser (example)
├── export/          # Export formats (JSON, CSV, Parquet, ClickHouse)
│   ├── base.py              # Common export functionality
│   ├── json_exporter.py     # JSON/JSONL export
│   ├── csv_exporter.py      # CSV export
│   ├── parquet_exporter.py  # Parquet export
│   ├── clickhouse_exporter.py   # ClickHouse export with state management
│   ├── clickhouse_service.py    # ClickHouse connection and table management
│   └── era_state_manager.py     # Granular era processing state tracking
├── cli.py          # Command line interface with remote and state management
├── Dockerfile      # Docker container configuration
└── docker-compose.yml # Docker Compose services
```

### Key Components

- **EraReader**: Handles local era file ingestion and record extraction
- **RemoteEraDownloader**: Downloads and processes remote era files with state integration
- **BlockParser**: Main parsing coordinator that delegates to fork parsers
- **Fork Parsers**: Specialized parsers for each fork (Phase0, Altair, etc.)
- **Exporters**: Format-specific output handlers with consistent interface
- **ClickHouseExporter**: Direct database export with granular state tracking
- **EraStateManager**: Granular tracking of processing status per era file and dataset
- **Config System**: Centralized network and fork configuration
- **Docker Support**: Containerized deployment with Docker and Docker Compose

## Supported Networks & Forks

### Networks
| Network | Genesis Time | Slot Duration | Slots/Epoch |
|---------|-------------|---------------|-------------|
| Mainnet | Dec 1, 2020 | 12 seconds | 32 |
| Gnosis | Dec 8, 2021 | 5 seconds | 16 |
| Sepolia | Jun 20, 2022 | 12 seconds | 32 |

### Forks
| Fork | Features Added |
|------|----------------|
| **Phase 0** | Basic consensus, attestations |
| **Altair** | Sync committees, light client support |
| **Bellatrix** | Execution layer integration (The Merge) |
| **Capella** | Withdrawal support |
| **Deneb** | Blob transactions (EIP-4844) |
| **Electra** | Execution requests, validator lifecycle |
| **Fulu*** | Validator consolidations, advanced attestations |

*\*Fulu is an example fork showing extensibility*

## Extending the Parser

### Adding New Networks

1. **Add network configuration** to `era_parser/config/networks.py`:

```python
NETWORK_CONFIGS['holesky'] = {
    'GENESIS_TIME': 1695902400,
    'SECONDS_PER_SLOT': 12,
    'SLOTS_PER_EPOCH': 32,
    'SLOTS_PER_HISTORICAL_ROOT': 8192,
    'FORK_EPOCHS': {
        'altair': 0,
        'bellatrix': 0,
        'capella': 256,
        'deneb': 29696,
        'electra': 999999999
    }
}
```

### Adding New Forks (Example: Fulu)

**Only 4 files need changes to add a new fork:**

#### 1. Fork Configuration (`era_parser/config/forks.py`)
```python
FORK_CONFIGS['fulu'] = {
    'name': 'Fulu',
    'has_validator_consolidations': True,  # New feature
    'has_advanced_attestations': True,    # New feature
}

def get_fork_by_slot(slot: int, network: str = 'mainnet') -> str:
    # Add Fulu check
    if epoch >= fork_epochs.get('fulu', float('inf')):
        return 'fulu'
    # ... existing logic
```

#### 2. Network Configuration (`era_parser/config/networks.py`)
```python
# Add Fulu epoch to each network
NETWORK_CONFIGS['mainnet']['FORK_EPOCHS']['fulu'] = 1500000000
NETWORK_CONFIGS['gnosis']['FORK_EPOCHS']['fulu'] = 2000000
```

#### 3. Fork Parser (`era_parser/parsing/forks/fulu.py`)
```python
from .electra import ElectraParser

class FuluParser(ElectraParser):
    def parse_validator_consolidation(self, data: bytes):
        # Implement Fulu-specific parsing
        return {"source_validator": data[0:8], "target_validator": data[8:16]}
    
    def parse_body(self, body_data: bytes):
        # Extend Electra parsing with Fulu features
        result = super().parse_body(body_data)
        # Add Fulu-specific fields
        return result
```

#### 4. Parser Registry (`era_parser/parsing/forks/__init__.py`)
```python
from .fulu import FuluParser

FORK_PARSERS = {
    # ... existing parsers ...
    'fulu': FuluParser,
}
```

**That's it!** The new fork automatically works with all commands:

```bash
# Automatically detects and parses Fulu fork
era-parser fulu-era-12345.era all-blocks --export clickhouse
era-parser --remote fulu 1000+ transactions --export clickhouse --resume

# Works with Docker too
docker-compose run --rm era-parser /app/era-files/fulu-era-12345.era all-blocks --export clickhouse
```

## Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-feature`
3. **Add tests** for new functionality
4. **Ensure all tests pass**: `pytest tests/`
5. **Update documentation**
6. **Submit a pull request**

### Contribution Guidelines

- Follow existing code style and patterns
- Add tests for new features
- Update documentation and examples
- Use the registry pattern for extensibility
- Maintain backwards compatibility
- Consider batch processing impact for new features
- Test both native and Docker deployments
- Test ClickHouse integration for new features

## License

MIT License - see LICENSE file for details.