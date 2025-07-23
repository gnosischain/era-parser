# Era Parser - Beacon Chain Era File Parser

A modular, extensible parser for Gnosis/Ethereum beacon chain era files supporting multiple networks, forks, and export formats with **remote era file processing**.

## Features

- 🌐 **Multi-Network Support**: Mainnet, Gnosis, Sepolia
- 🔄 **All Fork Support**: Phase 0, Altair, Bellatrix, Capella, Deneb, Electra, Fulu (example)
- 📊 **Multiple Export Formats**: JSON, JSONL, CSV, Parquet
- 🎯 **Selective Data Extraction**: Extract specific data types
- 📁 **Flexible Output**: Single files or separate files per data type
- 🚀 **Batch Processing**: Process multiple era files at once with wildcards
- 🌍 **Remote Era Processing**: Download and process era files from remote URLs
- 🧩 **Modular Architecture**: Easy to extend with new forks and networks
- ⚡ **High Performance**: Memory-efficient streaming processing
- 🔍 **Fork Auto-Detection**: Automatically detects and uses correct parser

## Installation

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

### System Dependencies

**macOS:**
```bash
brew install snappy
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libsnappy-dev
```

## Quick Start

### Local File Processing

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

### Remote Era Processing

Set up remote processing by configuring the base URL:

```bash
# Set the remote era files base URL
export ERA_BASE_URL=https://era-files.com

# Process a range of remote eras
era-parser --remote gnosis 1082-1100 all-blocks gnosis_data.parquet --separate

# Process from era 1082 until no more files found
era-parser --remote gnosis 1082+ transactions txs.csv

# Download only (no processing)
era-parser --remote gnosis 1082-1100 --download-only

# Resume previous processing
era-parser --remote gnosis 1082+ all-blocks data.parquet --resume
```

### Batch Processing (Local Files)

```bash
# Process all Gnosis era files in current directory
era-parser --batch gnosis-*.era all-blocks gnosis_dataset.parquet

# Process files in a specific directory
era-parser --batch /data/era_files/ transactions tx_analysis.csv

# Process era range with separate files per data type
era-parser --batch mainnet-026*.era all-blocks era_26xx.parquet --separate
```

## Usage

### Command Syntax

```bash
# Local file processing
era-parser <era_file> <command> [options]

# Local batch processing
era-parser --batch <pattern> <command> <base_output> [--separate]

# Remote era processing
era-parser --remote <network> <era_range> <command> <output> [flags]

# Remote utility commands
era-parser --remote-progress <network>
era-parser --remote-clear <network>
```

### Remote Era Processing Commands

#### Process Remote Eras
```bash
era-parser --remote <network> <era_range> <command> <output> [flags]
```

**Parameters:**
- `<network>`: Network name (`gnosis`, `mainnet`, `sepolia`)
- `<era_range>`: Era range specification (see formats below)
- `<command>`: Processing command (`all-blocks`, `transactions`, `withdrawals`, `attestations`)
- `<output>`: Output filename/path
- `[flags]`: Optional flags (`--separate`, `--resume`)

#### Era Range Formats

| Format | Description | Example |
|--------|-------------|---------|
| `1082` | Single era | Process only era 1082 |
| `1082-1100` | Era range (inclusive) | Process eras 1082 through 1100 |
| `1082+` | Open range | Process from 1082 until no more files |

#### Remote Commands

| Command | Description | Example |
|---------|-------------|---------|
| **Process eras** | Download and process | `era-parser --remote gnosis 1082-1100 all-blocks data.parquet` |
| **Download only** | Download without processing | `era-parser --remote gnosis 1082-1100 --download-only` |
| **Show progress** | Display processing status | `era-parser --remote-progress gnosis` |
| **Clear progress** | Reset processing state | `era-parser --remote-clear gnosis` |

### Environment Configuration

Create a `.env` file in your project root:

```bash
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
export ERA_BASE_URL=https://era-files.com
export ERA_DOWNLOAD_DIR=./temp_era_files
export ERA_CLEANUP_AFTER_PROCESS=true
```

### Local Commands

| Command | Description | Single File Example | Batch Example |
|---------|-------------|---------------------|---------------|
| `stats` | Show era file statistics | `era-parser era.era stats` | N/A |
| `block <slot>` | Parse specific block | `era-parser era.era block 21348352` | N/A |
| `all-blocks` | Export all block data | `era-parser era.era all-blocks data.json` | `era-parser --batch *.era all-blocks dataset.parquet` |
| `transactions` | Extract only transactions | `era-parser era.era transactions txs.csv` | `era-parser --batch gnosis-*.era transactions tx_data.csv` |
| `withdrawals` | Extract only withdrawals | `era-parser era.era withdrawals w.parquet` | `era-parser --batch mainnet-*.era withdrawals wd_data.parquet` |
| `attestations` | Extract only attestations | `era-parser era.era attestations atts.json` | `era-parser --batch *.era attestations att_analysis.csv` |

## Remote Processing Examples

### Basic Remote Processing
```bash
# Set environment
export ERA_BASE_URL=https://era-files.com

# Process era range with separate files per data type
era-parser --remote gnosis 1082-1100 all-blocks gnosis_analysis.parquet --separate
```

**Output files:**
```
output/gnosis_analysis_era_01082_blocks.parquet
output/gnosis_analysis_era_01082_transactions.parquet
output/gnosis_analysis_era_01082_withdrawals.parquet
output/gnosis_analysis_era_01083_blocks.parquet
...
```

### Open-Ended Processing with Resume
```bash
# Start processing from era 1082 until no more files
era-parser --remote gnosis 1082+ transactions gnosis_txs.csv

# If interrupted, resume with:
era-parser --remote gnosis 1082+ transactions gnosis_txs.csv --resume

# Check progress
era-parser --remote-progress gnosis
```

### Download First, Process Later
```bash
# Download eras without processing
era-parser --remote gnosis 1082-1090 --download-only

# Files are downloaded to ERA_DOWNLOAD_DIR
# Process later using local commands:
for era in temp_era_files/gnosis-*.era; do
    era-parser "$era" all-blocks "processed_$(basename $era .era).json"
done
```

### Research Use Cases with Remote Processing

#### MEV Analysis
```bash
# Extract all transactions from remote era range
era-parser --remote mainnet 2500+ transactions mev_dataset.parquet --resume

# Get fee recipient patterns across multiple eras
era-parser --remote mainnet 2400-2500 all-blocks fee_analysis.csv --separate
```

#### Validator Performance Studies
```bash
# Extract attestation data across Gnosis era range
era-parser --remote gnosis 1000+ attestations validator_performance.csv --resume

# Get comprehensive validator data
era-parser --remote gnosis 1000-1100 all-blocks validator_study.parquet --separate
```

#### Time Series Analysis
```bash
# Process sequential eras for longitudinal studies
era-parser --remote gnosis 0+ all-blocks complete_timeseries.parquet --separate --resume
```

### Progress Management

```bash
# Show processing progress for a network
era-parser --remote-progress gnosis
```

**Output:**
```
📊 Remote Processing Progress (gnosis)
   Processed eras: 15
   Failed eras: 2
   Last run: 2025-01-27 14:30:22
   Progress file: /tmp/era_downloads/.era_progress_gnosis.json
```

```bash
# Clear progress to start fresh
era-parser --remote-clear gnosis
```

## Output Formats

Files are auto-detected by extension:

| Format | Extension | Description | Best For |
|--------|-----------|-------------|----------|
| **JSON** | `.json` | Complete nested structure | API integration, small datasets |
| **JSON Lines** | `.jsonl` | One JSON per line | Streaming, large datasets |
| **CSV** | `.csv` | Flattened tabular data | Excel, pandas, analysis |
| **Parquet** | `.parquet` | Compressed columnar | Big data, analytics, ML |

### Remote Processing Output Structure

When using `--separate` with remote processing:

```
output/
├── gnosis_analysis_era_01082_blocks.parquet
├── gnosis_analysis_era_01082_transactions.parquet
├── gnosis_analysis_era_01082_withdrawals.parquet
├── gnosis_analysis_era_01082_attestations.parquet
├── gnosis_analysis_era_01083_blocks.parquet
├── gnosis_analysis_era_01083_transactions.parquet
├── ...
└── progress files and summaries
```

### Data Output Examples

**Transactions (`data_transactions.csv`):**
```csv
slot,block_number,transaction_index,transaction_hash,fee_recipient,gas_used,timestamp
21348352,39771042,0,0x02f903d464...,0x5112d584a1c72fc250...,226309,1745735100
21348352,39771042,1,0x02f901f464...,0x5112d584a1c72fc250...,195562,1745735100
```

**Withdrawals (`data_withdrawals.csv`):**
```csv
slot,block_number,withdrawal_index,validator_index,address,amount,timestamp
21348352,39771042,84223338,74400,0xfc9b67b6034f6b306ea9bd8ec1baf3efa2490394,37969609,1745735100
21348352,39771042,84223339,74401,0xfc9b67b6034f6b306ea9bd8ec1baf3efa2490394,17027278,1745735100
```

**Attestations (`data_attestations.csv`):**
```csv
slot,attestation_index,committee_index,source_epoch,target_epoch,beacon_block_root
21348352,0,33,1334270,1334271,0x4726093400094404407e84ed9d0bc5b0586980bc8240f576ce109f51cfa756cd
21348352,1,10,1334270,1334271,0x4726093400094404407e84ed9d0bc5b0586980bc8240f576ce109f51cfa756cd
```

## Error Handling and Troubleshooting

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
# Check what's been processed
era-parser --remote-progress gnosis

# Resume from where you left off
era-parser --remote gnosis 1082+ all-blocks data.parquet --resume

# Clear progress if needed
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
era-parser --batch 'gnosis-*.era' all-blocks data.json
```

**Memory issues:**
```bash
# Use separate files mode to reduce memory usage
era-parser --batch era-*.era all-blocks data.parquet --separate
```

## Performance Tips

### For Remote Processing
- Use `--separate` for better memory efficiency with large datasets
- Use Parquet format for optimal compression and speed  
- Use `--resume` for long-running jobs to handle interruptions
- Process in reasonable chunks rather than enormous ranges

### For Local Processing
- Process era ranges in batches for very large datasets
- Use separate files mode for multiple data types
- Monitor disk space when processing many eras

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
├── export/          # Export formats (JSON, CSV, Parquet)
│   ├── base.py          # Common export functionality
│   ├── json_exporter.py # JSON/JSONL export
│   ├── csv_exporter.py  # CSV export
│   └── parquet_exporter.py # Parquet export
└── cli.py          # Command line interface with remote support
```

### Key Components

- **EraReader**: Handles local era file ingestion and record extraction
- **RemoteEraDownloader**: Downloads and processes remote era files
- **BlockParser**: Main parsing coordinator that delegates to fork parsers
- **Fork Parsers**: Specialized parsers for each fork (Phase0, Altair, etc.)
- **Exporters**: Format-specific output handlers with consistent interface
- **Config System**: Centralized network and fork configuration

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
era-parser fulu-era-12345.era all-blocks fulu_data.json
era-parser --remote fulu 1000+ transactions fulu_txs.csv --resume
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

## License

MIT License - see LICENSE file for details.