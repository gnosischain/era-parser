# Era Parser - Beacon Chain Era File Parser

A modular, extensible parser for Gnosis/Ethereum beacon chain era files supporting multiple networks, forks, and export formats with **remote era file processing** and **granular ClickHouse state management**.

## 🚀 Quick Start

### Installation

#### Native Installation
```bash
# Clone and install
git clone https://github.com/your-org/era-parser.git
cd era-parser
python -m venv era_parser_env
source era_parser_env/bin/activate  # Windows: era_parser_env\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

**System Dependencies:**
- **macOS:** `brew install snappy`
- **Ubuntu/Debian:** `sudo apt-get install libsnappy-dev`

#### Docker Installation
```bash
git clone https://github.com/your-org/era-parser.git
cd era-parser
cp .env.example .env  # Configure your settings
docker build -t era-parser:latest .
mkdir -p output era-files
```

### Basic Usage

#### Local Era Files
```bash
# Show era statistics
era-parser gnosis-02607-fe3b60d1.era stats

# Parse single block
era-parser gnosis-02607-fe3b60d1.era block 21348352

# Export all data to separate files
era-parser gnosis-02607-fe3b60d1.era all-blocks data.parquet --separate

# Export specific data type
era-parser gnosis-02607-fe3b60d1.era transactions txs.json

# Export to ClickHouse
era-parser gnosis-02607-fe3b60d1.era all-blocks --export clickhouse
```

#### Remote Era Processing
```bash
# Set environment variables
export ERA_BASE_URL=https://era-files.com
export CLICKHOUSE_HOST=your-clickhouse-host.com
export CLICKHOUSE_PASSWORD=your-password

# Process era range to ClickHouse
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Force reprocess with data cleanup
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --force

# Download without processing
era-parser --remote gnosis 1082-1100 --download-only
```

#### Docker Usage
```bash
# Local file processing
docker-compose run --rm era-parser /app/era-files/your-file.era stats
docker-compose run --rm era-parser /app/era-files/your-file.era all-blocks --export clickhouse

# Remote processing
docker-compose run --rm era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse

# Interactive shell
docker-compose run --rm shell
```

## 🔧 Configuration

### Environment Variables
Create a `.env` file:
```bash
# ClickHouse (required for ClickHouse export)
CLICKHOUSE_HOST=your-clickhouse-host.com
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=true

# Remote Era URLs
ERA_BASE_URL=https://era-files.com

# Download settings
DOWNLOAD_TIMEOUT=300
MAX_RETRIES=3
DOWNLOAD_THREADS=4
```

### Networks Supported
- **gnosis** (auto-detected from `gnosis-*.era`)
- **mainnet** (auto-detected from `mainnet-*.era`)  
- **holesky** (auto-detected from `holesky-*.era`)

Network configuration is automatic based on era filename.

## 📋 Available Commands

### Era Range Formats
```bash
1082-1100    # Specific range
1082         # Single era
1082+        # Open-ended (from era 1082 onwards)
```

### Data Types
- `all-blocks` - All beacon chain data (default)
- `blocks` - Block headers and metadata  
- `transactions` - Execution layer transactions
- `attestations` - Validator attestations
- `deposits` - Validator deposits
- `withdrawals` - Validator withdrawals (Capella+)
- `voluntary-exits` - Voluntary validator exits
- `proposer-slashings` - Proposer slashing events
- `attester-slashings` - Attester slashing events
- `sync-aggregates` - Sync committee aggregates (Altair+)
- `bls-changes` - BLS to execution changes (Capella+)
- `execution-payloads` - Execution layer payloads (Bellatrix+)
- `blob-kzg-commitments` - Blob KZG commitments (Deneb+)
- `execution-requests` - Execution requests (Electra+)

## 🎯 Usage Examples

### Research & Analysis
```bash
# Extract validator data for analysis period
era-parser --remote gnosis 1000-1100 attestations --export clickhouse
era-parser --remote gnosis 1000-1100 deposits --export clickhouse
era-parser --remote gnosis 1000-1100 withdrawals --export clickhouse

# Continuous monitoring
era-parser --remote gnosis 2500+ all-blocks --export clickhouse
```

### Data Export
```bash
# Export to multiple CSV files for Excel analysis
era-parser era-file.era all-blocks data.csv --separate

# Create Parquet files for data science workflows
era-parser --batch 'gnosis-*.era' all-blocks analysis.parquet --separate

# Single JSON file with all data
era-parser era-file.era all-blocks complete_data.json
```

### State Management
```bash
# Check processing status
era-parser --era-status gnosis

# View failed processing attempts
era-parser --era-failed gnosis

# Clean up stale processing entries
era-parser --era-cleanup 30
```

### Processing Modes

#### Normal Mode (Default)
Processes all specified eras:
```bash
# Processes all eras in range
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse
```

#### Force Mode
Cleans existing data first, then reprocesses everything:
```bash
# Clean and reprocess all eras
era-parser --remote gnosis 1082-1100 all-blocks --export clickhouse --force
```

**Force Mode Benefits:**
- **Data Recovery**: Clean corrupted data and regenerate
- **Schema Changes**: Reprocess after database updates  
- **Fresh Start**: Completely regenerate a data range
- **Testing**: Ensure clean state for testing

## 🔍 Output Formats

| Format | Extension | Best For | Example |
|--------|-----------|----------|---------|
| **JSON** | `.json` | API integration, small datasets | `data.json` |
| **JSON Lines** | `.jsonl` | Streaming, large datasets | `data.jsonl` |
| **CSV** | `.csv` | Excel, pandas analysis | `data.csv` |
| **Parquet** | `.parquet` | Big data, analytics, ML | `data.parquet` |
| **ClickHouse** | `--export clickhouse` | Production analytics, queries | N/A |

## 🏗️ Architecture

```
era-parser/
├── cli.py              # Command-line interface
├── commands/           # Command implementations
├── core/               # Business logic
├── config/             # Network and fork configurations
├── export/             # Export formats and ClickHouse
├── ingestion/          # Era file reading and remote downloading
└── parsing/            # Block parsing with fork-specific logic
```

## 📄 License

MIT License - see LICENSE file for details.

---

**Need help?** Check our [documentation](docs/) or open an issue on GitHub.