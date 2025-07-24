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

# Process with resume capability
era-parser --remote gnosis 1082+ all-blocks --export clickhouse --resume

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

# Remote Processing (required for --remote commands)
ERA_BASE_URL=https://era-files.com
ERA_DOWNLOAD_DIR=./temp_era_files
ERA_CLEANUP_AFTER_PROCESS=true
ERA_MAX_RETRIES=3
```

## 📊 Features

- **🌐 Multi-Network Support**: Mainnet, Gnosis, Sepolia
- **🔄 All Fork Support**: Phase 0, Altair, Bellatrix, Capella, Deneb, Electra
- **📋 Multiple Export Formats**: JSON, JSONL, CSV, Parquet, ClickHouse
- **🎯 Selective Data Extraction**: Extract specific data types
- **📁 Flexible Output**: Single files or separate files per data type
- **🚀 Batch Processing**: Process multiple era files at once
- **🌍 Remote Era Processing**: Download and process from remote URLs
- **🗄️ ClickHouse Integration**: Direct export with granular state tracking
- **📈 Era State Management**: Track processing status per dataset
- **🐳 Docker Support**: Containerized deployment
- **⚡ High Performance**: Memory-efficient streaming processing

## 📖 Documentation

- **[Setup Guide](docs/SETUP.md)** - Detailed installation and configuration
- **[Development Guide](docs/DEVELOPMENT.md)** - Architecture and contributing
- **[Era File Format](docs/ERA_FILE_FORMAT.md)** - Technical era file structure
- **[Parsed Fields Reference](docs/PARSED_FIELDS.md)** - Complete field documentation
- **[Network and Fork Support](docs/NETWORKS_FORKS.md)** - Supported networks and forks
- **[ClickHouse Integration](docs/CLICKHOUSE.md)** - Database schema and usage
- **[Remote Processing](docs/REMOTE_PROCESSING.md)** - Remote era file processing

## 🎯 Common Use Cases

### Research and Analytics
```bash
# Extract all validator attestations for analysis
era-parser --remote gnosis 1000-1100 attestations --export clickhouse

# Get comprehensive block data with separate tables
era-parser --remote mainnet 2500+ all-blocks --export clickhouse --resume

# Export transaction data for MEV analysis
era-parser --batch 'mainnet-*.era' transactions --export clickhouse
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