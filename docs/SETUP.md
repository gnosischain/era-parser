# Setup and Installation Guide

## System Requirements

- Python 3.8 or higher
- 4GB+ RAM (for processing large era files)
- 10GB+ free disk space (for output files)

## Quick Setup (Recommended)

### 1. Install System Dependencies

**macOS:**
```bash
brew install snappy
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install libsnappy-dev
```

**Windows:**
```bash
# Use conda for easier snappy installation
conda install snappy
```

### 2. Create Virtual Environment
```bash
# Clone the repository
git clone <repository-url>
cd era-parser

# Create virtual environment
python -m venv era_parser_env

# Activate virtual environment
source era_parser_env/bin/activate  # macOS/Linux
# or
era_parser_env\Scripts\activate     # Windows
```

### 3. Install Python Dependencies
```bash
# Install dependencies
pip install -r requirements.txt

# Install era-parser in development mode
pip install -e .
```

### 4. Verify Installation
```bash
# Test all dependencies
python -c "import snappy, pandas, pyarrow; print('✅ All dependencies installed successfully!')"

# Test era-parser CLI
era-parser --help
```

## Alternative Installation Methods

### Option 1: Using Conda (Easier for Windows)

```bash
# Create conda environment
conda create -n era_parser python=3.11
conda activate era_parser

# Install all dependencies via conda
conda install -c conda-forge python-snappy pandas pyarrow numpy

# Install era-parser
pip install -e .
```

### Option 2: Using Poetry

```bash
# Install poetry if not already installed
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Option 3: Docker (Isolated Environment)

```dockerfile
# Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libsnappy-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN pip install -e .

CMD ["era-parser"]
```

```bash
# Build and run
docker build -t era-parser .
docker run -v $(pwd)/data:/app/data era-parser era-parser data/era-file.era stats
```

## Quick Start Examples

### 1. Basic Era File Analysis
```bash
# Show era file statistics (auto-detects network)
era-parser gnosis-02607-fe3b60d1.era stats

# Parse a single block
era-parser gnosis-02607-fe3b60d1.era block 21348352
```

### 2. Export All Blocks
```bash
# Export to JSON (complete nested data)
era-parser gnosis-02607-fe3b60d1.era all-blocks blocks.json

# Export to CSV (flattened, single file)
era-parser gnosis-02607-fe3b60d1.era all-blocks blocks.csv

# Export to separate CSV files (recommended for analysis)
era-parser gnosis-02607-fe3b60d1.era all-blocks data.csv --separate

# Export to Parquet (best compression and performance)
era-parser gnosis-02607-fe3b60d1.era all-blocks data.parquet --separate
```

### 3. Extract Specific Data Types
```bash
# Extract only transactions
era-parser gnosis-02607-fe3b60d1.era transactions txs.csv

# Extract only withdrawals (Capella+ forks)
era-parser gnosis-02607-fe3b60d1.era withdrawals withdrawals.parquet

# Extract only attestations  
era-parser gnosis-02607-fe3b60d1.era attestations attestations.json
```

## Troubleshooting

### Common Issues

#### 1. Snappy Installation Problems

**Error**: `ModuleNotFoundError: No module named 'snappy'`

**Solution**:
```bash
# macOS: Install system snappy first
brew install snappy
pip install --no-cache-dir python-snappy

# Ubuntu/Debian: Install dev headers
sudo apt-get install libsnappy-dev
pip install python-snappy

# Alternative: Use conda
conda install -c conda-forge python-snappy
```

#### 2. Memory Issues with Large Era Files

**Error**: `MemoryError` or system becomes unresponsive

**Solutions**:
```bash
# Use separate files mode to reduce memory usage
era-parser large-era.era all-blocks data.parquet --separate

# Export specific data types instead of all blocks
era-parser large-era.era transactions txs.csv

# Increase virtual memory (Linux)
sudo sysctl vm.overcommit_memory=1
```

#### 3. Permission Errors

**Error**: `PermissionError: [Errno 13] Permission denied`

**Solutions**:
```bash
# Create output directory with proper permissions
mkdir -p output
chmod 755 output

# Run with proper permissions
sudo chown -R $USER:$USER era-parser/
```

#### 4. Path Issues

**Error**: `era-parser: command not found`

**Solutions**:
```bash
# Make sure virtual environment is activated
source era_parser_env/bin/activate

# Verify installation
pip list | grep era-parser

# Use full path if needed
python -m era_parser.cli
```

### Performance Optimization

#### 1. For Large Era Files (>1GB)
- Use `--separate` flag for CSV/Parquet exports
- Export specific data types instead of all blocks
- Ensure at least 8GB RAM available
- Use SSD storage for better I/O performance

#### 2. For Batch Processing
```bash
# Process multiple era files
for era in era-files/*.era; do
    echo "Processing $era..."
    era-parser "$era" all-blocks "output/$(basename $era .era).parquet" --separate
done
```

#### 3. For Memory-Constrained Systems
```bash
# Extract data incrementally
era-parser era.era transactions txs.csv
era-parser era.era withdrawals withdrawals.csv  
era-parser era.era attestations attestations.csv
```

## Configuration

### 1. Output Directory

By default, files are saved to the `output/` directory. You can customize this:

```bash
# Set custom output directory
export ERA_PARSER_OUTPUT_DIR="/path/to/custom/output"
era-parser era.era all-blocks data.csv
```

### 2. Network Detection

Era files are automatically detected by filename, but you can verify:

```python
from era_parser.config import detect_network_from_filename
from era_parser.ingestion import EraReader

# Automatic detection from filename
network = detect_network_from_filename("gnosis-02607-fe3b60d1.era")
print(f"Detected network: {network}")  # Output: gnosis

# Using EraReader (recommended)
era_reader = EraReader("gnosis-02607-fe3b60d1.era")
era_info = era_reader.get_era_info()
print(f"Network: {era_info['network']}")
print(f"Era: {era_info['era_number']}")
print(f"Slots: {era_info['start_slot']} - {era_info['end_slot']}")
```

### 3. Custom Fork Epochs

For testing or custom networks, you can modify fork epochs:

```python
from era_parser.config.networks import NETWORK_CONFIGS

# Add custom network
NETWORK_CONFIGS['custom'] = {
    'GENESIS_TIME': 1234567890,
    'SECONDS_PER_SLOT': 12,
    'SLOTS_PER_EPOCH': 32,
    'SLOTS_PER_HISTORICAL_ROOT': 8192,
    'FORK_EPOCHS': {
        'altair': 100,
        'bellatrix': 200,
        # ... etc
    }
}
```

## Development Setup

If you plan to contribute or modify the parser:

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install

# Run tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=era_parser --cov-report=html
```

## Getting Help

### 1. Check Logs
```bash
# Enable verbose logging
export ERA_PARSER_LOG_LEVEL=DEBUG
era-parser era.era stats
```

### 2. Validate Era File
```bash
# Check if era file is valid
file era-file.era

# Show first few bytes (should show e2store format)
hexdump -C era-file.era | head  

# Use the CLI to validate structure
era-parser era-file.era stats
```

### 3. Test with Small Era File
Start with a smaller era file to verify your setup works before processing large files.

### 4. Community Support
- Check existing GitHub issues
- Create detailed bug reports with:
  - Operating system and Python version
  - Complete error messages
  - Steps to reproduce
  - Era file information (network, era number, file size)

## Next Steps

Once installation is complete:

1. **Read the [ERA File Format Guide](ERA_FILE_FORMAT.md)** to understand the underlying data structure
2. **Try the [Quick Start examples](#quick-start-examples)** with your era files
3. **Explore the output formats** to find what works best for your analysis
4. **Check the [Development Guide](DEVELOPMENT.md)** if you want to extend the parser

## Deactivating Environment

When you're done working:

```bash
# Deactivate virtual environment
deactivate

# Or if using conda
conda deactivate
```