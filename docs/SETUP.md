# Setup Guide

This comprehensive guide covers all installation methods, configuration options, and initial setup for Era Parser.

## Table of Contents

- [System Requirements](#system-requirements)
- [Installation Methods](#installation-methods)
- [Configuration](#configuration)
- [Verification](#verification)
- [Docker Setup](#docker-setup)
- [Development Setup](#development-setup)
- [Troubleshooting](#troubleshooting)

## System Requirements

### Minimum Requirements
- **Python**: 3.8+ (recommended: 3.10+)
- **Memory**: 4GB RAM (8GB+ recommended for large era files)
- **Storage**: 10GB free space (more for batch processing)
- **Network**: Stable internet connection for remote processing

### System Dependencies

**macOS (Homebrew)**:
```bash
# Install Snappy compression library
brew install snappy

# Optional: Install ClickHouse client for testing
brew install clickhouse
```

**Ubuntu/Debian**:
```bash
# Install required system packages
sudo apt-get update
sudo apt-get install -y python3-dev python3-pip python3-venv libsnappy-dev

# Optional: Install ClickHouse client
sudo apt-get install -y clickhouse-client
```

**CentOS/RHEL/Fedora**:
```bash
# Install required packages
sudo yum install -y python3-devel python3-pip snappy-devel
# or for newer versions:
sudo dnf install -y python3-devel python3-pip snappy-devel

# Build tools if needed
sudo yum groupinstall -y "Development Tools"
```

**Windows**:
```powershell
# Install Python from python.org
# Install Visual Studio Build Tools
# Install dependencies through pip (may require compilation)
```

## Installation Methods

### Method 1: Standard Installation (Recommended)

```bash
# Clone the repository
git clone https://github.com/your-org/era-parser.git
cd era-parser

# Create virtual environment
python -m venv era_parser_env

# Activate virtual environment
source era_parser_env/bin/activate  # Linux/macOS
# era_parser_env\Scripts\activate     # Windows

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Install Era Parser
pip install -e .

# Verify installation
era-parser --help
```

### Method 2: Direct pip Installation

```bash
# Create virtual environment
python -m venv era_parser_env
source era_parser_env/bin/activate

# Install directly from repository
pip install git+https://github.com/your-org/era-parser.git

# Verify installation
era-parser --help
```

### Method 3: Development Installation

```bash
# Clone with development dependencies
git clone https://github.com/your-org/era-parser.git
cd era-parser

# Create development environment
python -m venv era_parser_dev
source era_parser_dev/bin/activate

# Install development dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .

# Install pre-commit hooks
pre-commit install

# Run tests
pytest
```

## Configuration

### Environment Variables

Create a `.env` file in your project directory:

```bash
# .env file template
# Copy to .env and configure your values

# ClickHouse Configuration (required for ClickHouse export)
CLICKHOUSE_HOST=your-clickhouse-host.com
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=true

# Remote Processing Configuration (required for --remote commands)
ERA_BASE_URL=https://era-files-bucket.s3.eu-central-1.amazonaws.com
ERA_DOWNLOAD_DIR=./temp_era_files
ERA_CLEANUP_AFTER_PROCESS=true
ERA_MAX_RETRIES=3
ERA_MAX_CONCURRENT_DOWNLOADS=10

# Optional: Performance Tuning
ERA_DEBUG=false
ERA_BATCH_SIZE=10000
```

### Configuration Templates

**ClickHouse Cloud**:
```bash
# .env for ClickHouse Cloud
CLICKHOUSE_HOST=your-instance.clickhouse.cloud
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_DATABASE=default
CLICKHOUSE_SECURE=true
```

**Self-Hosted ClickHouse**:
```bash
# .env for self-hosted ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=false
```

**S3 Storage**:
```bash
# .env for S3-hosted era files
ERA_BASE_URL=https://your-bucket.s3.region.amazonaws.com
ERA_DOWNLOAD_DIR=/tmp/era_downloads
ERA_CLEANUP_AFTER_PROCESS=true
ERA_MAX_RETRIES=5
```

### Directory Structure

Set up recommended directory structure:

```bash
# Create project directories
mkdir -p era-parser-workspace/{config,data,output,logs,temp_era_files}
cd era-parser-workspace

# Create configuration
cp /path/to/era-parser/.env.example .env
# Edit .env with your settings

# Directory structure:
# era-parser-workspace/
# ├── .env              # Environment configuration
# ├── config/           # Additional configuration files
# ├── data/             # Local era files
# ├── output/           # Export output files
# ├── logs/             # Processing logs
# └── temp_era_files/   # Temporary downloads
```

## Verification

### Basic Installation Test

```bash
# Test CLI availability
era-parser --help

# Test with sample era file (if available)
era-parser sample.era stats

# Test configuration
python -c "
import era_parser
from era_parser.config import get_network_config
print('Era Parser version:', era_parser.__version__)
print('Mainnet config:', get_network_config('mainnet'))
"
```

### ClickHouse Connection Test

```bash
# Test ClickHouse connection
python -c "
import os
from era_parser.export.clickhouse_service import ClickHouseService

# Load environment
from dotenv import load_dotenv
load_dotenv()

try:
    service = ClickHouseService()
    print('✅ ClickHouse connection successful')
    print('Database:', service.database)
except Exception as e:
    print('❌ ClickHouse connection failed:', e)
"
```

### Remote Processing Test

```bash
# Test remote discovery (without processing)
era-parser --remote gnosis 1082 --download-only

# Test with small era range
era-parser --remote gnosis 1082 all-blocks test_output.json
```

## Docker Setup

### Quick Docker Start

```bash
# Clone repository
git clone https://github.com/your-org/era-parser.git
cd era-parser

# Copy environment template
cp .env.example .env
# Edit .env with your configuration

# Build Docker image
docker build -t era-parser:latest .

# Create directories
mkdir -p output era-files

# Test Docker installation
docker run --rm -v $(pwd)/output:/app/output era-parser:latest --help
```

### Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  era-parser:
    build: .
    image: era-parser:latest
    env_file: .env
    volumes:
      - ./output:/app/output
      - ./era-files:/app/era-files:ro
      - ./temp_era_files:/app/temp_era_files
    working_dir: /app

  shell:
    <<: *era-parser
    entrypoint: ["/bin/bash"]
    stdin_open: true
    tty: true
```

**Usage**:
```bash
# Process local era file
docker-compose run --rm era-parser /app/era-files/sample.era stats

# Remote processing
docker-compose run --rm era-parser --remote gnosis 1082 all-blocks --export clickhouse

# Interactive shell
docker-compose run --rm shell
```

### Production Docker Setup

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  era-parser:
    image: era-parser:latest
    restart: unless-stopped
    env_file: .env
    volumes:
      - ./output:/app/output
      - ./logs:/app/logs
      - era_downloads:/tmp/era_downloads
    healthcheck:
      test: ["CMD", "era-parser", "--era-status", "all"]
      interval: 30s
      timeout: 10s
      retries: 3
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

volumes:
  era_downloads:
```

## Development Setup

### Development Environment

```bash
# Clone repository
git clone https://github.com/your-org/era-parser.git
cd era-parser

# Create development environment
python -m venv era_parser_dev
source era_parser_dev/bin/activate

# Install development dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .

# Install pre-commit hooks
pre-commit install

# Set up testing environment
cp .env.example .env.test
# Configure test database and settings
```

### Development Tools

```bash
# Code formatting
black era_parser/
isort era_parser/

# Linting
flake8 era_parser/
pylint era_parser/

# Type checking
mypy era_parser/

# Testing
pytest                          # Run all tests
pytest tests/test_parsing.py    # Run specific test file
pytest -v --cov=era_parser      # Run with coverage

# Performance profiling
python -m cProfile -o profile.stats era_parser/cli.py sample.era stats
```

### IDE Configuration

**VS Code** (`.vscode/settings.json`):
```json
{
    "python.defaultInterpreterPath": "./era_parser_env/bin/python",
    "python.formatting.provider": "black",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests/"]
}
```

**PyCharm**:
1. Set Python interpreter to `era_parser_env/bin/python`
2. Configure code style to use Black
3. Enable pytest as test runner
4. Set working directory to project root

## Troubleshooting

### Installation Issues

**Python Version Issues**:
```bash
# Check Python version
python --version

# Use specific Python version
python3.10 -m venv era_parser_env
```

**Dependency Conflicts**:
```bash
# Clean installation
pip uninstall era-parser
pip cache purge
pip install --no-cache-dir -r requirements.txt
pip install -e .
```

**Snappy Library Issues**:
```bash
# macOS
brew reinstall snappy
pip uninstall python-snappy
pip install --no-cache-dir python-snappy

# Ubuntu
sudo apt-get install --reinstall libsnappy-dev
pip install --no-cache-dir python-snappy
```

### Configuration Issues

**Environment Variables Not Loading**:
```bash
# Check if .env file exists and is readable
ls -la .env
cat .env

# Test environment loading
python -c "
import os
from dotenv import load_dotenv
load_dotenv()
print('CLICKHOUSE_HOST:', os.getenv('CLICKHOUSE_HOST'))
"
```

**Permission Issues**:
```bash
# Fix directory permissions
chmod 755 era-parser/
chmod 644 .env
chmod -R 755 output/
```

### Runtime Issues

**Memory Issues**:
```bash
# Monitor memory usage
htop
# or
ps aux | grep era-parser

# Reduce batch size
export ERA_BATCH_SIZE=5000
```

**Network Issues**:
```bash
# Test connectivity
curl -I $ERA_BASE_URL
ping your-clickhouse-host.com

# Increase timeouts
export ERA_MAX_RETRIES=5
```

**ClickHouse Connection Issues**:
```bash
# Test connection manually
clickhouse-client --host your-host --secure --password

# Check firewall settings
telnet your-clickhouse-host.com 8443
```

### Getting Help

**Debug Mode**:
```bash
# Enable debug logging
export ERA_DEBUG=true
era-parser your-command

# Increase verbosity
era-parser -v your-command
```

**Log Analysis**:
```bash
# Check system logs
tail -f /var/log/messages
journalctl -f

# Application logs
tail -f logs/era-parser.log
```

**Community Support**:
- GitHub Issues: Report bugs and feature requests
- Documentation: Check docs/ directory for detailed guides
- Examples: See examples/ directory for usage patterns

### Performance Optimization

**System Tuning**:
```bash
# Increase file descriptor limits
ulimit -n 65536

# Optimize Python
export PYTHONOPTIMIZE=1

# Use faster JSON library if available
pip install orjson
```

**ClickHouse Tuning**:
```sql
-- Optimize ClickHouse settings
SET max_memory_usage = 10000000000;
SET max_threads = 8;
SET max_insert_block_size = 1048576;
```
