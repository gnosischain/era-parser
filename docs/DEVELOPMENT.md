# Development Guide

## Project Structure

```
era-parser/
├── README.md                    # Main project documentation
├── setup.py                     # Package setup and installation
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore patterns
│
├── docs/                        # Documentation
│   ├── ERA_FILE_FORMAT.md      # Detailed era file format specification
│   ├── CLICKHOUSE.md           # ClickHouse integration guide
│   ├── PARSED_FIELDS.md        # Complete field documentation
│   └── DEVELOPMENT.md          # This file
│
├── era_parser/                  # Main package
│   ├── __init__.py             # Package initialization
│   ├── cli.py                  # Command line interface
│   │
│   ├── commands/               # CLI command implementations
│   │   ├── __init__.py
│   │   ├── base.py             # Common command functionality
│   │   ├── local.py            # Local file processing
│   │   ├── remote.py           # Remote era processing  
│   │   ├── batch.py            # Batch operations
│   │   ├── state.py            # State management commands
│   │   └── migrate.py          # Database migrations
│   │
│   ├── core/                   # Business logic
│   │   ├── __init__.py
│   │   ├── era_processor.py         # Main processing coordinator
│   │   ├── resume_handler.py        # Resume and force mode logic
│   │   ├── era_data_cleaner.py      # Data cleanup operations
│   │   ├── era_completion_manager.py # Completion status tracking
│   │   └── output_manager.py        # Output file management
│   │
│   ├── config/                 # Network and fork configurations
│   │   ├── __init__.py
│   │   ├── networks.py         # Network-specific settings
│   │   └── forks.py            # Fork definitions and detection
│   │
│   ├── ingestion/              # Era file reading and remote downloading
│   │   ├── __init__.py
│   │   ├── era_reader.py       # Main era file reader
│   │   ├── compression.py      # Snappy decompression utilities
│   │   └── remote_downloader.py # Remote era file downloading
│   │
│   ├── parsing/                # Block parsing with fork-specific logic
│   │   ├── __init__.py
│   │   ├── block_parser.py     # Main parsing coordinator
│   │   ├── ssz_utils.py        # SSZ parsing utilities
│   │   └── forks/              # Individual fork parsers
│   │       ├── __init__.py
│   │       ├── base.py         # Common parsing functionality
│   │       ├── phase0.py       # Phase 0 parser
│   │       ├── altair.py       # Altair parser
│   │       ├── bellatrix.py    # Bellatrix parser
│   │       ├── capella.py      # Capella parser
│   │       ├── deneb.py        # Deneb parser
│   │       └── electra.py      # Electra parser
│   │
│   └── export/                 # Export formats and ClickHouse integration
│       ├── __init__.py
│       ├── base.py             # Common export functionality
│       ├── json_exporter.py    # JSON/JSONL export
│       ├── csv_exporter.py     # CSV export
│       ├── parquet_exporter.py # Parquet export
│       ├── clickhouse_exporter.py # ClickHouse integration
│       └── era_state_manager.py   # Era processing state management
│
├── output/                     # Default output directory (gitignored)
├── tests/                      # Test files (when added)
└── era_parser_env/             # Virtual environment (gitignored)
```

## Key Implementation Details

### 1. Resume Logic Architecture

The resume system is built around three core components:

#### `ResumeHandler` (core/resume_handler.py)
Central coordinator for processing mode logic:
```python
def get_eras_to_process(self, network: str, available_eras: List[Tuple[int, str]], 
                       resume: bool = False, force: bool = False) -> List[Tuple[int, str]]:
    """
    Determine which eras need processing
    
    Args:
        network: Network name
        available_eras: List of (era_number, url) tuples
        resume: Whether to resume (skip completed eras) - Note: removed from remote processing
        force: Whether to force reprocess everything
        
    Returns:
        List of (era_number, url) tuples to process
    """
```

**Processing Modes:**
- **Normal Mode** (`force=False`): Process all eras
- **Force Mode** (`force=True`): Clean all eras first, then process everything

#### `EraDataCleaner` (core/era_data_cleaner.py)  
Handles data cleanup operations:
```python
def clean_era_completely(self, network: str, era_number: int) -> None:
    """Clean all data for an era's slot range - used for force mode"""
    
def era_has_partial_data(self, network: str, era_number: int) -> bool:
    """Check if era has partial data in any table"""
    
def get_completed_eras(self, network: str, start_era: int = None, end_era: int = None) -> Set[int]:
    """Get completed eras from era_completion table"""
```

#### `EraCompletionManager` (core/era_completion_manager.py)
Tracks processing completion status:
```python
def record_era_completion(self, network: str, era_number: int, 
                         total_records: int, datasets_processed: List[str]) -> None:
    """Record successful era completion"""
    
def record_era_failure(self, network: str, era_number: int, error_message: str) -> None:
    """Record era processing failure"""
```

### 2. Era State Management

The `EraStateManager` maintains processing state in ClickHouse:

```sql
-- Era completion tracking table
CREATE TABLE era_completion (
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
    retry_count UInt8
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (network, era_number);
```

**Key Features:**
- **Atomic Operations**: Era status updates are atomic
- **Retry Tracking**: Tracks failure attempts and retry counts
- **Dataset Granularity**: Records which datasets were successfully processed
- **Slot Range Mapping**: Maps era numbers to slot ranges for data cleanup

### 3. CLI Command Structure

The CLI is organized into command modules:

#### `commands/remote.py`
Handles remote era processing:
```python
class RemoteCommand(BaseCommand):
    def execute(self, args: List[str]) -> None:
        # Parse arguments
        network = args[0]
        era_range = args[1] 
        command = args[2] if len(args) > 2 else None
        
        # Check for flags
        force = '--force' in args
        
        # Process era range
        downloader = get_remote_era_downloader()
        result = downloader.process_era_range(
            start_era, end_era,
            command=command,
            force=force,
            export_type=export_type
        )
```

#### `commands/state.py`
Manages processing state:
```python
# Available commands:
era-parser --era-status <network>     # Check completion status
era-parser --era-failed <network>     # View failed eras  
era-parser --era-cleanup <days>       # Clean old records
era-parser --era-check <network> <range> # Check specific eras
```

### 4. Remote Processing Flow

The remote processing workflow:

1. **Discovery**: Find available era files on remote server
2. **Force Mode Logic**: Determine which eras need processing based on force flag
3. **Download**: Download era files to temporary directory
4. **Process**: Extract and export data using era processor
5. **Tracking**: Record completion status in ClickHouse
6. **Cleanup**: Remove temporary files

```python
# In remote_downloader.py
def process_era_range(self, start_era: int, end_era: Optional[int] = None,
                     command: str = "all-blocks", base_output: str = "output",
                     separate_files: bool = False, force: bool = False,
                     export_type: str = "file") -> Dict[str, Any]:
    
    # Get all available eras
    available_eras = self.discover_era_files(start_era, end_era)
    
    if force:
        # Force mode: clean all eras first
        for era_number, _ in available_eras:
            state_manager.clean_era_data(era_number, self.network)
        return available_eras
    
    # Normal mode: process all eras (simplified logic)
    return available_eras
```

## Adding New Features

### Adding a New Fork Parser

1. **Create the parser file** in `parsing/forks/fulu.py`:
```python
from typing import Dict, Any, Optional
from .base import BaseForkParser

class FuluParser(BaseForkParser):
    """Parser for Fulu fork (example future fork)"""
    
    FORK_NAME = "fulu"
    
    def parse_beacon_block_body(self, body_data: bytes) -> Optional[Dict[str, Any]]:
        """Parse Fulu fork beacon block body"""
        # Inherit base parsing
        parsed_data = super().parse_beacon_block_body(body_data)
        if not parsed_data:
            return None
        
        # Add new Fulu-specific fields
        try:
            # Example: new field at end of body
            new_field_offset = self.get_offset_at(body_data, 15)  # 16th field
            new_field_data = body_data[new_field_offset:]
            parsed_data['new_fulu_field'] = self.parse_new_field(new_field_data)
        except Exception as e:
            print(f"Failed to parse Fulu-specific field: {e}")
        
        return parsed_data
    
    def parse_new_field(self, data: bytes) -> Any:
        """Parse new field introduced in Fulu fork"""
        # Implementation specific to the new field
        pass
```

2. **Register the parser** in `parsing/forks/__init__.py`:
```python
from .fulu import FuluParser

FORK_PARSERS = {
    # ... existing parsers ...
    'fulu': FuluParser,  # Add this line
}
```

3. **Update network config** in `config/networks.py`:
```python
NETWORK_CONFIGS['mainnet']['FORK_EPOCHS']['fulu'] = 999999999  # Future epoch
```

**That's it!** The new fork automatically works with all CLI commands:
```bash
era-parser fulu-era-12345.era all-blocks fulu_data.json
era-parser --remote mainnet 12345 all-blocks --export clickhouse
```

### Adding a New Network

1. **Add network configuration** in `config/networks.py`:
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

2. **Test network detection** (automatic from filename):
```python
from era_parser.config import detect_network_from_filename

# Should auto-detect 'holesky' from filename
network = detect_network_from_filename('holesky-01234-abcdef.era')
assert network == 'holesky'
```

The network is now available in all CLI commands and programmatic usage.

### Adding a New Export Format

1. Create `export/xml_exporter.py` (example):
```python
from typing import List, Dict, Any
import xml.etree.ElementTree as ET
from .base import BaseExporter

class XMLExporter(BaseExporter):
    """Exporter for XML format"""
    
    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        root = ET.Element("era_data")
        
        # Add metadata
        metadata = ET.SubElement(root, "metadata")
        for key, value in self.create_metadata(len(blocks)).items():
            ET.SubElement(metadata, key).text = str(value)
        
        # Add blocks
        blocks_elem = ET.SubElement(root, "blocks")
        for block in blocks:
            block_elem = ET.SubElement(blocks_elem, "block")
            # Convert block to XML structure...
        
        # Write to file
        tree = ET.ElementTree(root)
        tree.write(f"output/{output_file}", encoding='utf-8', xml_declaration=True)
    
    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        # Implementation for specific data types
        pass
```

2. Register in `export/__init__.py`:
```python
from .xml_exporter import XMLExporter

__all__ = ["BaseExporter", "JSONExporter", "CSVExporter", "ParquetExporter", "XMLExporter"]
```

3. Update CLI to support new format:
```python
# In era_processor.py, update export logic:
elif output_file.endswith('.xml'):
    exporter = XMLExporter(era_info)
    exporter.export_blocks(data, output_file)
```

### Adding a New CLI Command

1. Create command module `commands/analyze.py`:
```python
from typing import List
from .base import BaseCommand

class AnalyzeCommand(BaseCommand):
    """Handler for data analysis operations"""
    
    def execute(self, args: List[str]) -> None:
        """Execute analysis command"""
        if not self.validate_required_args(args, 2, "era-parser --analyze <network> <analysis_type>"):
            return
        
        network = args[0]
        analysis_type = args[1]
        
        if analysis_type == "validator-performance":
            self._analyze_validator_performance(network)
        elif analysis_type == "network-health":
            self._analyze_network_health(network)
        else:
            print(f"❌ Unknown analysis type: {analysis_type}")
    
    def _analyze_validator_performance(self, network: str):
        # Implementation
        pass
```

2. Register in `cli.py`:
```python
elif first_arg == "--analyze":
    from .commands.analyze import AnalyzeCommand
    command = AnalyzeCommand()
    command.execute(sys.argv[2:])
```

## Development Workflow

### 1. Setting Up Development Environment
```bash
# Clone repository
git clone <repository-url>
cd era-parser

# Create virtual environment
python -m venv era_parser_env
source era_parser_env/bin/activate  # or era_parser_env\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Set up ClickHouse for testing
docker run -d --name clickhouse-dev -p 8123:8123 clickhouse/clickhouse-server
```

### 2. Testing Changes
```bash
# Test with local era file
era-parser test-era-file.era blocks test_output.json

# Test remote processing
era-parser --remote gnosis 1000 blocks --export clickhouse

# Test force mode
era-parser --remote gnosis 1000 blocks --export clickhouse --force
```

### 3. Debugging Processing Issues
```python
# Enable debug logging in era_processor.py
import logging
logging.basicConfig(level=logging.DEBUG)

# Check era completion status
era-parser --era-status gnosis

# View failed eras
era-parser --era-failed gnosis

# Check specific era
era-parser --era-check gnosis 1000-1002
```

### 4. Database Schema Changes

When modifying ClickHouse schema:

1. **Update table definitions** in `export/clickhouse_exporter.py`
2. **Create migration** in `commands/migrate.py`
3. **Test migration** on development database
4. **Document changes** in `CLICKHOUSE.md`

Example migration:
```python
def migrate_add_new_field():
    """Add new field to blocks table"""
    try:
        client.command("""
            ALTER TABLE beacon_chain.blocks 
            ADD COLUMN new_field String DEFAULT ''
        """)
        print("✅ Added new_field to blocks table")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
```

### 5. Performance Optimization

#### Parsing Performance
- Use fixed-size parsing where possible (avoid SSZ offset calculations)
- Implement parsing caches for repeated structures
- Profile parsing bottlenecks with `cProfile`

#### ClickHouse Performance
- Use appropriate data types (UInt64 vs String)
- Add indexes for common query patterns
- Optimize batch insert sizes
- Monitor query performance with `EXPLAIN`

#### Memory Management
- Process eras in streaming fashion for large datasets
- Clean up temporary files promptly
- Monitor memory usage during batch processing

## Code Style Guidelines

### Python Code Style
- Follow PEP 8 with 100-character line limit
- Use type hints for all function parameters and returns
- Add docstrings for all public methods
- Use descriptive variable names

### Error Handling
```python
# Good: Specific exception handling with logging
try:
    result = self.process_era(era_file)
except EraParsingError as e:
    logger.error(f"Failed to parse era {era_file}: {e}")
    return None
except ClickHouseError as e:
    logger.error(f"Database error processing {era_file}: {e}")
    raise
```

### Logging
```python
import logging
logger = logging.getLogger(__name__)

# Use appropriate log levels
logger.debug("Processing era file: %s", era_file)
logger.info("Completed era %d with %d records", era_number, record_count)
logger.warning("Era %d has partial data, cleaning", era_number)
logger.error("Failed to process era %d: %s", era_number, error)
```

## Testing Strategy

### Unit Tests
```python
# Test individual components
def test_resume_handler():
    handler = ResumeHandler(mock_client, "test_db")
    
    # Test normal mode
    result = handler.get_eras_to_process("mainnet", available_eras, force=False)
    assert len(result) == len(available_eras)
    
    # Test force mode  
    result = handler.get_eras_to_process("mainnet", available_eras, force=True)
    assert len(result) == len(available_eras)  # Should process all after cleanup
```

### Integration Tests
```python
# Test complete processing workflows
def test_remote_processing_with_force():
    # Initial processing
    result1 = process_era_range("gnosis", 1000, 1002, force=False)
    assert result1["success"]
    
    # Force should clean and reprocess
    result2 = process_era_range("gnosis", 1000, 1002, force=True)
    assert result2["success"]
```

### Performance Tests
- Test parsing performance with various era file sizes
- Benchmark ClickHouse insert performance
- Test memory usage under different processing modes

## Deployment

### Production Deployment
1. **Environment Setup**: Configure production ClickHouse cluster
2. **Resource Planning**: Estimate CPU, memory, and storage requirements
3. **Monitoring**: Set up logging and metrics collection
4. **Backup Strategy**: Regular ClickHouse backups and era file archival

### Scaling Considerations
- **Horizontal Scaling**: Multiple parser instances with different era ranges
- **Database Sharding**: Partition data by network or time period
- **Caching**: Cache frequently accessed era completion data
- **Load Balancing**: Distribute processing across multiple workers

---

**Next Steps**: Check out the [Era File Format Guide](ERA_FILE_FORMAT.md) for low-level implementation details.