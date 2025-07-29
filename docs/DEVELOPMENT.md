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
│   │   ├── processor.py        # Main processing coordinator
│   │   ├── era_slot_calculator.py # Era/slot mapping utilities
│   │   └── output_manager.py   # Output file management
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
│       ├── clickhouse_service.py   # ClickHouse connection service
│       ├── era_state_manager.py    # Unified era processing state
│       └── migrations/         # Database migrations
│           ├── __init__.py
│           ├── migration_manager.py
│           ├── base_migration.py
│           ├── 001_initial_tables.py
│           ├── 002_performance_optimizations.py
│           └── sql/            # SQL migration files
│
├── output/                     # Default output directory (gitignored)
├── tests/                      # Test files (when added)
└── era_parser_env/             # Virtual environment (gitignored)
```

## Key Implementation Details

### 1. Unified State Management Architecture

The new system uses a single `EraStateManager` that handles all era processing state:

#### `EraStateManager` (export/era_state_manager.py)
Central coordinator for all state operations:
```python
class EraStateManager:
    """Unified era state management with data cleanup and completion tracking"""
    
    def determine_eras_to_process(self, network: str, available_eras: List[Tuple[int, str]], 
                                 force: bool = False) -> List[Tuple[int, str]]:
        """
        Determine which eras need processing - UNIFIED LOGIC
        
        Args:
            network: Network name
            available_eras: List of (era_number, url) tuples
            force: Whether to force reprocess everything
            
        Returns:
            List of (era_number, url) tuples to process
        """
```

**Processing Modes:**
- **Normal Mode** (`force=False`): Skip completed eras, process remaining
- **Force Mode** (`force=True`): Clean all eras first, then process everything

#### Era Completion Tracking
```python
def record_era_completion(self, era_number: int, network: str, 
                         datasets_processed: List[str], total_records: int) -> None:
    """Record successful era completion"""
    
def record_era_failure(self, era_number: int, network: str, error_message: str) -> None:
    """Record era processing failure"""
```

#### Data Cleaning Operations
```python
def clean_era_completely(self, network: str, era_number: int) -> None:
    """Remove ALL data for an era's slot range - used for force mode"""
    
def era_has_partial_data(self, era_number: int, network: str) -> bool:
    """Check if era has partial data in any table"""
```

### 2. Simplified Processing Architecture

The new `EraProcessor` uses a streamlined approach:

```python
class EraProcessor:
    """Core era processing functionality"""
    
    def setup(self, era_file: str, network: str = None):
        """Setup processor with era file"""
        self.network = network or detect_network_from_filename(era_file)
        self.network_config = get_network_config(self.network)
        self.era_reader = EraReader(era_file, self.network)
        self.block_parser = BlockParser(self.network)
    
    def process_single_era(self, command: str, output_file: str, separate_files: bool, export_type: str = "file") -> bool:
        """Process a single era file"""
```

### 3. Fork-Specific Parsing with Schema Declaration

Fork parsers now use a declarative schema approach:

```python
class AltairParser(Phase0Parser):
    """Parser for Altair fork blocks - adds sync_aggregate"""
    
    # Define block body schema declaratively
    BODY_SCHEMA = [
        ('fixed', 'sync_aggregate', 160),  # Fixed 160-byte sync aggregate
    ]

class BellatrixParser(AltairParser):
    """Parser for Bellatrix fork blocks - adds execution_payload"""
    
    # Inherit Altair schema and add execution_payload
    BODY_SCHEMA = AltairParser.BODY_SCHEMA + [
        ('variable', 'execution_payload', 'parse_execution_payload_bellatrix'),
    ]
```

### 4. Migration System

The codebase now includes a complete migration system:

```python
class MigrationManager:
    """Manages ClickHouse schema migrations with backward compatibility"""
    
    def run_migrations(self, target_version: Optional[str] = None) -> bool:
        """Run pending migrations up to target version"""
```

**Available Migration Commands:**
```bash
era-parser --migrate status                  # Show migration status
era-parser --migrate run [version]           # Run migrations
era-parser --migrate list                    # List available migrations
```

## Adding New Features

### Adding a New Fork Parser

1. **Create the parser file** in `parsing/forks/fulu.py`:
```python
from .electra import ElectraParser

class FuluParser(ElectraParser):
    """Parser for Fulu fork (example future fork)"""
    
    # Inherit Electra schema and add new fields
    BODY_SCHEMA = ElectraParser.BODY_SCHEMA + [
        ('variable', 'new_fulu_field', 'parse_new_fulu_field'),
    ]
    
    def parse_new_fulu_field(self, data: bytes) -> Dict[str, Any]:
        """Parse new field introduced in Fulu fork"""
        # Implementation specific to the new field
        pass
```

2. **Register the parser** in `parsing/forks/__init__.py`:
```python
from .fulu import FuluParser

FORK_PARSERS = {
    # ... existing parsers ...
    'fulu': FuluParser,
}
```

3. **Update network config** in `config/networks.py`:
```python
NETWORK_CONFIGS['mainnet']['FORK_EPOCHS']['fulu'] = 999999999  # Future epoch
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

2. **Update network detection** in `config/networks.py`:
```python
def detect_network_from_filename(filename: str) -> str:
    """Detect network from era filename"""
    filename = filename.lower()
    if 'holesky' in filename:
        return 'holesky'
    # ... existing logic
```

### Adding a New Export Format

1. Create `export/xml_exporter.py`:
```python
from .base import BaseExporter

class XMLExporter(BaseExporter):
    """Exporter for XML format"""
    
    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        # Implementation
        pass
```

2. Update processing logic in `core/processor.py`:
```python
elif output_file.endswith('.xml'):
    from ..export.xml_exporter import XMLExporter
    exporter = XMLExporter(era_info)
    exporter.export_blocks(data, output_file)
```

### Adding a New CLI Command

1. Create command module `commands/analyze.py`:
```python
from .base import BaseCommand

class AnalyzeCommand(BaseCommand):
    """Handler for data analysis operations"""
    
    def execute(self, args: List[str]) -> None:
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

1. **Create migration** in `export/migrations/`:
```python
# 003_add_new_field.py
def up(client, database: str):
    """Add new field to table"""
    client.command(f"""
        ALTER TABLE {database}.blocks 
        ADD COLUMN new_field String DEFAULT ''
    """)

def down(client, database: str):
    """Remove new field"""
    client.command(f"""
        ALTER TABLE {database}.blocks 
        DROP COLUMN new_field
    """)
```

2. **Run migration**:
```bash
era-parser --migrate run
```

3. **Update table definitions** in `export/clickhouse_service.py`
4. **Document changes** in `CLICKHOUSE.md`

### 5. Performance Optimization

#### Parsing Performance
- Use declarative schema approach for consistent parsing
- Implement fixed-size parsing for known structures
- Profile parsing bottlenecks with `cProfile`

#### ClickHouse Performance  
- Use single global batch size (100,000 records)
- Optimize data type usage (UInt64 vs String)
- Monitor query performance with `EXPLAIN`

#### State Management
- Unified state manager eliminates redundant checks
- Single timestamp approach improves partitioning
- Era completion tracking enables efficient resume

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
def test_era_state_manager():
    state_manager = EraStateManager()
    
    # Test normal mode
    result = state_manager.determine_eras_to_process("mainnet", available_eras, force=False)
    assert len(result) <= len(available_eras)  # May skip completed
    
    # Test force mode  
    result = state_manager.determine_eras_to_process("mainnet", available_eras, force=True)
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
- Benchmark ClickHouse insert performance with unified batch size
- Test memory usage under different processing modes

## Deployment

### Production Deployment
1. **Environment Setup**: Configure production ClickHouse cluster
2. **Migration**: Run database migrations before deployment
3. **Monitoring**: Set up logging and metrics collection
4. **Backup Strategy**: Regular ClickHouse backups and era file archival

### Scaling Considerations
- **Horizontal Scaling**: Multiple parser instances with different era ranges
- **Database Sharding**: Partition data by network or time period
- **State Management**: Unified state manager handles concurrent access
- **Load Balancing**: Distribute processing across multiple workers

---

**Next Steps**: Check out the [Era File Format Guide](ERA_FILE_FORMAT.md) for low-level implementation details.