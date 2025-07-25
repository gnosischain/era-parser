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
│   └── DEVELOPMENT.md          # This file
│
├── era_parser/                  # Main package
│   ├── __init__.py             # Package initialization
│   ├── cli.py                  # Command line interface
│   │
│   ├── config/                 # Network and fork configurations
│   │   ├── __init__.py
│   │   ├── networks.py         # Network-specific settings
│   │   └── forks.py            # Fork definitions and detection
│   │
│   ├── ingestion/              # Era file reading and decompression
│   │   ├── __init__.py
│   │   ├── era_reader.py       # Main era file reader
│   │   └── compression.py      # Snappy decompression utilities
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
│   └── export/                 # Export formats (JSON, CSV, Parquet)
│       ├── __init__.py
│       ├── base.py             # Common export functionality
│       ├── json_exporter.py    # JSON/JSONL export
│       ├── csv_exporter.py     # CSV export
│       └── parquet_exporter.py # Parquet export
│
├── output/                     # Default output directory (gitignored)
├── tests/                      # Test files (when added)
└── era_parser_env/             # Virtual environment (gitignored)
```

## Key Implementation Details

### 1. Fork Parser Architecture

The parser uses a modular inheritance-based system where each fork extends the previous fork's functionality:

```python
# Inheritance chain: Phase0 → Altair → Bellatrix → Capella → Deneb → Electra
BaseForkParser (abstract)
├── Phase0Parser
└── AltairParser (extends Phase0Parser)
    └── BellatrixParser (extends AltairParser) 
        └── CapellaParser (extends BellatrixParser)
            └── DenebParser (extends CapellaParser)
                └── ElectraParser (extends DenebParser)

# Registry system for dynamic parser selection
FORK_PARSERS = {
    'phase0': Phase0Parser,
    'altair': AltairParser,
    'bellatrix': BellatrixParser,
    'capella': CapellaParser,
    'deneb': DenebParser,
    'electra': ElectraParser,
}

# Usage
fork = get_fork_by_slot(slot, network)
parser = get_fork_parser(fork)()
body = parser.parse_body(body_data)
```

### 2. SSZ Parsing Strategy

Each fork parser implements `parse_body()` which handles the mixed structure correctly:

```python
class BaseForkParser:
    def parse_fixed_fields(self, body_data: bytes):
        # Common 200-byte fixed fields: randao_reveal, eth1_data, graffiti
        pass
    
    def parse_base_variable_fields(self, body_data: bytes, start_pos: int):
        # 5 base variable field offsets (20 bytes)
        pass
    
    def parse_variable_field_data(self, body_data: bytes, offsets: List[int], 
                                 field_definitions: List[tuple]):
        # Parse actual variable data using offsets
        pass

class AltairParser(BaseForkParser):
    def parse_body(self, body_data: bytes):
        # 1. Parse fixed fields (200 bytes)
        # 2. Parse base variable field offsets (20 bytes) 
        # 3. Parse sync_aggregate INLINE (160 bytes)
        # 4. Parse variable field data using offsets
        pass

class BellatrixParser(AltairParser):
    def parse_body(self, body_data: bytes):
        # Extends Altair + adds execution_payload offset parsing
        pass
```

### 3. Network Configuration System

Networks are centrally configured with automatic detection:

```python
# era_parser/config/networks.py
NETWORK_CONFIGS = {
    'mainnet': {
        'GENESIS_TIME': 1606824023,
        'SECONDS_PER_SLOT': 12,
        'SLOTS_PER_EPOCH': 32,
        'FORK_EPOCHS': {...}
    },
    'gnosis': {
        'GENESIS_TIME': 1638993340,
        'SECONDS_PER_SLOT': 5,
        'SLOTS_PER_EPOCH': 16,
        'FORK_EPOCHS': {...}
    }
}

# Automatic network detection from filename
def detect_network_from_filename(filename: str) -> str:
    filename = filename.lower()
    for network in NETWORK_CONFIGS.keys():
        if network in filename:
            return network
    return 'mainnet'
```

### 4. Export System Architecture

All exporters follow a consistent interface:

```python
class BaseExporter(ABC):
    def __init__(self, era_info: Dict[str, Any]):
        self.era_info = era_info
    
    @abstractmethod
    def export_blocks(self, blocks: List[Dict], output_file: str):
        pass
    
    @abstractmethod 
    def export_data_type(self, data: List[Dict], output_file: str, data_type: str):
        pass
    
    def flatten_block_for_table(self, block: Dict) -> Dict:
        # Convert nested block structure to flat table format
        pass

# Concrete implementations
class JSONExporter(BaseExporter):
    # Preserves full nested structure
    pass

class CSVExporter(BaseExporter):
    # Flattens structure, uses JSON strings for complex fields
    pass

class ParquetExporter(BaseExporter):  
    # Optimized for analytics, includes metadata
    pass
```

## Adding New Features

### Adding a New Fork

To add a new fork (e.g., "Fulu"), you need to modify exactly 4 files:

1. **config/forks.py** - Add fork configuration:
```python
FORK_CONFIGS['fulu'] = {
    'name': 'Fulu',
    'has_validator_consolidations': True,  # New feature
    'has_advanced_attestations': True,     # New feature
}

def get_fork_by_slot(slot: int, network: str = 'mainnet') -> str:
    # Add Fulu check in the cascade
    if epoch >= fork_epochs.get('fulu', float('inf')):
        return 'fulu'
    # ... existing logic
```

2. **config/networks.py** - Add activation epoch for each network:
```python
NETWORK_CONFIGS['mainnet']['FORK_EPOCHS']['fulu'] = 1500000000
NETWORK_CONFIGS['gnosis']['FORK_EPOCHS']['fulu'] = 2000000
NETWORK_CONFIGS['sepolia']['FORK_EPOCHS']['fulu'] = 999999999
```

3. **parsing/forks/fulu.py** - Create the fork parser:
```python
from typing import Dict, Any
from ..ssz_utils import parse_list_of_items, read_uint32_at
from .electra import ElectraParser

class FuluParser(ElectraParser):
    """Parser for Fulu fork blocks"""
    
    def parse_validator_consolidation(self, data: bytes) -> Dict[str, Any]:
        # Implement Fulu-specific parsing
        return {
            "source_validator": data[0:8], 
            "target_validator": data[8:16]
        }
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        # Call parent parser first
        result = super().parse_body(body_data)
        
        # Add Fulu-specific field parsing
        # ... implementation details
        
        return result
```

4. **parsing/forks/__init__.py** - Register the parser:
```python
from .fulu import FuluParser

FORK_PARSERS = {
    # ... existing parsers ...
    'fulu': FuluParser,  # Add this line
}
```

**That's it!** The new fork automatically works with all CLI commands:
```bash
era-parser fulu-era-12345.era all-blocks fulu_data.json
era-parser fulu-era-12345.era transactions fulu_txs.csv --separate
```

### Adding a New Network

1. **config/networks.py** - Add network configuration:
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
# In era_parser/cli.py, update export_data method:
elif output_file.endswith('.xml'):
    exporter = XMLExporter(era_info)
    exporter.export_blocks(data, output_file)
```

## Development Workflow

### 1. Setup Development Environment

```bash
# Clone repository
git clone <repository-url>
cd era-parser

# Create virtual environment
python -m venv era_parser_env
source era_parser_env/bin/activate  # On Windows: era_parser_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### 2. Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=era_parser --cov-report=html
```

### 3. Code Style

The project follows Python best practices:

- Use type hints where possible
- Follow PEP 8 naming conventions
- Write docstrings for public methods
- Keep methods focused and small
- Use meaningful variable names

### 4. Testing New Parsers

When adding a new fork parser:

```python
# Test with actual era files
era-parser test-era-file.era stats
era-parser test-era-file.era block <slot>

# Verify fork detection
from era_parser.config import get_fork_by_slot
print(get_fork_by_slot(slot, 'mainnet'))

# Test parsing
from era_parser.parsing.forks import get_fork_parser
parser = get_fork_parser('new_fork')
# Test parser methods...
```

## Performance Considerations

### 1. Memory Usage

The parser processes blocks incrementally to avoid loading entire era files into memory:

- Era files are read sequentially
- Blocks are parsed one at a time
- Large data structures use generators where possible

### 2. Processing Speed

- Snappy decompression is the main bottleneck
- SSZ parsing is optimized for minimal allocations
- Progress tracking for long-running operations

### 3. Output Optimization

- Parquet format provides best compression
- Separate files mode reduces memory usage
- CSV includes metadata as comments

## Debugging Tips

### 1. SSZ Structure Issues

```python
# Debug block structure
from era_parser.parsing.ssz_utils import read_uint32_at, read_uint64_at

# Check offsets
for i in range(5):
    offset = read_uint32_at(data, 200 + i*4)
    print(f"Offset {i}: {offset}")

# Verify field boundaries
print(f"Data length: {len(data)}")
```

### 2. Fork Detection Problems

```python
# Debug fork calculation with new config system
from era_parser.config import get_fork_by_slot, get_network_config

slot = 12345
network = 'gnosis'  # or 'mainnet', 'sepolia'

# Get network config
config = get_network_config(network)
epoch = slot // config['SLOTS_PER_EPOCH']
fork = get_fork_by_slot(slot, network)

print(f"Network: {network}")
print(f"Slot {slot} → Epoch {epoch} → Fork {fork}")
print(f"Fork epochs: {config['FORK_EPOCHS']}")

# Check if slot is near a fork boundary
for fork_name, fork_epoch in config['FORK_EPOCHS'].items():
    slot_boundary = fork_epoch * config['SLOTS_PER_EPOCH']
    if abs(slot - slot_boundary) < 100:  # Within 100 slots
        print(f"⚠️  Slot {slot} is near {fork_name} fork boundary at slot {slot_boundary}")
```

### 3. Export Issues

```python
# Test export without full processing
from era_parser.export import JSONExporter, CSVExporter

# Create minimal test data
test_blocks = [{"slot": "123", "data": {"message": {"proposer_index": "1"}}}]
era_info = {"era_number": 1, "network": "test", "start_slot": 0, "end_slot": 8191}

# Test JSON export
json_exporter = JSONExporter(era_info)
json_exporter.export_blocks(test_blocks, "test_output.json")

# Test CSV export  
csv_exporter = CSVExporter(era_info)
csv_exporter.export_blocks(test_blocks, "test_output.csv")

print("Export test completed - check output/ directory")

# Test CLI export
import subprocess
result = subprocess.run([
    'era-parser', 'test.era', 'all-blocks', 'test.json'
], capture_output=True, text=True)

if result.returncode != 0:
    print("CLI Error:", result.stderr)
else:
    print("CLI Success:", result.stdout)
```

## Release Process

### 1. Version Bumping

Update version in:
- `setup.py`
- `era_parser/__init__.py`

### 2. Release Notes

Document changes in:
- New features
- Bug fixes
- Breaking changes
- Migration notes

### 3. Testing

- Test with multiple era files
- Verify all export formats
- Check backward compatibility

This development guide should help contributors understand the codebase structure and development workflow.