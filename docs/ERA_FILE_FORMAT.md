# ERA Files: A Guide to Gnosis/Ethereum Beacon Chain Archive Format

## Table of Contents
1. [What are ERA Files?](#what-are-era-files)
2. [File Structure & SSZ Encoding](#file-structure--ssz-encoding)
3. [Fork Evolution](#fork-evolution)
4. [Era File Naming Convention](#era-file-naming-convention)
5. [Understanding Slot and Era Mapping](#understanding-slot-and-era-mapping)
6. [BeaconBlockBody Structure Evolution](#beaconblockbody-structure-evolution)
7. [Critical SSZ Parsing Concepts](#critical-ssz-parsing-concepts)
8. [Parsing Implementation Guide](#parsing-implementation-guide)
9. [Tools & Utilities](#tools--utilities)

## What are ERA Files?

ERA files (`.era`) are specialized archive files designed for long-term storage and distribution of Gnosis/Ethereum beacon chain historical data. They are based on the e2store format but follow a strict content structure optimized for efficient access to blocks and states.

### Key Features:
- **Compressed storage**: Uses snappy frame compression
- **Self-contained**: Each era file contains complete block and state data
- **Efficient access**: Includes indices for fast random access
- **Chain verification**: Enables cryptographic verification of chain history
- **Cold storage optimized**: Designed for archival and distribution

## File Structure & SSZ Encoding

### Basic E2Store Structure
```
era := group+
group := Version | block* | era-state | other-entries* | slot-index(block)? | slot-index(state)
```

### Record Types
- **Version** (`0x65 0x32`): File format identifier
- **CompressedSignedBeaconBlock** (`0x01 0x00`): Snappy-compressed SSZ blocks
- **CompressedBeaconState** (`0x02 0x00`): Snappy-compressed SSZ states
- **SlotIndex** (`0x69 0x32`): Offset indices for efficient access

### SSZ Encoding Fundamentals

**Simple Serialize (SSZ)** is Ethereum's serialization format with two field types:

#### Fixed-Size Fields
- Stored **inline** in the container
- No offset pointers needed
- Examples: `uint64`, `Bytes32`, `BLSSignature`

#### Variable-Size Fields  
- Use **4-byte offset pointers**
- Actual data stored at the end
- Examples: `List[T, N]`, `ByteList[N]`

#### Container Layout Example
```
[Fixed Fields] [Offsets] [Variable Data]
bytes 0-199:   Fixed fields (randao_reveal, eth1_data, graffiti)
bytes 200-219: Offsets (5 × 4 bytes for variable fields)
bytes 220-379: sync_aggregate (160 bytes - FIXED SIZE, inline)
bytes 380-383: execution_payload offset (4 bytes)
bytes 384+:    Variable data (blocks, attestations, execution payload)
```

## Fork Evolution

Ethereum beacon chain has evolved through several forks, each adding new fields to the `BeaconBlockBody` structure:

### Phase 0 (Genesis)
- **Fields**: 8 total (3 fixed + 5 variable)
- **Variable fields**: proposer_slashings, attester_slashings, attestations, deposits, voluntary_exits

### Altair Fork (2021)
- **Added**: `sync_aggregate` (160 bytes, **fixed-size**, embedded inline)
- **Purpose**: Enables light client synchronization

### Bellatrix Fork (The Merge, 2022) 
- **Added**: `execution_payload` (**variable-size**, uses offset)
- **Purpose**: Bridges beacon chain with execution layer

### Capella Fork (2023)
- **Added**: `bls_to_execution_changes` (variable-size)
- **Modified**: `execution_payload` now includes `withdrawals` list

### Deneb Fork (2024)
- **Added**: `blob_kzg_commitments` (variable-size)
- **Purpose**: Enables Proto-Danksharding (EIP-4844)

### Electra Fork (2025)
- **Added**: `execution_requests` (variable-size container)
- **Structure**: `{deposits: [], withdrawals: [], consolidations: []}`
- **Purpose**: Validator lifecycle management through execution layer

## Era File Naming Convention

Era files follow the pattern: `<config-name>-<era-number>-<short-historical-root>.era`

### Filename Analysis Example
From the filename `mainnet-02612-24eac76f.era`:
* **Era number**: 2612
* **Config**: mainnet 
* **Hash**: 24eac76f

### Slot Range Calculation
Each era covers `SLOTS_PER_HISTORICAL_ROOT = 8192` slots.

For **era 2612**:
* **Start slot**: `2612 × 8192 = 21,381,184`
* **End slot**: `21,381,184 + 8191 = 21,389,375`

So this era file covers **slots 21,381,184 to 21,389,375**.

### Finding Test Slots
You can use any slot in that range. For example:
* First slot: `21381184`
* Middle slot: `21385280` 
* Last slot: `21389375`

### Components:
- **config-name**: Network identifier (`mainnet`, `gnosis`, `sepolia`, etc.)
- **era-number**: 5-digit zero-padded era number (00000-99999)
- **short-historical-root**: First 4 bytes of the era's historical root (8 hex chars)

## Understanding Slot and Era Mapping

### Basic Relationships
```
SLOTS_PER_HISTORICAL_ROOT = 8192  (for most networks)
SLOTS_PER_EPOCH = 32             (mainnet) or 16 (gnosis)
SECONDS_PER_SLOT = 12            (mainnet) or 5 (gnosis)
```

### Era Calculation
```python
era_number = slot // SLOTS_PER_HISTORICAL_ROOT
era_start_slot = era_number * SLOTS_PER_HISTORICAL_ROOT
era_end_slot = era_start_slot + SLOTS_PER_HISTORICAL_ROOT - 1
```

### Fork Detection by Slot
```python
# Example: Gnosis Chain Fork Epochs
FORK_EPOCHS = {
    'altair': 512,
    'bellatrix': 385536, 
    'capella': 648704,
    'deneb': 889856,
    'electra': 1337856
}

def get_fork_by_slot(slot: int, slots_per_epoch: int = 32) -> str:
    epoch = slot // slots_per_epoch
    if epoch >= 1337856: return "electra"
    if epoch >= 889856: return "deneb"
    if epoch >= 648704: return "capella"
    if epoch >= 385536: return "bellatrix"
    if epoch >= 512: return "altair"
    return "phase0"
```

## BeaconBlockBody Structure Evolution

### Critical Insight: Mixed Fixed/Variable Field Handling

The `BeaconBlockBody` contains both fixed-size and variable-size fields, requiring different parsing approaches:

#### Fixed-Size Fields (Embedded Inline)
```python
# sync_aggregate: ALWAYS 160 bytes, no offset needed
sync_committee_bits: Bitvector[512]     # 64 bytes
sync_committee_signature: BLSSignature  # 96 bytes
# Total: 160 bytes embedded directly in the structure
```

#### Variable-Size Fields (Use Offsets)
```python
# execution_payload: Variable size, requires 4-byte offset pointer
transactions: List[Transaction, MAX_TRANSACTIONS]  # Variable!
withdrawals: List[Withdrawal, MAX_WITHDRAWALS]     # Variable!
```

### Binary Layout Evolution

#### Altair+ Layout
```
[0x000-0x05F] randao_reveal (96 bytes)
[0x060-0x0A7] eth1_data (72 bytes)  
[0x0A8-0x0C7] graffiti (32 bytes)
[0x0C8-0x0DF] Variable field offsets (5 × 4 bytes)
[0x0E0-0x17F] sync_aggregate (160 bytes INLINE)
[0x180-0x183] execution_payload offset (Bellatrix+)
[0x184-0x187] bls_to_execution_changes offset (Capella+)
[0x188-0x18B] blob_kzg_commitments offset (Deneb+) 
[0x18C-0x18F] execution_requests offset (Electra+)
[variable]    Actual variable data
```

## Critical SSZ Parsing Concepts

### 1. Field Type Recognition
```python
# Incorrect approach: Treating sync_aggregate as variable-size
sync_aggregate_offset = read_uint32_le(data[offset_pos])

# Correct approach: Reading sync_aggregate inline
sync_aggregate_pos = 200 + (5 * 4)  # After fixed + 5 offsets
sync_aggregate = data[sync_aggregate_pos:sync_aggregate_pos+160]
```

### 2. Offset Calculation
```python
# execution_payload offset comes AFTER sync_aggregate data
execution_payload_offset_pos = 200 + (5 * 4) + 160
```

### 3. List Parsing Strategy
```python
def parse_list_of_items(data: bytes, item_parser_func):
    # Fixed-size items (withdrawals, deposit requests)
    if item_parser_func.__name__ in FIXED_SIZE_PARSERS:
        item_size = ITEM_SIZES[item_parser_func.__name__]
        return [item_parser_func(data[i*item_size:(i+1)*item_size]) 
                for i in range(len(data) // item_size)]
    
    # Variable-size items (attestations)
    first_offset = read_uint32_at(data, 0)
    num_items = first_offset // 4
    offsets = [read_uint32_at(data, i*4) for i in range(num_items)]
    # ... parse using offsets
```

## Parsing Implementation Guide

### 1. Era File Navigation 
```python
# New approach using EraReader class
from era_parser.ingestion import EraReader

def read_era_file(filepath: str, network: str = None):
    # EraReader handles all era file operations
    era_reader = EraReader(filepath, network)
    
    # Get era information
    era_info = era_reader.get_era_info()
    
    # Get statistics
    stats = era_reader.get_statistics()
    
    # Get all block records (slot, compressed_data) pairs
    block_records = era_reader.get_block_records()
    
    return era_info, stats, block_records

# EraReader internally handles:
# - Network detection from filename
# - Reading and parsing e2store records
# - Extracting slot numbers from compressed blocks
# - Providing convenient access methods
```

### 2. Block Extraction (New Modular Structure)
```python
# New approach using EraReader and BlockParser classes
from era_parser.ingestion import EraReader
from era_parser.parsing import BlockParser

def extract_block(era_file: str, target_slot: int, network: str = None):
    # Use EraReader to read era file
    era_reader = EraReader(era_file, network)
    
    # Get all block records
    block_records = era_reader.get_block_records()
    
    # Find target block
    for slot, compressed_data in block_records:
        if slot == target_slot:
            # Use BlockParser to parse the block
            block_parser = BlockParser(network or era_reader.network)
            return block_parser.parse_block(compressed_data, slot)
    
    return None
```

### 3. Fork-Aware Field Parsing 
```python
# New modular approach using fork-specific parsers
from era_parser.parsing.block_parser import BlockParser
from era_parser.parsing.forks import get_fork_parser

def parse_block_with_new_structure(compressed_data: bytes, slot: int, network: str):
    # Main block parser coordinates the process
    block_parser = BlockParser(network)
    return block_parser.parse_block(compressed_data, slot)

# Inside BlockParser.parse_block():
def parse_block(self, compressed_data: bytes, slot: int):
    # Decompress data
    decompressed_data = decompress_snappy_framed(compressed_data)
    
    # Determine fork
    fork = get_fork_by_slot(slot, self.network)
    
    # Get fork-specific parser
    fork_parser = get_fork_parser(fork)
    
    # Parse body using fork-specific logic
    body = fork_parser.parse_body(message_data[body_offset:])

# Each fork parser implements parse_body() differently:
class AltairParser(BaseForkParser):
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        # Fork-specific parsing logic here
        pass
```

## Tools & Utilities

### 1. Era File Analysis
```bash
# List era file contents
era-parser stats example-02612-24eac76f.era

# Extract specific block  
era-parser example-02612-24eac76f.era block 21385280
```

### 2. SSZ Debugging
```bash
# Install ZCLI for SSZ validation
go install github.com/protolambda/zcli@latest

# Validate block structure
zcli pretty deneb BeaconBlockBody block.ssz

# Convert to JSON
zcli convert deneb BeaconBlockBody block.ssz block.json
```

### 3. Slot/Era Calculation 
```python
# New modular config system
from era_parser.config import get_fork_by_slot, get_network_config

def era_info(slot: int, network: str = 'mainnet'):
    # Get network configuration
    config = get_network_config(network)
    
    era = slot // config['SLOTS_PER_HISTORICAL_ROOT']
    era_start = era * config['SLOTS_PER_HISTORICAL_ROOT']
    era_end = era_start + config['SLOTS_PER_HISTORICAL_ROOT'] - 1
    epoch = slot // config['SLOTS_PER_EPOCH']
    fork = get_fork_by_slot(slot, network)
    
    print(f"Slot {slot} ({network}):")
    print(f"  Era: {era} (slots {era_start}-{era_end})")
    print(f"  Epoch: {epoch}")
    print(f"  Fork: {fork}")

# Access network configurations
from era_parser.config.networks import NETWORK_CONFIGS
print("Available networks:", list(NETWORK_CONFIGS.keys()))
```

### 4. Field Size Reference 
```python
# Fixed-size items handled by parse_list_of_items in ssz_utils.py
from era_parser.parsing.ssz_utils import parse_list_of_items

FIELD_SIZES = {
    # Fixed-size fields (embedded inline)
    "sync_aggregate": 160,
    
    # Fixed-size items (parsed without offset tables)
    "withdrawal": 44,
    "deposit_request": 192,    # 48+32+8+96+8
    "withdrawal_request": 76,  # 20+48+8  
    "consolidation_request": 116, # 20+48+48
}

# The new parser automatically handles these in fork-specific parsers:
# - AltairParser handles sync_aggregate
# - CapellaParser adds withdrawal parsing
# - ElectraParser adds execution request parsing
```

## Testing with Your Era File

When you have an era file to test with, follow these steps:

### 1. Parse the Filename
Extract the era number from the filename to understand the slot range.

### 2. Calculate Valid Slots
Use the era number to find valid slots within that era's range.

### 3. Determine the Fork
Calculate the epoch from your target slot to determine which fork structure to expect.

### 4. Test the Parser
```bash
era-parser your-era-file.era block <target_slot>
```

### 5. Verify Fork Compatibility
Make sure your parser handles the correct fork for the era you're testing. Different eras may contain blocks from different forks depending on when fork transitions occurred.

## Conclusion

ERA files provide a robust archival format for Ethereum beacon chain data, but require careful handling of the evolving SSZ structure across forks. The key to successful parsing is understanding:

1. **Mixed field types**: Some fields are fixed-size (embedded), others variable-size (use offsets)
2. **Fork evolution**: Each fork adds new fields in specific positions
3. **SSZ semantics**: Proper offset calculation and list parsing strategies
4. **Era structure**: How slots map to eras and files

This understanding is crucial for implementing robust era file parsers that can handle the full spectrum of Ethereum beacon chain history.