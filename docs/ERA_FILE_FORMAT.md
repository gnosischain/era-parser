# ERA Files: A Guide to Gnosis/Ethereum Beacon Chain Archive Format

## Table of Contents
1. [What are ERA Files?](#what-are-era-files)
2. [File Structure & SSZ Encoding](#file-structure--ssz-encoding)
3. [Fork Evolution](#fork-evolution)
4. [Era File Naming Convention](#era-file-naming-convention)
5. [Understanding Slot and Era Mapping](#understanding-slot-and-era-mapping)
6. [BeaconBlockBody Structure Evolution](#beaconblockbody-structure-evolution)
7. [Critical SSZ Parsing Concepts](#critical-ssz-parsing-concepts)
8. [Fixed-Size vs Variable-Size Data Structures](#fixed-size-vs-variable-size-data-structures)
9. [Parsing Implementation Guide](#parsing-implementation-guide)
10. [Tools & Utilities](#tools--utilities)

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
    if hasattr(item_parser_func, 'ssz_size'):
        item_size = item_parser_func.ssz_size
        return [item_parser_func(data[i*item_size:(i+1)*item_size]) 
                for i in range(len(data) // item_size)]
    
    # Variable-size items (attestations)
    first_offset = read_uint32_at(data, 0)
    num_items = first_offset // 4
    offsets = [read_uint32_at(data, i*4) for i in range(num_items)]
    # ... parse using offsets
```

## Fixed-Size vs Variable-Size Data Structures

### The Critical Distinction

One of the most important aspects of SSZ parsing is correctly identifying whether a data structure uses **fixed-size** or **variable-size** encoding. Getting this wrong leads to parsing errors like:

```
Invalid offset table for parse_deposit, first_offset=1707068757, trying fallback parsing
```

### Fixed-Size Data Structures

These structures have a **known, constant byte size** and are parsed **directly** without offset tables:

#### Deposits (1240 bytes)
```python
# Structure: 33 × 32-byte proof hashes + 184 bytes deposit data
proof: Vector[Bytes32, 33]       # 1056 bytes (33 × 32)
data: DepositData               # 184 bytes
  - pubkey: BLSPubkey           # 48 bytes
  - withdrawal_credentials: Bytes32  # 32 bytes  
  - amount: uint64              # 8 bytes
  - signature: BLSSignature     # 96 bytes
# Total: 1056 + 184 = 1240 bytes
```

#### Withdrawals (44 bytes)
```python
# Structure: Fixed withdrawal data
index: uint64                   # 8 bytes
validator_index: uint64         # 8 bytes
address: ExecutionAddress       # 20 bytes
amount: uint64                  # 8 bytes
# Total: 44 bytes
```

#### Voluntary Exits (112 bytes)
```python
# Structure: Exit message + signature
message:                        # 16 bytes
  - epoch: uint64               # 8 bytes
  - validator_index: uint64     # 8 bytes
signature: BLSSignature         # 96 bytes
# Total: 16 + 96 = 112 bytes
```

#### KZG Commitments (48 bytes)
```python
# Structure: Single BLS12-381 G1 point
commitment: BLSCommitment       # 48 bytes
```

#### Execution Requests (Fixed per type)
```python
# Deposit Request: 192 bytes
pubkey: BLSPubkey               # 48 bytes
withdrawal_credentials: Bytes32  # 32 bytes
amount: uint64                  # 8 bytes
signature: BLSSignature         # 96 bytes
index: uint64                   # 8 bytes
# Total: 192 bytes

# Withdrawal Request: 76 bytes
source_address: ExecutionAddress # 20 bytes
validator_pubkey: BLSPubkey     # 48 bytes
amount: uint64                  # 8 bytes
# Total: 76 bytes

# Consolidation Request: 116 bytes
source_address: ExecutionAddress # 20 bytes
source_pubkey: BLSPubkey        # 48 bytes
target_pubkey: BLSPubkey        # 48 bytes
# Total: 116 bytes
```

### Variable-Size Data Structures

These structures use **SSZ offset tables** to indicate where each item starts and ends:

#### Attestations
```python
# Structure: Variable-length with offset table
attesting_indices: List[ValidatorIndex, MAX_VALIDATORS] # Variable!
data: AttestationData           # Fixed 128 bytes
signature: BLSSignature         # 96 bytes
```

#### Proposer/Attester Slashings
```python
# Structure: Multiple variable-length components
signed_header_1: SignedBeaconBlockHeader    # Variable
signed_header_2: SignedBeaconBlockHeader    # Variable
# OR
attestation_1: IndexedAttestation           # Variable  
attestation_2: IndexedAttestation           # Variable
```

## Parsing Implementation Guide

### 1. Era File Navigation 
```python
# Using EraReader class
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
```

### 2. Block Extraction with Unified Processing
```python
# Using EraProcessor with unified architecture
from era_parser.core import EraProcessor

def extract_block(era_file: str, target_slot: int, network: str = None):
    # Use EraProcessor for unified processing
    processor = EraProcessor()
    processor.setup(era_file, network)
    
    # Parse single block
    return processor.parse_single_block(target_slot)

def process_era_file(era_file: str, command: str, output_file: str, export_type: str = "file"):
    # Unified processing approach
    processor = EraProcessor()
    processor.setup(era_file)
    
    # Process with command
    success = processor.process_single_era(command, output_file, separate_files=False, export_type=export_type)
    return success
```

### 3. Fork-Aware Field Parsing with Schema Declaration
```python
# Using declarative schema approach
from era_parser.parsing.forks import get_fork_parser
from era_parser.parsing.block_parser import BlockParser

def parse_block_with_schema(compressed_data: bytes, slot: int, network: str):
    # Main block parser coordinates the process
    block_parser = BlockParser(network)
    return block_parser.parse_block(compressed_data, slot)

# Fork parsers now use declarative schema:
class AltairParser(Phase0Parser):
    BODY_SCHEMA = [
        ('fixed', 'sync_aggregate', 160),  # Fixed 160-byte sync aggregate
    ]

class BellatrixParser(AltairParser):
    BODY_SCHEMA = AltairParser.BODY_SCHEMA + [
        ('variable', 'execution_payload', 'parse_execution_payload_bellatrix'),
    ]
```

### 4. Enhanced Attester Slashing Parsing with Self-Describing Items

```python
def parse_attester_slashing(self, data: bytes) -> Optional[Dict[str, Any]]:
    """Parse attester slashing with full validator indices support"""
    try:
        if len(data) < 8:
            return None
        
        # Read offsets for two IndexedAttestations
        attestation_1_offset = read_uint32_at(data, 0)
        attestation_2_offset = read_uint32_at(data, 4)
        
        # Parse both attestations
        attestation_1_data = data[attestation_1_offset:attestation_2_offset]
        attestation_2_data = data[attestation_2_offset:]
        
        attestation_1 = self.parse_indexed_attestation(attestation_1_data)
        attestation_2 = self.parse_indexed_attestation(attestation_2_data)
        
        if not attestation_1 or not attestation_2:
            return None
        
        return {
            "attestation_1": attestation_1,  # Includes attesting_indices array
            "attestation_2": attestation_2   # Includes attesting_indices array
        }
        
    except Exception as e:
        return None

# Self-describing item parsers with ssz_size attribute
def parse_deposit(self, data: bytes) -> Optional[Dict[str, Any]]:
    """Parse a single deposit"""
    try:
        if len(data) < 1240:
            return None
        
        # Parse proof and deposit data
        # ... implementation
        
    except Exception as e:
        return None

# Set SSZ size for fixed-size items
parse_deposit.ssz_size = 1240
```

## Tools & Utilities

### 1. Era File Analysis
```bash
# List era file contents
era-parser stats example-02612-24eac76f.era

# Extract specific block  
era-parser example-02612-24eac76f.era block 21385280

# Check for parsing issues
era-parser example-02612-24eac76f.era all-blocks test.json --separate
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
# Using modular config system
from era_parser.config import get_fork_by_slot, get_network_config
from era_parser.core.era_slot_calculator import EraSlotCalculator

def era_info(slot: int, network: str = 'mainnet'):
    # Get era range
    era_number = EraSlotCalculator.get_era_from_slot(network, slot)
    start_slot, end_slot = EraSlotCalculator.get_era_slot_range(network, era_number)
    
    # Get network and fork info
    config = get_network_config(network)
    epoch = slot // config['SLOTS_PER_EPOCH']
    fork = get_fork_by_slot(slot, network)
    
    print(f"Slot {slot} ({network}):")
    print(f"  Era: {era_number} (slots {start_slot}-{end_slot})")
    print(f"  Epoch: {epoch}")
    print(f"  Fork: {fork}")
```

### 4. Fixed-Size Field Reference 
```python
# Self-describing parsers in parsing/forks/base.py
FIELD_SIZES = {
    # Fixed-size fields (embedded inline)
    "sync_aggregate": 160,  # 64 + 96 bytes
    
    # Fixed-size items (parsed without offset tables via ssz_size attribute)
    "withdrawal": 44,           # 8+8+20+8 bytes
    "deposit": 1240,            # 1056+184 bytes
    "voluntary_exit": 112,      # 8+8+96 bytes  
    "kzg_commitment": 48,       # 48 bytes
    "deposit_request": 192,     # 48+32+8+96+8 bytes
    "withdrawal_request": 76,   # 20+48+8 bytes
    "consolidation_request": 116, # 20+48+48 bytes
}

# Parsers automatically handle these via ssz_size attribute:
parse_deposit.ssz_size = 1240
parse_withdrawal.ssz_size = 44
parse_voluntary_exit.ssz_size = 112
```

### 5. Parsing Validation Tools

```python
# Test self-describing parsing
def validate_self_describing_parsing():
    """Validate that self-describing structures parse correctly"""
    
    # Test deposit parsing (1240 bytes)
    test_deposit_data = b'\x00' * 1240  # Mock 1240-byte deposit
    deposits = parse_list_of_items(test_deposit_data, parse_deposit)
    assert len(deposits) == 1, f"Expected 1 deposit, got {len(deposits)}"
    
    # Test multiple deposits (2480 bytes = 2 deposits)  
    test_multi_deposits = b'\x00' * 2480
    deposits = parse_list_of_items(test_multi_deposits, parse_deposit)
    assert len(deposits) == 2, f"Expected 2 deposits, got {len(deposits)}"
    
    # Test withdrawal parsing (44 bytes)
    test_withdrawal_data = b'\x00' * 44
    withdrawals = parse_list_of_items(test_withdrawal_data, parse_withdrawal)
    assert len(withdrawals) == 1, f"Expected 1 withdrawal, got {len(withdrawals)}"
    
    print("✅ All self-describing parsing tests passed!")

# Test variable-size parsing
def validate_variable_size_parsing():
    """Validate that variable-size structures still use offset tables"""
    
    # Attestations should NOT have ssz_size attribute
    assert not hasattr(parse_attestation, 'ssz_size')
    
    # Attester slashings should NOT have ssz_size attribute
    assert not hasattr(parse_attester_slashing, 'ssz_size')
    
    print("✅ Variable-size parsing validation passed!")
```

## Testing with Your Era File

When you have an era file to test with, follow these steps:

### 1. Parse the Filename
Extract the era number from the filename to understand the slot range.

### 2. Calculate Valid Slots
Use the era number to find valid slots within that era's range.

### 3. Determine the Fork
Calculate the epoch from your target slot to determine which fork structure to expect.

### 4. Test the Parser with Unified Processing
```bash
# Test basic parsing (using unified processor)
era-parser your-era-file.era block <target_slot>

# Test full data extraction (using declarative schemas)
era-parser your-era-file.era all-blocks test_output.json --separate

# Test ClickHouse export (with unified state management)
era-parser your-era-file.era all-blocks --export clickhouse
```

### 5. Verify Enhanced Data Extraction

Check that the enhanced attester slashing data is properly extracted:

```python
import json

# Load exported data
with open('test_output_attester_slashings.json', 'r') as f:
    data = json.load(f)

# Verify enhanced fields are present
for slashing in data.get('data', []):
    assert 'att_1_attesting_indices' in slashing
    assert 'att_2_attesting_indices' in slashing  
    assert 'att_1_validator_count' in slashing
    assert 'att_2_validator_count' in slashing
    assert 'total_slashed_validators' in slashing
    
    # Verify indices are JSON arrays
    att_1_indices = json.loads(slashing['att_1_attesting_indices'])
    att_2_indices = json.loads(slashing['att_2_attesting_indices'])
    
    # Verify counts match array lengths
    assert len(att_1_indices) == slashing['att_1_validator_count']
    assert len(att_2_indices) == slashing['att_2_validator_count']
    
    print(f"✅ Slashing at slot {slashing['slot']}: {slashing['total_slashed_validators']} validators")
```

### 6. Verify Fork Compatibility
Make sure your parser handles the correct fork for the era you're testing. Different eras may contain blocks from different forks depending on when fork transitions occurred.

### 7. Debug Any Remaining Issues

If you still see parsing errors:

```python
# Check if new data types need to be added to self-describing parsers
def debug_unknown_structure(data: bytes, parser_name: str):
    """Debug unknown data structure to determine if it's fixed-size"""
    
    print(f"Debugging {parser_name}:")
    print(f"  Data length: {len(data)} bytes")
    
    # Check if length is divisible by common fixed sizes
    common_sizes = [32, 44, 48, 76, 96, 112, 116, 192, 1240]
    for size in common_sizes:
        if len(data) % size == 0:
            items = len(data) // size
            print(f"  Could be {items} items of {size} bytes each")
    
    # Try reading as offset table
    if len(data) >= 4:
        first_offset = read_uint32_at(data, 0)
        print(f"  First offset: {first_offset}")
        if first_offset < len(data) and first_offset % 4 == 0:
            num_items = first_offset // 4
            print(f"  Potential items from offset table: {num_items}")
        else:
            print(f"  Invalid offset table - likely fixed-size structure!")
```

## Conclusion

ERA files provide a robust archival format for Ethereum beacon chain data, but require careful handling of the evolving SSZ structure across forks. The key to successful parsing with the new refactored system is understanding:

1. **Unified Processing**: Single `EraProcessor` coordinates all operations
2. **Declarative Schema**: Fork parsers use schema definitions for consistency
3. **Self-Describing Items**: Parser functions have `ssz_size` attributes for fixed-size structures
4. **Unified State Management**: `EraStateManager` handles all completion tracking
5. **Single Timestamp**: All records use unified timestamp for efficient partitioning
6. **Enhanced Data Extraction**: Complete validator information in slashing events

The **self-describing parser system** with `ssz_size` attributes eliminates offset table errors with structures like deposits, withdrawals, and voluntary exits. The **unified processing architecture** provides consistent handling across all networks and forks, while the **declarative schema approach** makes fork parsers easier to maintain and extend.