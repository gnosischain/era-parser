# Era-Parser Validation Tests

Simple test suite that compares RPC beacon chain data against era-parser decoded data field-by-field.

## 🎯 Overview

This validation suite ensures your era-parser produces accurate beacon chain block data by comparing it against RPC responses (the "source of truth"). The tests perform deep field-by-field validation and provide detailed reporting of matches and differences.

## 📁 Structure

```
tests/
├── __init__.py               # Python package marker
├── conftest.py              # Pytest configuration  
├── data_models.py           # Simple data validation models
├── loaders.py               # JSON loading utilities
├── test_validation.py       # Main validation tests
├── test_data/               # Your test data files
│   ├── gnosis_10379290_rpc.json
│   ├── gnosis_10379290_era.json
│   ├── mainnet_12345678_rpc.json
│   └── mainnet_12345678_era.json
└── README.md               # This file
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Install the 3 required test dependencies
pip install -r requirements-test.txt
```

### 2. Add Test Data

Create JSON file pairs in `tests/test_data/`:

```bash
# Your test files should follow this naming pattern:
tests/test_data/
├── DESCRIPTION_SLOT_rpc.json    # RPC response data (source of truth)
└── DESCRIPTION_SLOT_era.json    # Era-parser decoded data
```

**Example:**
```bash
tests/test_data/
├── gnosis_10379290_rpc.json     # Gnosis chain slot 10379290 from RPC
├── gnosis_10379290_era.json     # Same slot from era-parser
├── mainnet_12345678_rpc.json    # Mainnet slot 12345678 from RPC
└── mainnet_12345678_era.json    # Same slot from era-parser
```

### 3. Run Tests

```bash
# Basic validation (shows pass/fail only)
python -m pytest tests/test_validation.py -v

# Detailed validation (shows all matched fields)
python -m pytest tests/test_validation.py -v -s

# Run all tests in tests directory
python -m pytest tests/ -v -s
```

## 📊 Test Output

### ✅ Successful Validation

When your era-parser data matches RPC data perfectly:

```
============================================================
🔍 VALIDATING: gnosis_10379290_rpc.json
============================================================
📊 Total fields to validate: 54
✅ ALL FIELDS MATCHED PERFECTLY!

📋 Successfully validated fields:
    1. message
    2. message.body
    3. message.body.attestations
    4. message.body.attestations[0].aggregation_bits
    5. message.body.attestations[0].data
    6. message.body.attestations[0].data.beacon_block_root
    ... (continues for all 54 fields)
   54. signature
============================================================
PASSED
```

### ❌ Failed Validation

When differences are found:

```
============================================================
🔍 VALIDATING: mainnet_12345678_rpc.json
============================================================
📊 Total fields to validate: 45
❌ DIFFERENCES FOUND:

🔄 Values that differ:
  • root['message']['body']['execution_payload']['gas_used']
    RPC:  15000000
    Era:  14999999

➖ Missing in era data:
  • root['message']['body']['new_field_from_rpc']

➕ Extra in era data:
  • root['metadata']['processing_time']
============================================================
FAILED
```

## 📝 Test Data Format

### RPC File Format (`*_rpc.json`)

This should be the exact JSON response from your beacon chain RPC endpoint:

```json
{
  "version": "capella",
  "execution_optimistic": false,
  "finalized": true,
  "data": {
    "message": {
      "slot": "10379290",
      "proposer_index": "123456",
      "parent_root": "0x...",
      "state_root": "0x...",
      "body": {
        "randao_reveal": "0x...",
        "eth1_data": {
          "deposit_root": "0x...",
          "deposit_count": "1000",
          "block_hash": "0x..."
        },
        "graffiti": "0x...",
        "proposer_slashings": [],
        "attester_slashings": [],
        "attestations": [...],
        "deposits": [],
        "voluntary_exits": [],
        "sync_aggregate": {
          "sync_committee_bits": "0x...",
          "sync_committee_signature": "0x..."
        },
        "execution_payload": {
          "parent_hash": "0x...",
          "fee_recipient": "0x...",
          "state_root": "0x...",
          "receipts_root": "0x...",
          "logs_bloom": "0x...",
          "prev_randao": "0x...",
          "block_number": "20000000",
          "gas_limit": "30000000",
          "gas_used": "15000000",
          "timestamp": "1700000000",
          "extra_data": "0x",
          "base_fee_per_gas": "10000000000",
          "block_hash": "0x...",
          "transactions": [],
          "withdrawals": [...]
        },
        "bls_to_execution_changes": []
      }
    },
    "signature": "0x..."
  }
}
```

### Era File Format (`*_era.json`)

This should be the JSON output from your era-parser for the same slot:

```json
{
  "data": {
    "message": {
      "slot": "10379290",
      "proposer_index": "123456",
      "parent_root": "0x...",
      "state_root": "0x...",
      "body": {
        // Same structure as RPC data.message.body
      }
    },
    "signature": "0x..."
  },
  "version": "capella",
  "execution_optimistic": false,
  "finalized": true,
  // Optional era-parser specific fields (ignored in comparison):
  "timestamp_utc": "2023-11-14T22:13:20+00:00",
  "metadata": {
    "compressed_size": 4096,
    "decompressed_size": 8192
  }
}
```

## 🧪 Available Tests

### 1. `test_block_validation()`

**Purpose:** Validates each RPC/era file pair for data consistency

**What it does:**
- Auto-discovers all `*_rpc.json` files
- Finds corresponding `*_era.json` files
- Compares the `data` section field-by-field
- Reports all matched fields and any differences
- Shows detailed diff information for failures

**Output:** Comprehensive field-by-field validation report

### 2. `test_all_files_paired()`

**Purpose:** Ensures every RPC file has a corresponding era file

**What it does:**
- Scans `test_data/` directory
- Checks that every `*_rpc.json` has a matching `*_era.json`
- Reports any orphaned files

**Output:** File pairing verification

## 🔧 Command Reference

```bash
# Run all tests with detailed field reporting
python -m pytest tests/test_validation.py -v -s

# Run just the block validation test
python -m pytest tests/test_validation.py::test_block_validation -v -s

# Run just the file pairing test
python -m pytest tests/test_validation.py::test_all_files_paired -v

# Run with even more pytest verbosity
python -m pytest tests/test_validation.py -vvv -s

# Run tests and stop on first failure
python -m pytest tests/test_validation.py -v -s -x

# Run a specific test file by pattern
python -m pytest tests/test_validation.py::test_block_validation[gnosis_10379290_rpc.json] -v -s
```

## 📈 Understanding Results

### Field Validation

The test validates **54 fields** in a typical Gnosis Capella block:

**Core Block Fields (4):**
- `message`, `message.slot`, `message.proposer_index`, `signature`

**Block Body Fields (8):**
- `message.body`, `randao_reveal`, `graffiti`, `eth1_data.*`

**Execution Payload Fields (16):**
- All execution layer fields like `gas_used`, `block_hash`, `transactions`, etc.

**Consensus Fields (26):**
- `attestations.*`, `sync_aggregate.*`, `withdrawals.*`, etc.

### Success Indicators

- ✅ **"ALL FIELDS MATCHED PERFECTLY!"** - Your era-parser is 100% accurate
- 📊 **Field count matches expectation** - All expected fields present
- 🔍 **All critical fields validated** - Core block data is correct

### Failure Analysis

When tests fail, look for:

1. **🔄 Values that differ** - Same field, different values
   - Usually indicates parsing logic issues
   - Check data type conversions (string vs number)

2. **➖ Missing in era data** - Fields in RPC but not era output  
   - Indicates incomplete era-parser field extraction
   - Check if new beacon chain fields need implementation

3. **➕ Extra in era data** - Fields in era but not RPC
   - Usually metadata fields (acceptable)
   - Or incorrect field additions

## 🚨 Troubleshooting

### No Tests Found
```bash
# Check test data directory
ls tests/test_data/

# Ensure files follow naming pattern
# ✅ correct: gnosis_10379290_rpc.json
# ❌ wrong:   gnosis_10379290.json
```

### Import Errors
```bash
# Use python -m pytest instead of just pytest
python -m pytest tests/test_validation.py -v -s

# Check dependencies are installed
pip list | grep -E "(pytest|deepdiff|pydantic)"
```

### Validation Failures
```bash
# Run with maximum verbosity to see exact differences
python -m pytest tests/test_validation.py -vvv -s

# Check your JSON files are valid
python -c "import json; print(json.load(open('tests/test_data/your_file.json')))"
```

## 🎯 Best Practices

### Test Data Collection

1. **Use real beacon chain data** - Don't create synthetic test data
2. **Test multiple forks** - Phase0, Altair, Bellatrix, Capella, Deneb, Electra
3. **Test edge cases** - Empty attestations, slashings, complex execution payloads
4. **Include different networks** - Mainnet, Gnosis, Sepolia

### File Organization

```bash
# Organize by network and fork for clarity
tests/test_data/
├── mainnet_capella_10380400_rpc.json
├── mainnet_capella_10380400_era.json
├── gnosis_deneb_15500000_rpc.json
├── gnosis_deneb_15500000_era.json
├── sepolia_altair_1234567_rpc.json
└── sepolia_altair_1234567_era.json
```

### Continuous Validation

Run tests regularly:
```bash
# Add to your development workflow
python -m pytest tests/test_validation.py -v -s

# Or create an alias
alias validate="python -m pytest tests/test_validation.py -v -s"
```

## 📚 Dependencies

**Required packages (3 total):**
- `pytest>=7.4.0` - Test framework
- `deepdiff>=6.7.0` - Deep dictionary comparison
- `pydantic>=2.0.0` - Data validation

**No other dependencies needed!** This is a minimalistic test suite focused purely on validation.

---

## 🎉 Success!

When you see **"ALL FIELDS MATCHED PERFECTLY!"** with all 54+ fields validated, you can be confident that your era-parser is producing accurate, complete beacon chain block data that matches the RPC source of truth exactly.