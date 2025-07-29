# Parsed Fields Reference

This document provides comprehensive documentation for all fields parsed from beacon chain era files, including their data types, parsing logic, and usage in different export formats.

## Table of Contents

- [Overview](#overview)
- [Core Tables](#core-tables)
  - [blocks](#blocks-table)
  - [sync_aggregates](#sync_aggregates-table)
  - [execution_payloads](#execution_payloads-table)
  - [transactions](#transactions-table)
  - [withdrawals](#withdrawals-table)
  - [attestations](#attestations-table)
  - [deposits](#deposits-table)
  - [voluntary_exits](#voluntary_exits-table)
  - [proposer_slashings](#proposer_slashings-table)
  - [attester_slashings](#attester_slashings-table)
  - [bls_changes](#bls_changes-table)
  - [blob_commitments](#blob_commitments-table)
  - [execution_requests](#execution_requests-table)
- [State Management](#state-management)

## Overview

Era Parser extracts data from beacon chain era files using fork-specific parsers with declarative schemas and exports to multiple formats. All tables use a **single timestamp approach** with `timestamp_utc` for efficient time-based partitioning and unified state management.

### Common Parsing Principles

1. **Single Timestamp**: All records use `timestamp_utc` calculated from slot timing or execution payload
2. **Unified Processing**: `EraProcessor` coordinates all parsing operations
3. **Declarative Schema**: Fork parsers use schema definitions for consistent parsing
4. **Self-Describing Items**: Parser functions have `ssz_size` attributes for fixed-size structures
5. **Graceful Degradation**: Missing or malformed data results in default values, not parsing failures
6. **Consistent Types**: All numeric values are converted to appropriate types (strings for large numbers)
7. **Hex Encoding**: All hash values and binary data are hex-encoded with `0x` prefix
8. **Unified State Management**: `EraStateManager` handles all completion tracking

## Core Tables

## blocks Table

**Purpose**: Beacon chain block headers and metadata (no execution layer data)

**Parsing Logic**: Extracts from the beacon block message structure using unified `EraProcessor`, excluding execution payload and sync aggregate data which are stored in separate tables.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | `message.slot` | Block slot number | Primary identifier, extracted from block header |
| `proposer_index` | UInt64 | `message.proposer_index` | Validator index that proposed this block | Converted from string to uint64 |
| `parent_root` | String | `message.parent_root` | Hash of the parent block | 32-byte hash, hex-encoded with 0x prefix |
| `state_root` | String | `message.state_root` | Root hash of the beacon state | 32-byte hash, hex-encoded with 0x prefix |
| `signature` | String | `data.signature` | Block signature from proposer | 96-byte BLS signature, hex-encoded |
| `version` | String | Fork detection | Fork version (phase0, altair, bellatrix, etc.) | Determined by slot number and network config |
| `timestamp_utc` | DateTime | Calculated | Block timestamp in UTC | **Single timestamp** calculated from genesis time + (slot × seconds_per_slot) or execution payload timestamp |
| `randao_reveal` | String | `body.randao_reveal` | RANDAO reveal value | 96-byte BLS signature for randomness |
| `graffiti` | String | `body.graffiti` | 32-byte graffiti field | Arbitrary data field, hex-encoded |
| `eth1_deposit_root` | String | `body.eth1_data.deposit_root` | ETH1 deposit tree root | From ETH1 data structure |
| `eth1_deposit_count` | UInt64 | `body.eth1_data.deposit_count` | Number of deposits in ETH1 | Running count of all deposits |
| `eth1_block_hash` | String | `body.eth1_data.block_hash` | ETH1 block hash reference | Links to execution layer block |

**ClickHouse Schema**:
```sql
CREATE TABLE blocks (
    slot UInt64,
    proposer_index UInt64 DEFAULT 0,
    parent_root String DEFAULT '',
    state_root String DEFAULT '',
    signature String DEFAULT '',
    version String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    randao_reveal String DEFAULT '',
    graffiti String DEFAULT '',
    eth1_deposit_root String DEFAULT '',
    eth1_deposit_count UInt64 DEFAULT 0,
    eth1_block_hash String DEFAULT '',
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, proposer_index)
```

---

## sync_aggregates Table

**Purpose**: Sync committee participation data (Altair+ forks)

**Parsing Logic**: Extracts sync committee bits and signature from the sync_aggregate field using declarative schema parsing. This is a fixed 160-byte structure embedded directly in the block body.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Block slot containing this sync aggregate | Links to blocks table |
| `sync_committee_bits` | String | `sync_aggregate.sync_committee_bits` | Bitfield of participating validators | 64-byte bitfield, hex-encoded |
| `sync_committee_signature` | String | `sync_aggregate.sync_committee_signature` | BLS aggregate signature | 96-byte signature, hex-encoded |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - same as block timestamp | Inherited from containing block |
| `participating_validators` | UInt32 | Calculated | Number of participating validators | Count of set bits in sync_committee_bits |

**Declarative Schema Parsing**:
```python
class AltairParser(Phase0Parser):
    BODY_SCHEMA = [
        ('fixed', 'sync_aggregate', 160),  # Fixed 160-byte sync aggregate
    ]

def parse_sync_aggregate(self, data: bytes) -> Dict[str, Any]:
    """Parse sync_aggregate - fixed 160-byte structure"""
    if len(data) < 160: 
        return {}
    return {
        "sync_committee_bits": "0x" + data[0:64].hex(), 
        "sync_committee_signature": "0x" + data[64:160].hex()
    }
```

---

## execution_payloads Table

**Purpose**: Execution layer block data (Bellatrix+ forks, post-merge)

**Parsing Logic**: Extracts execution payload data using fork-specific schema parsers. Structure varies by fork (Deneb adds blob gas fields).

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot number | Links to blocks table |
| `parent_hash` | String | `execution_payload.parent_hash` | Parent execution block hash | 32-byte hash |
| `fee_recipient` | String | `execution_payload.fee_recipient` | Address receiving block rewards | 20-byte address |
| `state_root` | String | `execution_payload.state_root` | Execution state root | 32-byte hash |
| `receipts_root` | String | `execution_payload.receipts_root` | Transaction receipts root | 32-byte Merkle root |
| `logs_bloom` | String | `execution_payload.logs_bloom` | Bloom filter for logs | 256-byte bloom filter |
| `prev_randao` | String | `execution_payload.prev_randao` | Previous RANDAO value | 32-byte randomness |
| `block_number` | UInt64 | `execution_payload.block_number` | Execution block number | Sequential block number |
| `gas_limit` | UInt64 | `execution_payload.gas_limit` | Block gas limit | Maximum gas for block |
| `gas_used` | UInt64 | `execution_payload.gas_used` | Gas used by transactions | Total gas consumed |
| `timestamp_utc` | DateTime | `execution_payload.timestamp` | **Single timestamp** - block timestamp | Primary timestamp, converted from Unix timestamp |
| `base_fee_per_gas` | String | `execution_payload.base_fee_per_gas` | EIP-1559 base fee | Wei amount as string (large number) |
| `block_hash` | String | `execution_payload.block_hash` | Execution block hash | 32-byte block identifier |
| `blob_gas_used` | UInt64 | `execution_payload.blob_gas_used` | Gas used by blobs (Deneb+) | EIP-4844 blob gas consumption |
| `excess_blob_gas` | UInt64 | `execution_payload.excess_blob_gas` | Excess blob gas (Deneb+) | For blob fee calculation |
| `extra_data` | String | `execution_payload.extra_data` | Extra data field | Arbitrary data, hex-encoded |
| `transactions_count` | UInt64 | Calculated | Number of transactions | Length of transactions array |
| `withdrawals_count` | UInt64 | Calculated | Number of withdrawals (Capella+) | Length of withdrawals array |

**Fork-Specific Schema Parsing**:
```python
class BellatrixParser(AltairParser):
    BODY_SCHEMA = AltairParser.BODY_SCHEMA + [
        ('variable', 'execution_payload', 'parse_execution_payload_bellatrix'),
    ]

class DenebParser(CapellaParser):
    # Automatically handles blob_gas_used and excess_blob_gas fields
    def parse_execution_payload_base(self, data: bytes, fork: str = "deneb"):
        # ... adds blob gas fields for Deneb+
```

---

## transactions Table

**Purpose**: Individual transaction records from execution payloads

**Parsing Logic**: Extracts transaction hashes from the execution payload's transactions array using unified processing. Each transaction is currently stored as a hash only.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `block_number` | UInt64 | `execution_payload.block_number` | Execution block number | From containing execution payload |
| `block_hash` | String | `execution_payload.block_hash` | Execution block hash | From containing execution payload |
| `transaction_index` | UInt64 | Array index | Index within block | 0-based position in transactions array |
| `transaction_hash` | String | `transactions[i]` | Transaction hash | RLP hash of transaction |
| `fee_recipient` | String | `execution_payload.fee_recipient` | Block fee recipient | Address receiving fees |
| `gas_limit` | UInt64 | `execution_payload.gas_limit` | Block gas limit | From containing block |
| `gas_used` | UInt64 | `execution_payload.gas_used` | Block gas used | Total for all transactions |
| `base_fee_per_gas` | String | `execution_payload.base_fee_per_gas` | EIP-1559 base fee | Wei amount |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - transaction timestamp | Same as containing block |

**Unified Processing Extraction**:
```python
# In EraProcessor.extract_all_data()
transactions = execution_payload.get("transactions", [])
for tx_index, tx_hash in enumerate(transactions):
    all_data['transactions'].append({
        "slot": slot,
        "block_number": execution_payload.get("block_number"),
        "block_hash": execution_payload.get("block_hash"),
        "transaction_index": tx_index,
        "transaction_hash": tx_hash,
        "timestamp_utc": timestamp_utc,  # SINGLE timestamp
        # ... other fields
    })
```

---

## withdrawals Table

**Purpose**: Validator withdrawal records (Capella+ forks)

**Parsing Logic**: Parses withdrawal objects from execution payload's withdrawals array using self-describing parser. Each withdrawal is a fixed 44-byte structure.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `block_number` | UInt64 | `execution_payload.block_number` | Execution block number | From containing execution payload |
| `block_hash` | String | `execution_payload.block_hash` | Execution block hash | From containing execution payload |
| `withdrawal_index` | UInt64 | `withdrawal.index` | Global withdrawal index | Monotonically increasing |
| `validator_index` | UInt64 | `withdrawal.validator_index` | Validator being withdrawn | Beacon chain validator index |
| `address` | String | `withdrawal.address` | Withdrawal destination | 20-byte execution address |
| `amount` | UInt64 | `withdrawal.amount` | Withdrawal amount | Wei amount (Gwei × 10^9) |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - withdrawal timestamp | Same as containing block |

**Self-Describing Parser**:
```python
def parse_withdrawal(self, data: bytes) -> Optional[Dict[str, Any]]:
    """Parse execution payload withdrawal - 44 bytes fixed size"""
    if len(data) < 44:
        return None
    return {
        "index": str(read_uint64_at(data, 0)), 
        "validator_index": str(read_uint64_at(data, 8)),
        "address": "0x" + data[16:36].hex(), 
        "amount": str(read_uint64_at(data, 36))
    }

# Set SSZ size for self-describing parsing
parse_withdrawal.ssz_size = 44
```

---

## attestations Table

**Purpose**: Validator attestation data showing consensus votes

**Parsing Logic**: Parses attestation objects from block body using variable-size SSZ parsing. Each attestation contains vote data and aggregation information.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot containing attestation | Links to blocks table |
| `attestation_index` | UInt64 | Array index | Index within block's attestations | 0-based position |
| `aggregation_bits` | String | `attestation.aggregation_bits` | Validator participation bitfield | Variable-length bitfield |
| `signature` | String | `attestation.signature` | BLS aggregate signature | 96-byte signature |
| `attestation_slot` | UInt64 | `attestation.data.slot` | Slot being attested to | May differ from containing block slot |
| `committee_index` | UInt64 | `attestation.data.index` | Committee index | Which committee made this attestation |
| `beacon_block_root` | String | `attestation.data.beacon_block_root` | Block root being attested | 32-byte hash |
| `source_epoch` | UInt64 | `attestation.data.source.epoch` | Source checkpoint epoch | Justification source |
| `source_root` | String | `attestation.data.source.root` | Source checkpoint root | 32-byte hash |
| `target_epoch` | UInt64 | `attestation.data.target.epoch` | Target checkpoint epoch | Justification target |
| `target_root` | String | `attestation.data.target.root` | Target checkpoint root | 32-byte hash |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - attestation inclusion timestamp | Same as containing block |

**Note**: Attestations use variable-size SSZ parsing (no `ssz_size` attribute) because they contain variable-length attesting_indices arrays.

---

## deposits Table

**Purpose**: Validator deposit data for new validator registrations

**Parsing Logic**: Parses deposit objects with proof data and deposit information using self-describing parser. Deposits are 1240 bytes: 1056 bytes of proof + 184 bytes of deposit data. **Self-describing parsing** prevents SSZ offset table errors.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `deposit_index` | UInt64 | Array index | Index within block | 0-based position |
| `pubkey` | String | `deposit.data.pubkey` | Validator public key | 48-byte BLS public key |
| `withdrawal_credentials` | String | `deposit.data.withdrawal_credentials` | Withdrawal credentials | 32-byte commitment |
| `amount` | UInt64 | `deposit.data.amount` | Deposit amount | Gwei amount (32 ETH = 32000000000) |
| `signature` | String | `deposit.data.signature` | Deposit signature | 96-byte BLS signature |
| `proof` | String | `deposit.proof` | Merkle proof | JSON array of 33 32-byte hashes |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - deposit inclusion timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
def parse_deposit(self, data: bytes) -> Optional[Dict[str, Any]]:
    """Parse deposit with 33-element proof and deposit data"""
    if len(data) < 1240:  # 33*32 + 184 bytes
        return None
    
    # Parse 33 proof elements (32 bytes each)
    proof = []
    for i in range(33):
        proof.append("0x" + data[i*32:(i+1)*32].hex())
    
    # Parse deposit data (remaining 184 bytes)
    deposit_data = data[1056:]  # Skip proof
    return {
        "proof": proof,
        "data": {
            "pubkey": "0x" + deposit_data[0:48].hex(),
            "withdrawal_credentials": "0x" + deposit_data[48:80].hex(),
            "amount": str(read_uint64_at(deposit_data, 80)),
            "signature": "0x" + deposit_data[88:184].hex()
        }
    }

# Set SSZ size for self-describing parsing
parse_deposit.ssz_size = 1240  # 33*32 + 184 bytes (proof + deposit data)
```

---

## voluntary_exits Table

**Purpose**: Voluntary validator exit requests

**Parsing Logic**: Parses voluntary exit messages using self-describing parser showing validators requesting to exit the network. **Self-describing parsing** (112 bytes) prevents offset errors.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `exit_index` | UInt64 | Array index | Index within block | 0-based position |
| `signature` | String | `voluntary_exit.signature` | Exit signature | 96-byte BLS signature |
| `epoch` | UInt64 | `voluntary_exit.message.epoch` | Exit epoch | When exit takes effect |
| `validator_index` | UInt64 | `voluntary_exit.message.validator_index` | Exiting validator | Validator index |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - exit request timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
# Set SSZ size for self-describing parsing
parse_voluntary_exit.ssz_size = 112  # 8 + 8 + 96 bytes (epoch + validator_index + signature)
```

---

## proposer_slashings Table

**Purpose**: Evidence of proposer violations (double block proposals)

**Parsing Logic**: Parses proposer slashing evidence containing two conflicting block headers from the same proposer using unified processing.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `slashing_index` | UInt64 | Array index | Index within block | 0-based position |
| `header_1_slot` | UInt64 | `signed_header_1.message.slot` | First block slot | From first conflicting header |
| `header_1_proposer_index` | UInt64 | `signed_header_1.message.proposer_index` | Proposer index | Should match header_2 |
| `header_1_parent_root` | String | `signed_header_1.message.parent_root` | First parent root | 32-byte hash |
| `header_1_state_root` | String | `signed_header_1.message.state_root` | First state root | 32-byte hash |
| `header_1_body_root` | String | `signed_header_1.message.body_root` | First body root | 32-byte hash |
| `header_1_signature` | String | `signed_header_1.signature` | First signature | 96-byte BLS signature |
| `header_2_slot` | UInt64 | `signed_header_2.message.slot` | Second block slot | From second conflicting header |
| `header_2_proposer_index` | UInt64 | `signed_header_2.message.proposer_index` | Proposer index | Should match header_1 |
| `header_2_parent_root` | String | `signed_header_2.message.parent_root` | Second parent root | 32-byte hash |
| `header_2_state_root` | String | `signed_header_2.message.state_root` | Second state root | 32-byte hash |
| `header_2_body_root` | String | `signed_header_2.message.body_root` | Second body root | 32-byte hash |
| `header_2_signature` | String | `signed_header_2.signature` | Second signature | 96-byte BLS signature |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - slashing evidence timestamp | Same as containing block |

---

## attester_slashings Table

**Purpose**: Evidence of attester violations (conflicting attestations) with comprehensive validator tracking

**Parsing Logic**: Parses attester slashing evidence containing two conflicting attestations using unified processing. Enhanced to capture all attesting validator indices and provide comprehensive analysis capabilities.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `slashing_index` | UInt64 | Array index | Index within block | 0-based position |
| **Attestation 1 Data** |
| `att_1_slot` | UInt64 | `attestation_1.data.slot` | First attestation slot | From first attestation |
| `att_1_committee_index` | UInt64 | `attestation_1.data.index` | First committee index | Committee that made attestation |
| `att_1_beacon_block_root` | String | `attestation_1.data.beacon_block_root` | First block root | 32-byte hash |
| `att_1_source_epoch` | UInt64 | `attestation_1.data.source.epoch` | First source epoch | Source checkpoint |
| `att_1_source_root` | String | `attestation_1.data.source.root` | First source root | 32-byte hash |
| `att_1_target_epoch` | UInt64 | `attestation_1.data.target.epoch` | First target epoch | Target checkpoint |
| `att_1_target_root` | String | `attestation_1.data.target.root` | First target root | 32-byte hash |
| `att_1_signature` | String | `attestation_1.signature` | First signature | 96-byte aggregate signature |
| `att_1_attesting_indices` | String | `attestation_1.attesting_indices` | **First validator indices** | **JSON array of validator indices** |
| `att_1_validator_count` | UInt32 | Calculated | **First validator count** | **Length of attesting_indices array** |
| **Attestation 2 Data** |
| `att_2_slot` | UInt64 | `attestation_2.data.slot` | Second attestation slot | From second attestation |
| `att_2_committee_index` | UInt64 | `attestation_2.data.index` | Second committee index | Committee that made attestation |
| `att_2_beacon_block_root` | String | `attestation_2.data.beacon_block_root` | Second block root | 32-byte hash |
| `att_2_source_epoch` | UInt64 | `attestation_2.data.source.epoch` | Second source epoch | Source checkpoint |
| `att_2_source_root` | String | `attestation_2.data.source.root` | Second source root | 32-byte hash |
| `att_2_target_epoch` | UInt64 | `attestation_2.data.target.epoch` | Second target epoch | Target checkpoint |
| `att_2_target_root` | String | `attestation_2.data.target.root` | Second target root | 32-byte hash |
| `att_2_signature` | String | `attestation_2.signature` | Second signature | 96-byte aggregate signature |
| `att_2_attesting_indices` | String | `attestation_2.attesting_indices` | **Second validator indices** | **JSON array of validator indices** |
| `att_2_validator_count` | UInt32 | Calculated | **Second validator count** | **Length of attesting_indices array** |
| **Metadata** |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - slashing evidence timestamp | Same as containing block |
| `total_slashed_validators` | UInt32 | Calculated | **Total unique validators** | **Count of unique validators across both attestations** |

**Enhanced Unified Processing**:
```python
# In EraProcessor.extract_all_data() - enhanced attester slashing extraction
for slash_idx, slashing in enumerate(attester_slashings):
    attestation_1 = slashing.get("attestation_1", {})
    attestation_2 = slashing.get("attestation_2", {})
    
    # Get attesting indices arrays
    att_1_indices = attestation_1.get("attesting_indices", [])
    att_2_indices = attestation_2.get("attesting_indices", [])
    
    # Calculate total unique slashed validators
    all_indices = set(att_1_indices + att_2_indices)
    
    all_data['attester_slashings'].append({
        "slot": slot,
        "slashing_index": slash_idx,
        # ... attestation data fields ...
        "att_1_attesting_indices": json.dumps(att_1_indices),  # Store as JSON
        "att_1_validator_count": len(att_1_indices),
        "att_2_attesting_indices": json.dumps(att_2_indices),  # Store as JSON
        "att_2_validator_count": len(att_2_indices),
        "timestamp_utc": timestamp_utc,  # SINGLE timestamp
        "total_slashed_validators": len(all_indices),
    })
```

---

## bls_changes Table

**Purpose**: BLS to execution address changes (Capella+ forks)

**Parsing Logic**: Parses BLS to execution changes using self-describing parser allowing validators to update their withdrawal credentials. **Self-describing parsing** (172 bytes) prevents offset table errors.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `change_index` | UInt64 | Array index | Index within block | 0-based position |
| `signature` | String | `bls_change.signature` | Change signature | 96-byte BLS signature |
| `validator_index` | UInt64 | `bls_change.message.validator_index` | Validator index | Validator making change |
| `from_bls_pubkey` | String | `bls_change.message.from_bls_pubkey` | Old BLS public key | 48-byte public key |
| `to_execution_address` | String | `bls_change.message.to_execution_address` | New execution address | 20-byte address |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - change timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
# Set SSZ size for self-describing parsing
parse_bls_to_execution_change.ssz_size = 172  # 8+48+20+96 bytes (validator_index + pubkey + address + signature)
```

---

## blob_commitments Table

**Purpose**: Blob KZG commitments (Deneb+ forks, EIP-4844)

**Parsing Logic**: Extracts KZG commitments for blob transactions from the blob_kzg_commitments array using self-describing parser. **Self-describing parsing** (48 bytes each).

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `commitment_index` | UInt64 | Array index | Index within block | 0-based position in commitments array |
| `commitment` | String | `blob_kzg_commitments[i]` | KZG commitment | 48-byte commitment to blob data |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - commitment timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
# Set SSZ size for self-describing parsing
parse_kzg_commitment.ssz_size = 48  # 48-byte KZG commitment
```

---

## execution_requests Table

**Purpose**: Execution layer requests (Electra+ forks)

**Parsing Logic**: Parses three types of execution requests using self-describing parsers: deposits, withdrawals, and consolidations. Uses a unified table with request_type discrimination.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `request_type` | String | Request type | Type of request | "deposit", "withdrawal", or "consolidation" |
| `request_index` | UInt64 | Array index | Index within type | 0-based position within request type |
| `pubkey` | String | Request data | Public key (deposits) | 48-byte key, empty for other types |
| `withdrawal_credentials` | String | Request data | Withdrawal credentials (deposits) | 32-byte credentials, empty for others |
| `amount` | UInt64 | Request data | Amount (deposits/withdrawals) | Wei amount, 0 for consolidations |
| `signature` | String | Request data | Signature (deposits) | 96-byte signature, empty for others |
| `deposit_request_index` | UInt64 | Request data | Global deposit index | Only for deposit requests |
| `source_address` | String | Request data | Source address (withdrawals/consolidations) | 20-byte address |
| `validator_pubkey` | String | Request data | Validator key (withdrawals) | 48-byte key |
| `source_pubkey` | String | Request data | Source validator key (consolidations) | 48-byte key |
| `target_pubkey` | String | Request data | Target validator key (consolidations) | 48-byte key |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - request timestamp | Same as containing block |

**Self-Describing Parsing for Request Types**:
```python
# Set SSZ sizes for self-describing parsing
parse_deposit_request.ssz_size = 192     # 48+32+8+96+8 bytes
parse_withdrawal_request.ssz_size = 76   # 20+48+8 bytes  
parse_consolidation_request.ssz_size = 116 # 20+48+48 bytes
```

---

## State Management

### Era Completion Tracking

The unified state management system tracks processing completion using a simplified table:

```sql
CREATE TABLE era_completion (
    network String,
    era_number UInt32,
    status Enum8('processing' = 0, 'completed' = 1, 'failed' = 2),
    slot_start UInt32,
    slot_end UInt32,
    total_records UInt64,
    datasets_processed Array(String),
    processing_started_at DateTime,
    completed_at DateTime DEFAULT now(),
    error_message String DEFAULT '',
    retry_count UInt8 DEFAULT 0,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY network
ORDER BY (network, era_number);
```

### Era Status View

A unified view provides the latest completion status:

```sql
CREATE VIEW era_status AS
SELECT 
    network,
    era_number,
    status,
    slot_start,
    slot_end,
    total_records,
    length(datasets_processed) as dataset_count,
    processing_started_at,
    completed_at,
    retry_count,
    error_message
FROM era_completion
WHERE (network, era_number, insert_version) IN (
    SELECT network, era_number, max(insert_version)
    FROM era_completion
    GROUP BY network, era_number
);
```

### State Management Operations

**Record Era Processing**:
```python
# Using EraStateManager
state_manager = EraStateManager()

# Start processing
state_manager.record_era_start(era_number, network)

# Record completion
state_manager.record_era_completion(era_number, network, datasets_processed, total_records)

# Record failure
state_manager.record_era_failure(era_number, network, error_message)
```

**Query Completion Status**:
```bash
# Check status
era-parser --era-status gnosis

# Output:
📊 Era Processing Summary (gnosis)
============================================================
✅ Completed eras: 138
❌ Failed eras: 3
📊 Total records: 9,645,234
```

**Clean Failed Eras**:
```bash
# Clean using unified state manager
era-parser --clean-failed-eras gnosis

# Force clean specific range
era-parser --remote --force-clean gnosis 1082-1100
```

## Data Export Considerations

### ClickHouse Optimizations
- **Single Timestamp**: All tables use `timestamp_utc` for unified time-based partitioning
- **Unified Batch Size**: Global batch size of 100,000 records for optimal performance
- **Atomic Processing**: Each era is processed atomically with unified state tracking
- **Streaming Insert**: Large datasets are automatically streamed with consistent batch sizes
- **Enhanced Data**: Comprehensive validator indices in attester slashings for deep analysis

### Export Formats
- **JSON**: Preserves full nested structure including enhanced validator data
- **CSV**: Flattened with JSON-encoded complex fields (validator indices as JSON arrays)
- **Parquet**: Efficient columnar storage with proper type preservation
- **ClickHouse**: Direct export with unified state management and single timestamp approach

### Performance Notes
- **Unified Processing**: `EraProcessor` provides consistent memory usage and performance
- **Self-Describing Parsers**: Eliminate offset table parsing errors for fixed-size structures
- **Single Global Batch**: Consistent 100,000 record batches across all tables
- **State Management**: Unified completion tracking eliminates redundant state checks