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

## Overview

Era Parser extracts data from beacon chain era files using fork-specific parsers and exports to multiple formats. All tables use a **single timestamp approach** with `timestamp_utc` for efficient time-based partitioning.

### Common Parsing Principles

1. **Single Timestamp**: All records use `timestamp_utc` calculated from slot timing or execution payload
2. **Fork Detection**: Automatic detection of fork version based on slot and network
3. **Graceful Degradation**: Missing or malformed data results in default values, not parsing failures
4. **Consistent Types**: All numeric values are converted to appropriate types (strings for large numbers)
5. **Hex Encoding**: All hash values and binary data are hex-encoded with `0x` prefix

## Core Tables

## blocks Table

**Purpose**: Beacon chain block headers and metadata (no execution layer data)

**Parsing Logic**: Extracts from the beacon block message structure, excluding execution payload and sync aggregate data which are stored in separate tables.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | `message.slot` | Block slot number | Primary identifier, extracted from block header |
| `proposer_index` | UInt64 | `message.proposer_index` | Validator index that proposed this block | Converted from string to uint64 |
| `parent_root` | String | `message.parent_root` | Hash of the parent block | 32-byte hash, hex-encoded with 0x prefix |
| `state_root` | String | `message.state_root` | Root hash of the beacon state | 32-byte hash, hex-encoded with 0x prefix |
| `signature` | String | `data.signature` | Block signature from proposer | 96-byte BLS signature, hex-encoded |
| `version` | String | Fork detection | Fork version (phase0, altair, bellatrix, etc.) | Determined by slot number and network config |
| `timestamp_utc` | DateTime | Calculated | Block timestamp in UTC | Calculated from genesis time + (slot × seconds_per_slot) or execution payload timestamp |
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
    eth1_block_hash String DEFAULT ''
) ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfMonth(timestamp_utc)
ORDER BY (slot, proposer_index)
```

---

## sync_aggregates Table

**Purpose**: Sync committee participation data (Altair+ forks)

**Parsing Logic**: Extracts sync committee bits and signature from the sync_aggregate field. This is a fixed 160-byte structure embedded directly in the block body.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Block slot containing this sync aggregate | Links to blocks table |
| `sync_committee_bits` | String | `sync_aggregate.sync_committee_bits` | Bitfield of participating validators | 64-byte bitfield, hex-encoded |
| `sync_committee_signature` | String | `sync_aggregate.sync_committee_signature` | BLS aggregate signature | 96-byte signature, hex-encoded |
| `timestamp_utc` | DateTime | Block timestamp | Same as block timestamp | Inherited from containing block |
| `participating_validators` | UInt32 | Calculated | Number of participating validators | Count of set bits in sync_committee_bits |

**Parsing Implementation**:
```python
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

**Parsing Logic**: Extracts execution payload data from the execution_payload field. Structure varies by fork (Deneb adds blob gas fields).

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
| `timestamp_utc` | DateTime | `execution_payload.timestamp` | Block timestamp | Primary timestamp, converted from Unix timestamp |
| `base_fee_per_gas` | String | `execution_payload.base_fee_per_gas` | EIP-1559 base fee | Wei amount as string (large number) |
| `block_hash` | String | `execution_payload.block_hash` | Execution block hash | 32-byte block identifier |
| `blob_gas_used` | UInt64 | `execution_payload.blob_gas_used` | Gas used by blobs (Deneb+) | EIP-4844 blob gas consumption |
| `excess_blob_gas` | UInt64 | `execution_payload.excess_blob_gas` | Excess blob gas (Deneb+) | For blob fee calculation |
| `extra_data` | String | `execution_payload.extra_data` | Extra data field | Arbitrary data, hex-encoded |
| `transactions_count` | UInt64 | Calculated | Number of transactions | Length of transactions array |
| `withdrawals_count` | UInt64 | Calculated | Number of withdrawals (Capella+) | Length of withdrawals array |

**Fork-Specific Parsing**:
- **Bellatrix**: Basic execution payload
- **Capella**: Adds withdrawals support
- **Deneb**: Adds `blob_gas_used` and `excess_blob_gas` fields

---

## transactions Table

**Purpose**: Individual transaction records from execution payloads

**Parsing Logic**: Extracts transaction hashes from the execution payload's transactions array. Each transaction is currently stored as a hash only.

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
| `timestamp_utc` | DateTime | Block timestamp | Transaction timestamp | Same as containing block |

**Note**: Era files contain transaction hashes only. Full transaction data requires separate RPC calls to execution clients.

---

## withdrawals Table

**Purpose**: Validator withdrawal records (Capella+ forks)

**Parsing Logic**: Parses withdrawal objects from execution payload's withdrawals array. Each withdrawal is a fixed 44-byte structure.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `block_number` | UInt64 | `execution_payload.block_number` | Execution block number | From containing execution payload |
| `block_hash` | String | `execution_payload.block_hash` | Execution block hash | From containing execution payload |
| `withdrawal_index` | UInt64 | `withdrawal.index` | Global withdrawal index | Monotonically increasing |
| `validator_index` | UInt64 | `withdrawal.validator_index` | Validator being withdrawn | Beacon chain validator index |
| `address` | String | `withdrawal.address` | Withdrawal destination | 20-byte execution address |
| `amount` | UInt64 | `withdrawal.amount` | Withdrawal amount | Wei amount (Gwei × 10^9) |
| `timestamp_utc` | DateTime | Block timestamp | Withdrawal timestamp | Same as containing block |

**Parsing Implementation**:
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
```

---

## attestations Table

**Purpose**: Validator attestation data showing consensus votes

**Parsing Logic**: Parses attestation objects from block body. Each attestation contains vote data and aggregation information.

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
| `timestamp_utc` | DateTime | Block timestamp | Attestation inclusion timestamp | Same as containing block |

**Consensus Significance**: Attestations are the primary consensus mechanism, showing which validators voted for which blocks and checkpoints.

---

## deposits Table

**Purpose**: Validator deposit data for new validator registrations

**Parsing Logic**: Parses deposit objects with proof data and deposit information. Deposits are 1240 bytes: 1056 bytes of proof + 184 bytes of deposit data.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `deposit_index` | UInt64 | Array index | Index within block | 0-based position |
| `pubkey` | String | `deposit.data.pubkey` | Validator public key | 48-byte BLS public key |
| `withdrawal_credentials` | String | `deposit.data.withdrawal_credentials` | Withdrawal credentials | 32-byte commitment |
| `amount` | UInt64 | `deposit.data.amount` | Deposit amount | Gwei amount (32 ETH = 32000000000) |
| `signature` | String | `deposit.data.signature` | Deposit signature | 96-byte BLS signature |
| `proof` | String | `deposit.proof` | Merkle proof | JSON array of 33 32-byte hashes |
| `timestamp_utc` | DateTime | Block timestamp | Deposit inclusion timestamp | Same as containing block |

**Parsing Implementation**:
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
```

---

## voluntary_exits Table

**Purpose**: Voluntary validator exit requests

**Parsing Logic**: Parses voluntary exit messages showing validators requesting to exit the network.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `exit_index` | UInt64 | Array index | Index within block | 0-based position |
| `signature` | String | `voluntary_exit.signature` | Exit signature | 96-byte BLS signature |
| `epoch` | UInt64 | `voluntary_exit.message.epoch` | Exit epoch | When exit takes effect |
| `validator_index` | UInt64 | `voluntary_exit.message.validator_index` | Exiting validator | Validator index |
| `timestamp_utc` | DateTime | Block timestamp | Exit request timestamp | Same as containing block |

---

## proposer_slashings Table

**Purpose**: Evidence of proposer violations (double block proposals)

**Parsing Logic**: Parses proposer slashing evidence containing two conflicting block headers from the same proposer.

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
| `timestamp_utc` | DateTime | Block timestamp | Slashing evidence timestamp | Same as containing block |

**Slashing Conditions**: Occurs when a validator proposes two different blocks for the same slot.

---

## attester_slashings Table

**Purpose**: Evidence of attester violations (conflicting attestations)

**Parsing Logic**: Parses attester slashing evidence containing two conflicting attestations that violate slashing conditions.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `slashing_index` | UInt64 | Array index | Index within block | 0-based position |
| `att_1_slot` | UInt64 | `attestation_1.data.slot` | First attestation slot | From first attestation |
| `att_1_committee_index` | UInt64 | `attestation_1.data.index` | First committee index | Committee that made attestation |
| `att_1_beacon_block_root` | String | `attestation_1.data.beacon_block_root` | First block root | 32-byte hash |
| `att_1_source_epoch` | UInt64 | `attestation_1.data.source.epoch` | First source epoch | Source checkpoint |
| `att_1_target_epoch` | UInt64 | `attestation_1.data.target.epoch` | First target epoch | Target checkpoint |
| `att_1_signature` | String | `attestation_1.signature` | First signature | 96-byte aggregate signature |
| `att_2_slot` | UInt64 | `attestation_2.data.slot` | Second attestation slot | From second attestation |
| `att_2_committee_index` | UInt64 | `attestation_2.data.index` | Second committee index | Committee that made attestation |
| `att_2_beacon_block_root` | String | `attestation_2.data.beacon_block_root` | Second block root | 32-byte hash |
| `att_2_source_epoch` | UInt64 | `attestation_2.data.source.epoch` | Second source epoch | Source checkpoint |
| `att_2_target_epoch` | UInt64 | `attestation_2.data.target.epoch` | Second target epoch | Target checkpoint |
| `att_2_signature` | String | `attestation_2.signature` | Second signature | 96-byte aggregate signature |
| `timestamp_utc` | DateTime | Block timestamp | Slashing evidence timestamp | Same as containing block |

**Slashing Conditions**: 
1. **Double voting**: Two different votes for the same target epoch
2. **Surround voting**: One attestation surrounds another

---

## bls_changes Table

**Purpose**: BLS to execution address changes (Capella+ forks)

**Parsing Logic**: Parses BLS to execution changes allowing validators to update their withdrawal credentials.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `change_index` | UInt64 | Array index | Index within block | 0-based position |
| `signature` | String | `bls_change.signature` | Change signature | 96-byte BLS signature |
| `validator_index` | UInt64 | `bls_change.message.validator_index` | Validator index | Validator making change |
| `from_bls_pubkey` | String | `bls_change.message.from_bls_pubkey` | Old BLS public key | 48-byte public key |
| `to_execution_address` | String | `bls_change.message.to_execution_address` | New execution address | 20-byte address |
| `timestamp_utc` | DateTime | Block timestamp | Change timestamp | Same as containing block |

**Purpose**: Allows validators to change from BLS withdrawal credentials to execution layer addresses for more flexible withdrawals.

---

## blob_commitments Table

**Purpose**: Blob KZG commitments (Deneb+ forks, EIP-4844)

**Parsing Logic**: Extracts KZG commitments for blob transactions from the blob_kzg_commitments array.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `commitment_index` | UInt64 | Array index | Index within block | 0-based position in commitments array |
| `commitment` | String | `blob_kzg_commitments[i]` | KZG commitment | 48-byte commitment to blob data |
| `timestamp_utc` | DateTime | Block timestamp | Commitment timestamp | Same as containing block |

**EIP-4844 Context**: Blob commitments enable data availability for rollups while keeping the actual blob data off-chain.

---

## execution_requests Table

**Purpose**: Execution layer requests (Electra+ forks)

**Parsing Logic**: Parses three types of execution requests: deposits, withdrawals, and consolidations. Uses a unified table with request_type discrimination.

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
| `timestamp_utc` | DateTime | Block timestamp | Request timestamp | Same as containing block |

**Request Types**:
1. **Deposit Requests**: Execution layer initiated validator deposits
2. **Withdrawal Requests**: Execution layer initiated withdrawals
3. **Consolidation Requests**: Merge multiple validators into one

**Parsing Implementation**:
```python
def parse_execution_requests(self, data: bytes) -> Dict[str, Any]:
    """Parse ExecutionRequests with 3 request types"""
    deposits_offset = read_uint32_at(data, 0)
    withdrawals_offset = read_uint32_at(data, 4) 
    consolidations_offset = read_uint32_at(data, 8)
    
    result = {"deposits": [], "withdrawals": [], "consolidations": []}
    
    # Parse each request type using fixed-size parsing
    if deposits_offset < len(data):
        deposits_data = data[deposits_offset:withdrawals_offset]
        result["deposits"] = parse_list_of_items(deposits_data, self.parse_deposit_request)
    
    # Similar for withdrawals and consolidations...
    return result
```

---

## Data Export Considerations

### ClickHouse Optimizations
- **Partitioning**: All tables are partitioned by `toStartOfMonth(timestamp_utc)` for efficient time-range queries
- **Ordering**: Primary keys are optimized for common query patterns
- **Data Types**: Large numbers are stored as strings to avoid overflow issues
- **Compression**: ClickHouse's native compression handles repetitive hash prefixes efficiently

### File Export Formats
- **JSON**: Preserves full nested structure and type information
- **CSV**: Flattened with JSON-encoded complex fields
- **Parquet**: Efficient columnar storage with proper type preservation
- **Separate Files**: `--separate` flag creates one file per table for easier analysis

### Performance Notes
- **Memory Usage**: Streaming parsing keeps memory usage constant regardless of era size
- **Processing Speed**: ~8192 blocks processed in seconds with progress indicators
- **Storage**: ClickHouse provides 10x+ compression compared to JSON files
- **Query Performance**: Indexed fields enable sub-second queries on millions of records