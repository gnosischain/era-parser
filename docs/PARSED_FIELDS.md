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
- [Execution Request Tables (Electra+)](#execution-request-tables-electra)
  - [deposit_requests](#deposit_requests-table)
  - [withdrawal_requests](#withdrawal_requests-table)  
  - [consolidation_requests](#consolidation_requests-table)
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

---

## deposits Table

**Purpose**: Validator deposit data for new validator registrations

**Parsing Logic**: Parses deposit objects with proof data and deposit information using self-describing parser. Deposits are 1240 bytes: 1056 bytes of proof + 184 bytes of deposit data.

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

---

## voluntary_exits Table

**Purpose**: Voluntary validator exit requests

**Parsing Logic**: Parses voluntary exit messages using self-describing parser showing validators requesting to exit the network.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `exit_index` | UInt64 | Array index | Index within block | 0-based position |
| `signature` | String | `voluntary_exit.signature` | Exit signature | 96-byte BLS signature |
| `epoch` | UInt64 | `voluntary_exit.message.epoch` | Exit epoch | When exit takes effect |
| `validator_index` | UInt64 | `voluntary_exit.message.validator_index` | Exiting validator | Validator index |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - exit request timestamp | Same as containing block |

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

---

## bls_changes Table

**Purpose**: BLS to execution address changes (Capella+ forks)

**Parsing Logic**: Parses BLS to execution changes using self-describing parser allowing validators to update their withdrawal credentials.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `change_index` | UInt64 | Array index | Index within block | 0-based position |
| `signature` | String | `bls_change.signature` | Change signature | 96-byte BLS signature |
| `validator_index` | UInt64 | `bls_change.message.validator_index` | Validator index | Validator making change |
| `from_bls_pubkey` | String | `bls_change.message.from_bls_pubkey` | Old BLS public key | 48-byte public key |
| `to_execution_address` | String | `bls_change.message.to_execution_address` | New execution address | 20-byte address |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - change timestamp | Same as containing block |

---

## blob_commitments Table

**Purpose**: Blob KZG commitments (Deneb+ forks, EIP-4844)

**Parsing Logic**: Extracts KZG commitments for blob transactions from the blob_kzg_commitments array using self-describing parser.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `commitment_index` | UInt64 | Array index | Index within block | 0-based position in commitments array |
| `commitment` | String | `blob_kzg_commitments[i]` | KZG commitment | 48-byte commitment to blob data |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - commitment timestamp | Same as containing block |

---

## Execution Request Tables (Electra+)

The Electra fork introduces execution layer requests for validator lifecycle management. Instead of a single unified table, Era Parser uses **separate optimized tables** for each request type.

### Benefits of Separate Tables:
- **Better Performance**: Each table optimized for its specific data type
- **Cleaner Queries**: `SELECT * FROM consolidation_requests WHERE ...`
- **No NULL Pollution**: Every column has meaningful data
- **Type Safety**: Proper data types for each field
- **Better Indexing**: Optimized ORDER BY for each request type
- **Future-Proof**: Easy to add new request types as separate tables

---

## deposit_requests Table

**Purpose**: Deposit requests from execution layer (EIP-6110, Electra+)

**Parsing Logic**: Parses execution layer deposit requests using self-describing parser. These are validator deposits initiated through the execution layer rather than the traditional deposit contract.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `request_index` | UInt64 | Array index | Index within deposits array | 0-based position |
| `pubkey` | String | `deposit_request.pubkey` | Validator public key | 48-byte BLS public key |
| `withdrawal_credentials` | String | `deposit_request.withdrawal_credentials` | Withdrawal credentials | 32-byte commitment |
| `amount` | UInt64 | `deposit_request.amount` | Deposit amount | Gwei amount |
| `signature` | String | `deposit_request.signature` | Deposit signature | 96-byte BLS signature |
| `deposit_request_index` | UInt64 | `deposit_request.index` | Global deposit request index | Monotonically increasing |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - request timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
parse_deposit_request.ssz_size = 192  # 48+32+8+96+8 bytes
```

---

## withdrawal_requests Table

**Purpose**: Withdrawal requests from execution layer (EIP-7002, Electra+)

**Parsing Logic**: Parses execution layer withdrawal requests using self-describing parser. These allow programmatic validator withdrawals initiated through smart contracts.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `request_index` | UInt64 | Array index | Index within withdrawals array | 0-based position |
| `source_address` | String | `withdrawal_request.source_address` | Requesting execution address | 20-byte address that initiated request |
| `validator_pubkey` | String | `withdrawal_request.validator_pubkey` | Validator public key | 48-byte BLS public key to withdraw |
| `amount` | UInt64 | `withdrawal_request.amount` | Withdrawal amount | Gwei amount to withdraw |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - request timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
parse_withdrawal_request.ssz_size = 76  # 20+48+8 bytes
```

---

## consolidation_requests Table

**Purpose**: Consolidation requests from execution layer (EIP-7251, Electra+)

**Parsing Logic**: Parses execution layer consolidation requests using self-describing parser. These allow combining multiple validators into fewer validators to optimize staking operations.

| Field | Type | Source | Description | Parsing Notes |
|-------|------|--------|-------------|---------------|
| `slot` | UInt64 | Block slot | Beacon block slot | Links to blocks table |
| `request_index` | UInt64 | Array index | Index within consolidations array | 0-based position |
| `source_address` | String | `consolidation_request.source_address` | Requesting execution address | 20-byte address that initiated request |
| `source_pubkey` | String | `consolidation_request.source_pubkey` | Source validator public key | 48-byte BLS public key (validator being consolidated) |
| `target_pubkey` | String | `consolidation_request.target_pubkey` | Target validator public key | 48-byte BLS public key (validator receiving consolidation) |
| `timestamp_utc` | DateTime | Block timestamp | **Single timestamp** - request timestamp | Same as containing block |

**Self-Describing Parsing**:
```python
parse_consolidation_request.ssz_size = 116  # 20+48+48 bytes
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

### Updated Dataset List

The complete list of datasets now includes separate execution request tables:

```python
ALL_DATASETS = [
    'blocks', 'sync_aggregates', 'execution_payloads', 'transactions', 
    'withdrawals', 'attestations', 'deposits', 'voluntary_exits',
    'proposer_slashings', 'attester_slashings', 'bls_changes', 
    'blob_commitments',
    'deposit_requests', 'withdrawal_requests', 'consolidation_requests'
]
```

## Data Export Considerations

### ClickHouse Optimizations
- **Separate Tables**: Each execution request type gets its own optimized table
- **Single Timestamp**: All tables use `timestamp_utc` for unified time-based partitioning
- **Unified Batch Size**: Global batch size of 100,000 records for optimal performance
- **Atomic Processing**: Each era is processed atomically with unified state tracking
- **Type-Specific Indexing**: Each table has optimal ORDER BY for its data type

### Export Formats
- **JSON**: Preserves full nested structure with separate request type objects
- **CSV**: Flattened with separate files for each request type when using `--separate`
- **Parquet**: Efficient columnar storage with proper type preservation
- **ClickHouse**: Direct export to separate optimized tables

### Performance Notes
- **Unified Processing**: `EraProcessor` provides consistent memory usage and performance
- **Self-Describing Parsers**: Eliminate offset table parsing errors for fixed-size structures
- **Single Global Batch**: Consistent 100,000 record batches across all tables
- **State Management**: Unified completion tracking eliminates redundant state checks
- **Optimized Queries**: Separate tables enable efficient type-specific queries