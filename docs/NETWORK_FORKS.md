# Networks and Forks Support

This document details the networks and forks supported by Era Parser, including their configurations, transition epochs, and parsing capabilities.

## Supported Networks

### Mainnet (Ethereum)
- **Genesis Time**: December 1, 2020 (1606824023)
- **Slot Duration**: 12 seconds
- **Slots per Epoch**: 32
- **Network Identifier**: `mainnet`

**Fork Schedule**:
| Fork | Epoch | Slot | Activation Date |
|------|-------|------|----------------|
| Phase 0 | 0 | 0 | Dec 1, 2020 |
| Altair | 74,240 | 2,375,680 | Oct 27, 2021 |
| Bellatrix | 144,896 | 4,636,672 | Sep 6, 2022 |
| Capella | 194,048 | 6,209,536 | Apr 12, 2023 |
| Deneb | 269,568 | 8,626,176 | Mar 13, 2024 |
| Electra | 999,999,999 | Future | TBD |

### Gnosis Chain
- **Genesis Time**: December 8, 2021 (1638993340)
- **Slot Duration**: 5 seconds
- **Slots per Epoch**: 16
- **Network Identifier**: `gnosis`

**Fork Schedule**:
| Fork | Epoch | Slot | Activation Date |
|------|-------|------|----------------|
| Phase 0 | 0 | 0 | Dec 8, 2021 |
| Altair | 512 | 8,192 | Dec 8, 2021 |
| Bellatrix | 385,536 | 6,168,576 | Aug 10, 2022 |
| Capella | 648,704 | 10,379,264 | May 17, 2023 |
| Deneb | 889,856 | 14,237,696 | Jan 11, 2024 |
| Electra | 1,337,856 | 21,405,696 | TBD |

### Sepolia Testnet
- **Genesis Time**: June 20, 2022 (1655733600)
- **Slot Duration**: 12 seconds
- **Slots per Epoch**: 32
- **Network Identifier**: `sepolia`

**Fork Schedule**:
| Fork | Epoch | Slot | Activation Date |
|------|-------|------|----------------|
| Phase 0 | 0 | 0 | Jun 20, 2022 |
| Altair | 50 | 1,600 | Jun 20, 2022 |
| Bellatrix | 100 | 3,200 | Jun 30, 2022 |
| Capella | 56,832 | 1,818,624 | Feb 28, 2023 |
| Deneb | 132,608 | 4,243,456 | Jan 30, 2024 |
| Electra | 999,999,999 | Future | TBD |

## Fork Specifications

### Phase 0 (Genesis)
**Features**: Basic beacon chain consensus
- **Block Structure**: Proposer/attester slashings, attestations, deposits, voluntary exits
- **Data Available**: Blocks, attestations, deposits, voluntary exits, slashings
- **Parser**: `Phase0Parser`

### Altair (Light Client Support)
**Features**: Sync committees for light client support
- **New Fields**: `sync_aggregate` in block body
- **Data Available**: All Phase 0 data + sync aggregates
- **Parser**: `AltairParser`

**Sync Committee**:
- 512 validators per sync committee
- Committee changes every 256 epochs (~27 hours)
- Provides efficient light client verification

### Bellatrix (The Merge)
**Features**: Integration with execution layer
- **New Fields**: `execution_payload` in block body
- **Data Available**: All Altair data + execution payloads, transactions
- **Parser**: `BellatrixParser`

**Execution Payload**:
- Contains execution block data
- Links beacon and execution layers
- Includes transaction hashes and gas data

### Capella (Withdrawals)
**Features**: Validator withdrawals enabled
- **New Fields**: `withdrawals` in execution payload, `bls_to_execution_changes` in block body
- **Data Available**: All Bellatrix data + withdrawals, BLS changes
- **Parser**: `CapellaParser`

**Withdrawals**:
- Automatic withdrawals for excess balance
- Full withdrawals for exited validators
- Withdrawal queue processing

### Deneb (Blob Transactions / EIP-4844)
**Features**: Blob transactions for data availability
- **New Fields**: `blob_gas_used`, `excess_blob_gas` in execution payload, `blob_kzg_commitments` in block body
- **Data Available**: All Capella data + blob commitments, blob gas data
- **Parser**: `DenebParser`

**Blob Data**:
- Up to 6 blobs per block
- KZG commitments for data availability
- Separate fee market for blob gas

### Electra (Validator Lifecycle)
**Features**: Enhanced validator operations
- **New Fields**: `execution_requests` in block body
- **Data Available**: All Deneb data + execution requests (deposits, withdrawals, consolidations)
- **Parser**: `ElectraParser`

**Execution Requests**:
- Execution layer initiated deposits
- Programmatic withdrawal requests
- Validator consolidation operations

## Fork Detection Logic

Era Parser automatically detects the correct fork based on slot number and network configuration:

```python
def get_fork_by_slot(slot: int, network: str = 'mainnet') -> str:
    """Determine fork by slot number and network"""
    config = get_network_config(network)
    epoch = slot // config['SLOTS_PER_EPOCH']
    
    fork_epochs = config['FORK_EPOCHS']
    
    if epoch >= fork_epochs.get('electra', float('inf')):
        return 'electra'
    elif epoch >= fork_epochs.get('deneb', float('inf')):
        return 'deneb'
    elif epoch >= fork_epochs.get('capella', float('inf')):
        return 'capella'
    elif epoch >= fork_epochs.get('bellatrix', float('inf')):
        return 'bellatrix'
    elif epoch >= fork_epochs.get('altair', float('inf')):
        return 'altair'
    else:
        return 'phase0'
```

## Network Configuration

Each network is configured in `era_parser/config/networks.py`:

```python
NETWORK_CONFIGS = {
    'mainnet': {
        'GENESIS_TIME': 1606824023,
        'SECONDS_PER_SLOT': 12,
        'SLOTS_PER_EPOCH': 32,
        'SLOTS_PER_HISTORICAL_ROOT': 8192,
        'FORK_EPOCHS': {
            'altair': 74240,
            'bellatrix': 144896,
            'capella': 194048,
            'deneb': 269568,
            'electra': 999999999
        }
    },
    # ... other networks
}
```

## Adding New Networks

To add support for a new network:

1. **Add Network Configuration**:
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

2. **Update Network Detection**:
```python
def detect_network_from_filename(filename: str) -> str:
    """Detect network from era filename"""
    filename = filename.lower()
    if 'holesky' in filename:
        return 'holesky'
    # ... existing logic
```

3. **Test with Era Files**:
```bash
era-parser holesky-00001-abcd1234.era stats
```

## Adding New Forks

To extend support for future forks (example: Fulu):

1. **Add Fork Configuration** (`era_parser/config/forks.py`):
```python
FORK_CONFIGS['fulu'] = {
    'name': 'Fulu',
    'has_validator_consolidations': True,
    'has_advanced_attestations': True,
}
```

2. **Update Network Configurations**:
```python
NETWORK_CONFIGS['mainnet']['FORK_EPOCHS']['fulu'] = 1500000
NETWORK_CONFIGS['gnosis']['FORK_EPOCHS']['fulu'] = 2000000
```

3. **Create Fork Parser** (`era_parser/parsing/forks/fulu.py`):
```python
from .electra import ElectraParser

class FuluParser(ElectraParser):
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        result = super().parse_body(body_data)
        # Add Fulu-specific parsing logic
        return result
```

4. **Register Parser** (`era_parser/parsing/forks/__init__.py`):
```python
from .fulu import FuluParser

FORK_PARSERS = {
    # ... existing parsers ...
    'fulu': FuluParser,
}
```

## Era File Naming Conventions

Era Parser expects files to follow standard naming conventions:

- **Format**: `{network}-{era_number:05d}-{hash}.era`
- **Examples**:
  - `mainnet-02500-a1b2c3d4.era`
  - `gnosis-01337-fe3b60d1.era`
  - `sepolia-00100-deadbeef.era`

The parser automatically detects the network from the filename and determines the appropriate fork based on the era number and network configuration.

## Compatibility Matrix

| Network | Phase 0 | Altair | Bellatrix | Capella | Deneb | Electra | Fulu |
|---------|---------|--------|-----------|---------|-------|---------|------|
| Mainnet | ✅ | ✅ | ✅ | ✅ | ✅ |  ✅ |⏳ |
| Gnosis | ✅ | ✅ | ✅ | ✅ | ✅ |  ✅ |⏳ |
| Sepolia | ✅ | ✅ | ✅ | ✅ | ✅ |  ✅ |⏳ |

**Legend**: ✅ Supported, ⏳ Future/Planned

## Performance Characteristics

### Processing Speed by Fork
- **Phase 0**: ~10,000 blocks/second
- **Altair**: ~9,000 blocks/second (sync aggregates)
- **Bellatrix**: ~8,000 blocks/second (execution payloads)
- **Capella**: ~7,500 blocks/second (withdrawals)
- **Deneb**: ~7,000 blocks/second (blob commitments)
- **Electra**: ~6,500 blocks/second (execution requests)

### Data Size by Fork
- **Phase 0**: ~2KB average block size
- **Altair**: ~2.2KB (+sync aggregates)
- **Bellatrix**: ~5KB (+execution payloads)
- **Capella**: ~5.5KB (+withdrawals)
- **Deneb**: ~6KB (+blob data)
- **Electra**: ~6.5KB (+execution requests)
