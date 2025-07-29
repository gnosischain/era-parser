from .networks import get_network_config

FORK_CONFIGS = {
    'phase0': {
        'name': 'Phase 0',
        'fields': ['proposer_slashings', 'attester_slashings', 'attestations', 'deposits', 'voluntary_exits'],
        'has_sync_aggregate': False,
        'has_execution_payload': False,
        'has_bls_changes': False,
        'has_blob_commitments': False,
        'has_execution_requests': False,
    },
    'altair': {
        'name': 'Altair',
        'fields': ['proposer_slashings', 'attester_slashings', 'attestations', 'deposits', 'voluntary_exits'],
        'has_sync_aggregate': True,
        'has_execution_payload': False,
        'has_bls_changes': False,
        'has_blob_commitments': False,
        'has_execution_requests': False,
    },
    'bellatrix': {
        'name': 'Bellatrix (The Merge)',
        'fields': ['proposer_slashings', 'attester_slashings', 'attestations', 'deposits', 'voluntary_exits'],
        'has_sync_aggregate': True,
        'has_execution_payload': True,
        'has_bls_changes': False,
        'has_blob_commitments': False,
        'has_execution_requests': False,
    },
    'capella': {
        'name': 'Capella',
        'fields': ['proposer_slashings', 'attester_slashings', 'attestations', 'deposits', 'voluntary_exits'],
        'has_sync_aggregate': True,
        'has_execution_payload': True,
        'has_withdrawals': True,
        'has_bls_changes': True,
        'has_blob_commitments': False,
        'has_execution_requests': False,
    },
    'deneb': {
        'name': 'Deneb',
        'fields': ['proposer_slashings', 'attester_slashings', 'attestations', 'deposits', 'voluntary_exits'],
        'has_sync_aggregate': True,
        'has_execution_payload': True,
        'has_withdrawals': True,
        'has_bls_changes': True,
        'has_blob_commitments': True,
        'has_execution_requests': False,
    },
    'electra': {
        'name': 'Electra',
        'fields': ['proposer_slashings', 'attester_slashings', 'attestations', 'deposits', 'voluntary_exits'],
        'has_sync_aggregate': True,
        'has_execution_payload': True,
        'has_withdrawals': True,
        'has_bls_changes': True,
        'has_blob_commitments': True,
        'has_execution_requests': True,
    }
}

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

def get_fork_config(fork_name: str) -> dict:
    """Get fork configuration by name"""
    if fork_name not in FORK_CONFIGS:
        raise ValueError(f"Unknown fork: {fork_name}. Available: {list(FORK_CONFIGS.keys())}")
    return FORK_CONFIGS[fork_name]