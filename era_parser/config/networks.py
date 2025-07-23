"""Network configuration for different Ethereum networks"""

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
            'electra': 999999999  # Future fork
        }
    },
    'gnosis': {
        'GENESIS_TIME': 1638993340,
        'SECONDS_PER_SLOT': 5,
        'SLOTS_PER_EPOCH': 16,
        'SLOTS_PER_HISTORICAL_ROOT': 8192,
        'FORK_EPOCHS': {
            'altair': 512,
            'bellatrix': 385536,
            'capella': 648704,
            'deneb': 889856,
            'electra': 1337856
        }
    },
    'sepolia': {
        'GENESIS_TIME': 1655733600,
        'SECONDS_PER_SLOT': 12,
        'SLOTS_PER_EPOCH': 32,
        'SLOTS_PER_HISTORICAL_ROOT': 8192,
        'FORK_EPOCHS': {
            'altair': 50,
            'bellatrix': 100,
            'capella': 56832,
            'deneb': 132608,
            'electra': 999999999  # Future fork
        }
    }
}

def get_network_config(network_name: str) -> dict:
    """Get network configuration by name"""
    network_name = network_name.lower()
    if network_name not in NETWORK_CONFIGS:
        raise ValueError(f"Unknown network: {network_name}. Available: {list(NETWORK_CONFIGS.keys())}")
    return NETWORK_CONFIGS[network_name]

def detect_network_from_filename(filename: str) -> str:
    """Detect network from era filename"""
    filename = filename.lower()
    for network in NETWORK_CONFIGS.keys():
        if network in filename:
            return network
    return 'mainnet'  # Default fallback