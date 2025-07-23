from .base import BaseForkParser
from .phase0 import Phase0Parser
from .altair import AltairParser  
from .bellatrix import BellatrixParser
from .capella import CapellaParser
from .deneb import DenebParser
from .electra import ElectraParser

# Parser registry
FORK_PARSERS = {
    'phase0': Phase0Parser,
    'altair': AltairParser,
    'bellatrix': BellatrixParser,
    'capella': CapellaParser,
    'deneb': DenebParser,
    'electra': ElectraParser,
}

def get_fork_parser(fork_name: str) -> BaseForkParser:
    """Get fork parser instance by name"""
    if fork_name not in FORK_PARSERS:
        raise ValueError(f"Unknown fork: {fork_name}. Available: {list(FORK_PARSERS.keys())}")
    return FORK_PARSERS[fork_name]()

__all__ = [
    "BaseForkParser", "get_fork_parser", "FORK_PARSERS",
    "Phase0Parser", "AltairParser", "BellatrixParser", 
    "CapellaParser", "DenebParser", "ElectraParser"
]