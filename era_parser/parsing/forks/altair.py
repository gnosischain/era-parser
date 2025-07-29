from typing import Dict, Any
from .phase0 import Phase0Parser

class AltairParser(Phase0Parser):
    """Parser for Altair fork blocks - adds sync_aggregate"""
    
    # Define block body schema declaratively
    BODY_SCHEMA = [
        ('fixed', 'sync_aggregate', 160),  # Fixed 160-byte sync aggregate
    ]