from typing import Dict, Any
from .base import BaseForkParser

class Phase0Parser(BaseForkParser):
    """Parser for Phase 0 blocks"""
    
    # Define block body schema declaratively
    BODY_SCHEMA = [
        # No additional fields beyond the base 5 variable fields
    ]