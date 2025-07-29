"""
Simple data loading utilities
"""
import json
from pathlib import Path
from typing import Dict, Any
from tests.data_models import BeaconBlockData


def load_json_file(filepath: Path) -> Dict[str, Any]:
    """Load and validate JSON file"""
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    # Validate structure
    validated = BeaconBlockData(**data)
    return validated.model_dump()


def normalize_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract just the block data for comparison"""
    return data.get('data', data)