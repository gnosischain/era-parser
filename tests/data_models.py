"""
Simple data models for validation
"""
from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class BeaconBlockData(BaseModel):
    """Flexible model for beacon block data"""
    data: Dict[str, Any]
    version: Optional[str] = None
    execution_optimistic: Optional[bool] = None
    finalized: Optional[bool] = None