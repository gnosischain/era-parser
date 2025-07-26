"""Era slot range calculation utilities"""

from typing import Tuple, List
from ..config import get_network_config

class EraSlotCalculator:
    """Calculate slot ranges for eras across networks"""
    
    @staticmethod
    def get_era_slot_range(network: str, era_number: int) -> Tuple[int, int]:
        """
        Get start and end slots for an era
        
        Args:
            network: Network name (mainnet, gnosis, sepolia)
            era_number: Era number
            
        Returns:
            Tuple of (start_slot, end_slot)
        """
        try:
            config = get_network_config(network)
            slots_per_era = config['SLOTS_PER_HISTORICAL_ROOT']
            
            # Ensure era_number is valid
            if era_number is None:
                era_number = 0
            
            start_slot = era_number * slots_per_era
            end_slot = start_slot + slots_per_era - 1
            
            return start_slot, end_slot
        except Exception as e:
            # Fallback for gnosis
            slots_per_era = 8192
            if era_number is None:
                era_number = 0
            start_slot = era_number * slots_per_era
            end_slot = start_slot + slots_per_era - 1
            return start_slot, end_slot
    
    @staticmethod
    def get_era_from_slot(network: str, slot: int) -> int:
        """
        Get era number from slot
        
        Args:
            network: Network name
            slot: Slot number
            
        Returns:
            Era number
        """
        try:
            config = get_network_config(network)
            slots_per_era = config['SLOTS_PER_HISTORICAL_ROOT']
            
            if slot is None:
                slot = 0
                
            return slot // slots_per_era
        except Exception:
            # Fallback
            if slot is None:
                slot = 0
            return slot // 8192
    
    @staticmethod
    def get_overlapping_eras(network: str, slot_start: int, slot_end: int) -> List[int]:
        """
        Find eras that overlap with slot range
        
        Args:
            network: Network name
            slot_start: Start slot
            slot_end: End slot
            
        Returns:
            List of era numbers that overlap the slot range
        """
        if slot_start is None:
            slot_start = 0
        if slot_end is None:
            slot_end = 0
            
        start_era = EraSlotCalculator.get_era_from_slot(network, slot_start)
        end_era = EraSlotCalculator.get_era_from_slot(network, slot_end)
        
        return list(range(start_era, end_era + 1))