"""SSZ parsing utilities - Clean production version"""

import struct
from typing import List, Callable, Any, Optional

def read_uint32_at(data: bytes, offset: int) -> int:
    """Read uint32 at offset"""
    if offset + 4 > len(data):
        return 0
    return struct.unpack("<I", data[offset:offset+4])[0]

def read_uint64_at(data: bytes, offset: int) -> int:
    """Read uint64 at offset"""
    if offset + 8 > len(data):
        return 0
    return struct.unpack("<Q", data[offset:offset+8])[0]

def parse_list_of_items(data: bytes, item_parser_func: Callable, *args) -> List[Any]:
    """
    Parse SSZ list with proper offset handling
    
    Args:
        data: SSZ encoded list data
        item_parser_func: Function to parse individual items
        *args: Additional arguments for parser function
        
    Returns:
        List of parsed items
    """
    items = []
    if not data:
        return items

    # Special handling for fixed-size items that don't use offset tables
    fixed_size_parsers = {
        'parse_withdrawal': 44,
        'parse_deposit_request': 192, 
        'parse_withdrawal_request': 76,
        'parse_consolidation_request': 116,
        'parse_kzg_commitment': 48
    }
    
    parser_name = item_parser_func.__name__ if hasattr(item_parser_func, '__name__') else 'unknown'
    
    if parser_name in fixed_size_parsers:
        item_size = fixed_size_parsers[parser_name]
        for i in range(len(data) // item_size):
            item_data = data[i*item_size : (i+1)*item_size]
            parsed = item_parser_func(item_data, *args)
            if parsed:
                items.append(parsed)
        return items

    # For variable-size items, check the offset pattern
    first_offset = read_uint32_at(data, 0)
    
    # Handle single item case: first_offset=0 means no offset table, data starts at position 0
    if first_offset == 0:
        item_data = data  # Use all the data
        parsed = item_parser_func(item_data, *args)
        if parsed:
            items.append(parsed)
        return items
    
    # Handle multiple items case (normal offset table)
    if not (4 <= first_offset <= len(data) and first_offset % 4 == 0):
        return items

    num_items = first_offset // 4
    if num_items == 0:
        return items
    
    offsets = [read_uint32_at(data, i * 4) for i in range(num_items)]
    
    # Parse each item using the offset table
    for i in range(num_items):
        start = offsets[i]
        end = offsets[i+1] if i + 1 < len(offsets) else len(data)
        
        if start >= len(data) or end > len(data) or start >= end:
            continue
            
        item_data = data[start:end]
        parsed = item_parser_func(item_data, *args)
        
        if parsed:
            items.append(parsed)
            
    return items