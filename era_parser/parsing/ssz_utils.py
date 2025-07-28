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
    Parse SSZ list with self-describing item parsers
    
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

    # Check if parser has self-described SSZ size
    item_size = getattr(item_parser_func, 'ssz_size', None)
    
    if item_size is not None:
        # Handle fixed-size items directly
        if len(data) % item_size != 0:
            print(f"Warning: Data length {len(data)} is not a multiple of item size {item_size}")
            num_complete_items = len(data) // item_size
            if num_complete_items > 0:
                data = data[:num_complete_items * item_size]
            else:
                return items
        
        num_items = len(data) // item_size
        
        for i in range(num_items):
            item_data = data[i*item_size : (i+1)*item_size]
            parsed = item_parser_func(item_data, *args)
            if parsed:
                items.append(parsed)
        return items

    # Handle variable-size items using offset table
    if len(data) < 4:
        parsed = item_parser_func(data, *args)
        if parsed:
            items.append(parsed)
        return items
    
    first_offset = read_uint32_at(data, 0)
    
    # Single item case
    if first_offset == 0:
        item_data = data
        parsed = item_parser_func(item_data, *args)
        if parsed:
            items.append(parsed)
        return items
    
    # Empty list case
    if first_offset == len(data):
        return items
    
    # Validate offset alignment
    if first_offset % 4 != 0 or first_offset < 4:
        parsed = item_parser_func(data, *args)
        if parsed:
            items.append(parsed)
        return items

    # Calculate number of items
    num_items = first_offset // 4
    
    if num_items == 0:
        return items
    
    # Validate offset table size
    if num_items * 4 > len(data):
        print(f"Not enough data for {num_items} offsets (need {num_items * 4}, have {len(data)})")
        return items
    
    # Read all offsets
    offsets = []
    for i in range(num_items):
        offset = read_uint32_at(data, i * 4)
        if offset <= len(data):
            offsets.append(offset)
    
    if not offsets:
        return items
    
    # Parse each item
    for i in range(len(offsets)):
        start = offsets[i]
        end = offsets[i + 1] if i + 1 < len(offsets) else len(data)
        
        if start >= len(data) or end > len(data) or start >= end:
            continue
            
        item_data = data[start:end]
        
        try:
            parsed = item_parser_func(item_data, *args)
            if parsed:
                items.append(parsed)
        except Exception as e:
            print(f"Error parsing item {i}: {e}")
            continue
            
    return items