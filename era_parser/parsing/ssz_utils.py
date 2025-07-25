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
    Parse SSZ list with proper offset handling - FIXED for better deposit support
    
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
        'parse_kzg_commitment': 48,
        'parse_voluntary_exit': 112,
        'parse_deposit': 1240,
    }
    
    parser_name = item_parser_func.__name__ if hasattr(item_parser_func, '__name__') else 'unknown'
    
    # FIXED: Handle deposits and other fixed-size items
    if parser_name in fixed_size_parsers:
        item_size = fixed_size_parsers[parser_name]
        
        # Ensure data length is multiple of item size
        if len(data) % item_size != 0:
            print(f"Warning: Data length {len(data)} is not a multiple of item size {item_size} for {parser_name}")
            # Try to parse as many complete items as possible
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

    # Special case: Proposer slashings are stored as raw 416-byte data (single item)
    if parser_name == 'parse_proposer_slashing' and len(data) == 416:
        parsed = item_parser_func(data, *args)
        if parsed:
            items.append(parsed)
        return items

    # For variable-size items, check the offset pattern
    if len(data) < 4:
        # Data too small for offset table, try parsing as single item
        parsed = item_parser_func(data, *args)
        if parsed:
            items.append(parsed)
        return items
    
    first_offset = read_uint32_at(data, 0)
    
    # Handle single item case: first_offset=0 means no offset table, data starts at position 0
    if first_offset == 0:
        item_data = data  # Use all the data
        parsed = item_parser_func(item_data, *args)
        if parsed:
            items.append(parsed)
        return items
    
    # Handle empty list case: first_offset equals data length
    if first_offset == len(data):
        return items
    
    # FIXED: Better validation for offset table
    if first_offset < 4 or first_offset > len(data) or first_offset % 4 != 0:
        # Invalid offset table - try parsing as single item or multiple fixed-size items
        print(f"Invalid offset table for {parser_name}, first_offset={first_offset}, trying fallback parsing")
        
        # For deposits, try fixed-size parsing as fallback
        if parser_name == 'parse_deposit':
            print(f"Trying fixed-size parsing for deposits with data length {len(data)}")
            # Recursive call will use the fixed-size logic above
            return parse_list_of_items(data, item_parser_func, *args)
        
        # For voluntary exits, try fixed-size parsing as fallback
        if parser_name == 'parse_voluntary_exit':
            print(f"Trying fixed-size parsing for voluntary exits with data length {len(data)}")
            # Recursive call will use the fixed-size logic above
            return parse_list_of_items(data, item_parser_func, *args)
        
        # For other types, try parsing as single item
        parsed = item_parser_func(data, *args)
        if parsed:
            items.append(parsed)
        return items

    num_items = first_offset // 4
    if num_items == 0:
        return items
    
    # FIXED: Better bounds checking for offsets
    if num_items * 4 > len(data):
        print(f"Invalid number of items {num_items} for data length {len(data)}")
        return items
    
    offsets = []
    for i in range(num_items):
        offset = read_uint32_at(data, i * 4)
        if offset > len(data):
            print(f"Invalid offset {offset} at index {i} for data length {len(data)}")
            continue
        offsets.append(offset)
    
    if not offsets:
        return items
    
    # Parse each item using the offset table
    for i in range(len(offsets)):
        start = offsets[i]
        end = offsets[i+1] if i + 1 < len(offsets) else len(data)
        
        if start >= len(data) or end > len(data) or start >= end:
            print(f"Invalid item bounds: start={start}, end={end}, data_length={len(data)}")
            continue
            
        item_data = data[start:end]
        parsed = item_parser_func(item_data, *args)
        
        if parsed:
            items.append(parsed)
            
    return items