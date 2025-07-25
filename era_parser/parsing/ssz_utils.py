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
    Parse SSZ list with proper offset handling - FIXED for better support of all item types
    
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

    # IMPROVED: Better parser name detection for type-specific handling
    parser_name = 'unknown'
    if hasattr(item_parser_func, '__name__'):
        parser_name = item_parser_func.__name__
    elif hasattr(item_parser_func, '__qualname__'):
        # Handle method names like 'BaseForkParser.parse_bls_to_execution_change'
        parser_name = item_parser_func.__qualname__.split('.')[-1]
    elif hasattr(item_parser_func, 'func'):
        # Handle partial functions
        parser_name = getattr(item_parser_func.func, '__name__', 'unknown')
    
    # ADDITIONAL: Check if it's a method and get the method name
    if parser_name == 'unknown' and hasattr(item_parser_func, '__self__'):
        # This is a bound method, try to get the method name
        method_name = getattr(item_parser_func, '__func__', None)
        if method_name and hasattr(method_name, '__name__'):
            parser_name = method_name.__name__

    # Special handling for fixed-size items that don't use offset tables
    fixed_size_parsers = {
        'parse_withdrawal': 44,
        'parse_deposit_request': 192, 
        'parse_withdrawal_request': 76,
        'parse_consolidation_request': 116,
        'parse_kzg_commitment': 48,
        'parse_voluntary_exit': 112,
        'parse_deposit': 1240,
        'parse_bls_to_execution_change': 172,  # BLS changes are FIXED SIZE!
    }
    
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

    # For variable-size items (like attestations, attester slashings), use offset table
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
    
    # FIXED: Better validation for offset table - must be aligned and reasonable
    if first_offset % 4 != 0 or first_offset < 4:
        # Only show warnings for unexpected cases, not for known variable-length parsers
        variable_length_parsers = {
            'parse_transaction_hash', 'parse_transaction_data', 'parse_hex_data',
            'parse_attestation', 'parse_attester_slashing', 'parse_indexed_attestation'
        }
        
        if parser_name not in variable_length_parsers:
            print(f"Invalid offset alignment for {parser_name}, first_offset={first_offset}, trying fallback parsing")
        
        # Try parsing as single item
        parsed = item_parser_func(data, *args)
        if parsed:
            items.append(parsed)
        return items

    # FIXED: Calculate number of items more carefully
    # The offset table size tells us how many items there are
    num_items = first_offset // 4
    
    if num_items == 0:
        return items
    
    # FIXED: Validate that we have enough data for all offsets
    if num_items * 4 > len(data):
        print(f"Not enough data for {num_items} offsets in {parser_name} (need {num_items * 4}, have {len(data)})")
        return items
    
    # Read all offsets
    offsets = []
    for i in range(num_items):
        offset = read_uint32_at(data, i * 4)
        if offset > len(data):
            print(f"Invalid offset {offset} at index {i} for data length {len(data)} in {parser_name}")
            # Don't add invalid offsets, but continue with valid ones
            continue
        offsets.append(offset)
    
    if not offsets:
        return items
    
    # FIXED: Parse each item using the offset table with proper bounds
    for i in range(len(offsets)):
        start = offsets[i]
        
        # Determine end position for this item
        if i + 1 < len(offsets):
            end = offsets[i + 1]
        else:
            end = len(data)
        
        # Validate bounds
        if start >= len(data) or end > len(data) or start >= end:
            print(f"Invalid item bounds in {parser_name}: start={start}, end={end}, data_length={len(data)}")
            continue
            
        item_data = data[start:end]
        
        try:
            parsed = item_parser_func(item_data, *args)
            if parsed:
                items.append(parsed)
        except Exception as e:
            print(f"Error parsing item {i} in {parser_name}: {e}")
            continue
            
    return items