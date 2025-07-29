import struct
import snappy

def decompress_snappy_framed(compressed_data: bytes) -> bytes:
    """
    Decompress snappy-framed data from era files
    
    Args:
        compressed_data: Compressed bytes
        
    Returns:
        Decompressed bytes
        
    Raises:
        ValueError: If decompression fails
    """
    try:
        return snappy.decompress(compressed_data)
    except Exception:
        pass

    # Handle framed format
    pos = 10 if compressed_data[:10] == b'\xff\x06\x00\x00sNaPpY' else 0
    decompressed_chunks = []
    
    while pos < len(compressed_data):
        if pos + 4 > len(compressed_data):
            break
            
        frame_type = compressed_data[pos]
        frame_len = struct.unpack("<I", compressed_data[pos+1:pos+4] + b'\x00')[0]
        pos += 4
        
        if pos + frame_len > len(compressed_data):
            break
        
        chunk_data = compressed_data[pos:pos+frame_len]
        
        if frame_type == 0x00 and len(chunk_data) >= 4:
            try:
                decompressed_chunks.append(snappy.uncompress(chunk_data[4:]))
            except Exception:
                pass
        elif frame_type == 0x01 and len(chunk_data) >= 4:
            decompressed_chunks.append(chunk_data[4:])
            
        pos += frame_len
    
    if decompressed_chunks:
        return b''.join(decompressed_chunks)
        
    raise ValueError("Failed to decompress snappy framed data")