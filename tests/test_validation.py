"""
Simple validation tests comparing RPC data against era-parser data
"""
import pytest
from pathlib import Path
from deepdiff import DeepDiff
from tests.loaders import load_json_file, normalize_data


# Find all RPC files in test_data directory  
TEST_DATA_DIR = Path(__file__).parent / "test_data"
rpc_files = list(TEST_DATA_DIR.glob("*_rpc.json"))


@pytest.mark.parametrize("rpc_file", rpc_files, ids=lambda p: p.name)
def test_block_validation(rpc_file: Path):
    """
    Compare RPC data against era-parser data for consistency
    """
    # Find corresponding era file
    era_file = rpc_file.with_name(rpc_file.name.replace("_rpc.json", "_era.json"))
    
    assert era_file.exists(), f"Missing era file: {era_file.name}"
    
    # Load both files
    rpc_data = load_json_file(rpc_file)
    era_data = load_json_file(era_file)
    
    # Extract block data for comparison
    rpc_block = normalize_data(rpc_data)
    era_block = normalize_data(era_data)
    
    # Get all field paths for reporting
    matched_fields = get_all_field_paths(rpc_block)
    
    print(f"\n{'='*60}")
    print(f"🔍 VALIDATING: {rpc_file.name}")
    print(f"{'='*60}")
    print(f"📊 Total fields to validate: {len(matched_fields)}")
    
    # Compare using DeepDiff
    diff = DeepDiff(rpc_block, era_block, ignore_order=True)
    
    if not diff:
        print("✅ ALL FIELDS MATCHED PERFECTLY!")
        print("\n📋 Successfully validated fields:")
        for i, field in enumerate(sorted(matched_fields), 1):
            print(f"  {i:3d}. {field}")
    else:
        print("❌ DIFFERENCES FOUND:")
        if 'values_changed' in diff:
            print("\n🔄 Values that differ:")
            for path, change in diff['values_changed'].items():
                print(f"  • {path}")
                print(f"    RPC:  {change['old_value']}")
                print(f"    Era:  {change['new_value']}")
        
        if 'dictionary_item_removed' in diff:
            print("\n➖ Missing in era data:")
            for item in diff['dictionary_item_removed']:
                print(f"  • {item}")
        
        if 'dictionary_item_added' in diff:
            print("\n➕ Extra in era data:")
            for item in diff['dictionary_item_added']:
                print(f"  • {item}")
    
    print(f"\n{'='*60}")
    
    # Assert no differences
    assert not diff, f"Data mismatch for {rpc_file.name}:\n{diff.pretty()}"


def get_all_field_paths(data, prefix='', paths=None):
    """Recursively get all field paths in nested data"""
    if paths is None:
        paths = set()
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{prefix}.{key}" if prefix else key
            paths.add(current_path)
            
            if isinstance(value, (dict, list)):
                get_all_field_paths(value, current_path, paths)
    elif isinstance(data, list) and data:
        # For lists, show the structure of the first item
        if isinstance(data[0], dict):
            get_all_field_paths(data[0], f"{prefix}[0]", paths)
    
    return paths


def test_all_files_paired():
    """Ensure all RPC files have corresponding era files"""
    rpc_files = list(TEST_DATA_DIR.glob("*_rpc.json"))
    era_files = list(TEST_DATA_DIR.glob("*_era.json"))
    
    rpc_slots = {f.stem.replace("_rpc", "") for f in rpc_files}
    era_slots = {f.stem.replace("_era", "") for f in era_files}
    
    missing_era = rpc_slots - era_slots
    missing_rpc = era_slots - rpc_slots
    
    assert not missing_era, f"Missing era files for slots: {missing_era}"
    assert not missing_rpc, f"Missing RPC files for slots: {missing_rpc}"