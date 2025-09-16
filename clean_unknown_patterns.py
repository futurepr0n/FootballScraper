#!/usr/bin/env python3
"""
Clean up remaining UNKNOWN patterns in filenames
"""

import os
from pathlib import Path

def clean_unknown_patterns():
    """Remove UNKNOWN patterns from CSV filenames"""
    
    boxscore_dir = Path(__file__).parent.parent / 'FootballData' / 'BOXSCORE_CSV'
    
    if not boxscore_dir.exists():
        print(f"Directory not found: {boxscore_dir}")
        return
    
    fixed_count = 0
    
    for csv_file in boxscore_dir.glob("*_UNKNOWN_*.csv"):
        old_name = csv_file.name
        new_name = old_name.replace('_UNKNOWN_', '_')
        new_path = csv_file.parent / new_name
        
        csv_file.rename(new_path)
        print(f"Fixed: {old_name} -> {new_name}")
        fixed_count += 1
    
    print(f"\nFixed {fixed_count} files with UNKNOWN pattern")
    
    # Final verification
    remaining = list(boxscore_dir.glob("*UNKNOWN*.csv"))
    if remaining:
        print(f"WARNING: {len(remaining)} files still contain UNKNOWN")
    else:
        print("All UNKNOWN patterns have been removed!")

if __name__ == "__main__":
    clean_unknown_patterns()