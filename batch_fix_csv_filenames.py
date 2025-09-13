#!/usr/bin/env python3
"""
Non-interactive batch CSV filename fixer
Processes all CSV files to replace scraper run dates with actual game dates
"""

from fix_csv_filenames import CSVFileFixer

def main():
    print("🔧 BATCH CSV FILENAME CORRECTION")
    print("=" * 50)
    print("Processing ALL CSV files to fix incorrect dates...")
    print("This will replace scraper run dates with actual game dates")
    print()
    
    fixer = CSVFileFixer()
    fixer.load_cache()
    
    # Run the batch fix on all files
    fixer.run_batch_fix()
    
    return 0

if __name__ == "__main__":
    exit(main())