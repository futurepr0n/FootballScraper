#!/usr/bin/env python3
"""
Fix CSV filename dates - replace scraper run dates with actual game dates
Batch processes all CSV files to extract correct dates from ESPN
"""

import os
import re
import subprocess
from pathlib import Path
import json
import time
import random

class CSVFileFixer:
    def __init__(self):
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.game_dates_cache = {}  # Cache game_id -> actual_date mapping
        self.failed_files = []
        self.success_count = 0
        self.skip_count = 0
        
    def extract_game_date_from_espn(self, game_id):
        """Extract actual game date from ESPN using game ID"""
        if game_id in self.game_dates_cache:
            print(f"  Using cached date for {game_id}")
            return self.game_dates_cache[game_id]
            
        espn_url = f"https://www.espn.com/nfl/game/_/gameId/{game_id}"
        print(f"  Fetching date from {espn_url}")
        
        try:
            # Use curl to fetch the page
            cmd = ['curl', '-s', '-m', '10', espn_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            if result.returncode != 0:
                print(f"  ❌ Curl failed for {game_id}")
                return None
                
            html_content = result.stdout
            
            # Look for title with date pattern: "Team1 XX-YY Team2 (Sep 10, 2023) Final Score - ESPN"
            title_match = re.search(r'<title[^>]*>.*?\(([^)]+)\).*?</title>', html_content)
            
            if title_match:
                date_str = title_match.group(1).strip()
                print(f"  Found date string: '{date_str}'")
                
                # Parse "Sep 10, 2023" format
                date_pattern = r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})'
                date_match = re.match(date_pattern, date_str)
                
                if date_match:
                    month_str = date_match.group(1)
                    day = date_match.group(2).zfill(2)
                    year = date_match.group(3)
                    
                    months = {
                        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                    }
                    
                    month_num = months.get(month_str)
                    if not month_num:
                        print(f"  ❌ Unknown month: {month_str}")
                        return None
                        
                    actual_date = f"{year}{month_num}{day}"
                    print(f"  ✅ Extracted date: {actual_date}")
                    
                    # Cache the result
                    self.game_dates_cache[game_id] = actual_date
                    return actual_date
            
            # Try alternative pattern in meta description
            desc_match = re.search(r'from\s+([^"]+)\s+on ESPN', html_content)
            if desc_match:
                date_str = desc_match.group(1).strip()
                print(f"  Found fallback date: '{date_str}'")
                # Could parse this too, but keeping simple for now
                
            print(f"  ❌ No date found in page for {game_id}")
            return None
            
        except Exception as e:
            print(f"  ❌ Error fetching {game_id}: {e}")
            return None
    
    def parse_filename(self, filename):
        """Parse CSV filename to extract components"""
        # Pattern: nfl_TEAM_category_weekX_YYYYMMDD_gameID.csv
        # Examples:
        # nfl_WAS_passing_week1_20250909_401547406.csv
        # nfl_SF_francisco passing_week5_20250909_401547354.csv
        
        pattern = r'nfl_([^_]+(?:_[^_]*)*?)_([^_]+(?:\s[^_]*)*?)_week(\d+)_(\d{8})_(\d+)\.csv'
        match = re.match(pattern, filename)
        
        if match:
            return {
                'team': match.group(1),
                'category': match.group(2),
                'week': match.group(3),
                'current_date': match.group(4),  # Wrong date (scraper run date)
                'game_id': match.group(5),
                'extension': '.csv'
            }
        
        print(f"  ❌ Could not parse filename: {filename}")
        return None
    
    def create_new_filename(self, parsed, actual_date):
        """Create new filename with correct date"""
        return f"nfl_{parsed['team']}_{parsed['category']}_week{parsed['week']}_{actual_date}_{parsed['game_id']}.csv"
    
    def fix_single_file(self, filename):
        """Fix a single CSV file"""
        print(f"\n📁 Processing: {filename}")
        
        parsed = self.parse_filename(filename)
        if not parsed:
            self.failed_files.append((filename, "Could not parse filename"))
            return False
            
        # Check if filename is already correct (unlikely but possible)
        current_date = parsed['current_date']
        if not (current_date.startswith('20250909') or current_date.startswith('20250910')):
            print(f"  ✅ Already has correct date format: {current_date}")
            self.skip_count += 1
            return True
            
        # Extract actual game date
        game_id = parsed['game_id']
        actual_date = self.extract_game_date_from_espn(game_id)
        
        if not actual_date:
            self.failed_files.append((filename, f"Could not extract date for game {game_id}"))
            return False
            
        # Check if dates are different
        if current_date == actual_date:
            print(f"  ✅ Dates already match: {actual_date}")
            self.skip_count += 1
            return True
            
        # Create new filename
        new_filename = self.create_new_filename(parsed, actual_date)
        
        # Rename the file
        old_path = self.csv_dir / filename
        new_path = self.csv_dir / new_filename
        
        try:
            if new_path.exists():
                print(f"  ⚠️  Target file already exists: {new_filename}")
                
                # Check file sizes to decide what to do
                old_size = old_path.stat().st_size
                new_size = new_path.stat().st_size
                
                print(f"    Old file size: {old_size} bytes")
                print(f"    Existing file size: {new_size} bytes")
                
                if old_size == new_size:
                    # Files are identical size - safe to delete duplicate
                    old_path.unlink()
                    print(f"  🗑️  Deleted duplicate: {filename} (same size as existing)")
                    self.success_count += 1
                    return True
                elif old_size > new_size:
                    # Old file is larger - replace the existing file
                    new_path.unlink()
                    old_path.rename(new_path)
                    print(f"  🔄 Replaced with larger file: {filename} → {new_filename}")
                    self.success_count += 1
                    return True
                else:
                    # Existing file is larger - delete the smaller old file
                    old_path.unlink()
                    print(f"  🗑️  Deleted smaller duplicate: {filename}")
                    self.success_count += 1
                    return True
                
            old_path.rename(new_path)
            print(f"  ✅ Renamed: {filename} → {new_filename}")
            self.success_count += 1
            return True
            
        except Exception as e:
            self.failed_files.append((filename, f"Rename failed: {e}"))
            print(f"  ❌ Rename failed: {e}")
            return False
    
    def run_batch_fix(self, limit=None, start_from=None):
        """Fix all CSV files in the directory"""
        print("🔧 Starting CSV filename correction...")
        print(f"📂 Processing files in: {self.csv_dir}")
        
        # Get all CSV files
        csv_files = [f for f in os.listdir(self.csv_dir) if f.endswith('.csv')]
        csv_files.sort()  # Process in consistent order
        
        total_files = len(csv_files)
        print(f"📊 Found {total_files} CSV files")
        
        if start_from:
            try:
                start_index = csv_files.index(start_from)
                csv_files = csv_files[start_index:]
                print(f"🔄 Starting from file: {start_from} (index {start_index})")
            except ValueError:
                print(f"⚠️  Start file not found: {start_from}")
        
        if limit:
            csv_files = csv_files[:limit]
            print(f"📏 Limited to first {limit} files")
        
        print(f"🎯 Will process {len(csv_files)} files")
        
        # Process each file
        for i, filename in enumerate(csv_files, 1):
            print(f"\n[{i}/{len(csv_files)}] ===============================")
            
            success = self.fix_single_file(filename)
            
            # Add delay to avoid overwhelming ESPN
            if i < len(csv_files):  # Don't delay after last file
                delay = random.uniform(0.5, 1.5)
                print(f"  ⏳ Waiting {delay:.1f}s...")
                time.sleep(delay)
        
        # Summary
        print("\n" + "="*60)
        print("🏁 BATCH FIX COMPLETE")
        print("="*60)
        print(f"✅ Successfully fixed: {self.success_count}")
        print(f"⏩ Skipped (already correct): {self.skip_count}")
        print(f"❌ Failed: {len(self.failed_files)}")
        print(f"📊 Total processed: {self.success_count + self.skip_count + len(self.failed_files)}")
        
        if self.failed_files:
            print(f"\n❌ Failed files:")
            for filename, reason in self.failed_files[:10]:  # Show first 10
                print(f"  - {filename}: {reason}")
            if len(self.failed_files) > 10:
                print(f"  ... and {len(self.failed_files) - 10} more")
        
        # Save cache for next run
        if self.game_dates_cache:
            cache_file = self.csv_dir / "game_dates_cache.json"
            with open(cache_file, 'w') as f:
                json.dump(self.game_dates_cache, f, indent=2)
            print(f"💾 Saved {len(self.game_dates_cache)} game dates to cache")
    
    def load_cache(self):
        """Load previously cached game dates"""
        cache_file = self.csv_dir / "game_dates_cache.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    self.game_dates_cache = json.load(f)
                print(f"💾 Loaded {len(self.game_dates_cache)} cached game dates")
            except Exception as e:
                print(f"⚠️  Could not load cache: {e}")

def main():
    fixer = CSVFileFixer()
    
    # Load any existing cache
    fixer.load_cache()
    
    print("🔧 CSV Filename Fixer")
    print("=====================")
    print("This will rename CSV files from scraper dates to actual game dates")
    print("Example: nfl_WAS_passing_week1_20250909_401547406.csv")
    print("      -> nfl_WAS_passing_week1_20230910_401547406.csv")
    print()
    
    # Test with a few files first
    while True:
        print("Options:")
        print("1. Test with 5 files first")
        print("2. Process all files (~13,406 files)")
        print("3. Process specific number of files")
        print("4. Resume from specific file")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            fixer.run_batch_fix(limit=5)
            break
        elif choice == "2":
            confirm = input("⚠️  This will process ~13,406 files. Continue? (yes/no): ").strip().lower()
            if confirm == "yes":
                fixer.run_batch_fix()
            break
        elif choice == "3":
            try:
                limit = int(input("How many files? "))
                fixer.run_batch_fix(limit=limit)
            except ValueError:
                print("Invalid number")
                continue
            break
        elif choice == "4":
            filename = input("Start from filename: ").strip()
            fixer.run_batch_fix(start_from=filename)
            break
        elif choice == "5":
            print("Exiting...")
            return 0
        else:
            print("Invalid choice")
            continue
    
    return 0

if __name__ == "__main__":
    exit(main())