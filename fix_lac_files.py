#!/usr/bin/env python3
"""
Fix LAC vs LAR confusion for game 401772714
Game 401772714 was LAC @ KC (Chargers @ Chiefs) not LAR (Rams)
"""

import os
from pathlib import Path

def fix_lac_files():
    """Rename LAR files to LAC for game 401772714"""
    
    boxscore_dir = Path(__file__).parent.parent / 'FootballData' / 'BOXSCORE_CSV'
    
    if not boxscore_dir.exists():
        print(f"Directory not found: {boxscore_dir}")
        return
    
    fixed_count = 0
    
    # Find all LAR files for game 401772714
    for csv_file in boxscore_dir.glob("nfl_LAR_*_401772714.csv"):
        old_name = csv_file.name
        new_name = old_name.replace('nfl_LAR_', 'nfl_LAC_')
        new_path = csv_file.parent / new_name
        
        csv_file.rename(new_path)
        print(f"Fixed: {old_name} -> {new_name}")
        fixed_count += 1
    
    print(f"\nFixed {fixed_count} files from LAR to LAC for game 401772714")
    
    # Verify LAC files exist
    lac_files = list(boxscore_dir.glob("nfl_LAC_*.csv"))
    print(f"\nTotal LAC files: {len(lac_files)}")
    
    # Check teams
    teams = set()
    for csv_file in boxscore_dir.glob("nfl_*_week1_*.csv"):
        parts = csv_file.name.split('_')
        if len(parts) >= 2:
            teams.add(parts[1])
    
    print(f"Total unique teams: {len(teams)}")
    print(f"Teams: {sorted(teams)}")
    
    # Should be 32 teams for Week 1
    if len(teams) == 32:
        print("\n✅ All 32 NFL teams are now present!")
    else:
        missing = {'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 
                  'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC',
                  'LAC', 'LAR', 'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG', 
                  'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WSH'} - teams
        if missing:
            print(f"Missing teams: {sorted(missing)}")

if __name__ == "__main__":
    fix_lac_files()