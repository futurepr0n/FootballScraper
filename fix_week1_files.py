#!/usr/bin/env python3
"""
Fix Week 1 2025 NFL files with correct dates and team names
"""

import os
import shutil
from pathlib import Path

# Correct game dates for Week 1 2025
GAME_DATES = {
    '401772714': '20250905',  # LAC @ KC (Friday in Brazil)
    '401772721': '20250907',  # PIT @ NYJ (Sunday) 
    '401772918': '20250907',  # BAL @ BUF (Sunday)
    '401772830': '20250907',  # ATL @ TB (Sunday)
    '401772722': '20250907',  # DET @ GB (Sunday)
    '401772810': '20250908',  # MIN @ CHI (Monday Night)
    '401772829': '20250907',  # CLE @ CIN (Sunday)
    '401772828': '20250907',  # JAX @ CAR (Sunday)
    '401772723': '20250907',  # HOU @ LAC (Sunday)
    '401772719': '20250907',  # IND @ MIA (Sunday)
    '401772718': '20250907',  # ARI @ NE (Sunday)
    '401772720': '20250907',  # NYG @ DAL (Sunday)
    '401772831': '20250907',  # SEA @ SF (Sunday)
    '401772832': '20250907',  # DEN @ NO (Sunday)
    '401772510': '20250904',  # PHI @ DAL (Thursday Kickoff)
    '401772827': '20250907',  # WAS @ NYG (Sunday)
    '401772971': '20250908',  # LAR @ DET (Monday Night)
}

# NYG games should be NYJ for this specific game
NYJ_GAME = '401772721'  # PIT @ NYJ

def fix_files():
    """Fix all Week 1 files with correct dates and team names"""
    
    boxscore_dir = Path(__file__).parent.parent / 'FootballData' / 'BOXSCORE_CSV'
    
    if not boxscore_dir.exists():
        print(f"Directory not found: {boxscore_dir}")
        return
    
    fixed_count = 0
    
    for csv_file in boxscore_dir.glob("*.csv"):
        filename = csv_file.name
        
        # Skip if not a week 1 file
        if "week1" not in filename:
            continue
            
        # Extract game ID from filename
        parts = filename.split('_')
        if len(parts) >= 5:
            game_id = parts[-1].replace('.csv', '')
            
            # Check if we need to fix the date
            if game_id in GAME_DATES:
                correct_date = GAME_DATES[game_id]
                
                # Check if file has wrong date (20250913 or UNKNOWN_DATE)
                if '20250913' in filename or 'UNKNOWN_DATE' in filename:
                    # Build new filename with correct date
                    new_parts = parts[:-2] + [correct_date, game_id + '.csv']
                    new_filename = '_'.join(new_parts)
                    
                    # Also fix NYG -> NYJ for game 401772721
                    if game_id == NYJ_GAME and 'NYG' in new_filename:
                        new_filename = new_filename.replace('nfl_NYG_', 'nfl_NYJ_')
                    
                    new_path = boxscore_dir / new_filename
                    
                    # Rename the file
                    if not new_path.exists():
                        shutil.move(str(csv_file), str(new_path))
                        print(f"Fixed: {filename} -> {new_filename}")
                        fixed_count += 1
                    else:
                        # File with correct name already exists, remove the wrong one
                        os.remove(str(csv_file))
                        print(f"Removed duplicate: {filename}")
                        fixed_count += 1
                        
                # Special case: Fix NYG -> NYJ for game 401772721 even if date is correct
                elif game_id == NYJ_GAME and 'NYG' in filename:
                    new_filename = filename.replace('nfl_NYG_', 'nfl_NYJ_')
                    new_path = boxscore_dir / new_filename
                    
                    if not new_path.exists():
                        shutil.move(str(csv_file), str(new_path))
                        print(f"Fixed team: {filename} -> {new_filename}")
                        fixed_count += 1
    
    print(f"\nFixed {fixed_count} files")
    
    # Show summary of teams
    teams = set()
    for csv_file in boxscore_dir.glob("nfl_*_week1_*.csv"):
        parts = csv_file.name.split('_')
        if len(parts) >= 2:
            teams.add(parts[1])
    
    print(f"\nTeams found in Week 1 files: {sorted(teams)}")
    
    # Check for NYJ
    nyj_files = list(boxscore_dir.glob("nfl_NYJ_*.csv"))
    print(f"\nNYJ files: {len(nyj_files)}")
    if nyj_files:
        for f in nyj_files[:3]:
            print(f"  - {f.name}")

if __name__ == "__main__":
    fix_files()