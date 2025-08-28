#!/usr/bin/env python3
"""
Quick script to collect specific missing games by ID
"""
import requests
import json
from pathlib import Path
import time

# Missing game IDs and their dates - Aug 23 games
MISSING_GAMES = {
    '401774590': '2025-08-23',  # Las Vegas Raiders at Arizona Cardinals
    '401777124': '2025-08-23',  # Baltimore Ravens at Washington Commanders
    '401772998': '2025-08-23',  # Indianapolis Colts at Cincinnati Bengals
    '401772999': '2025-08-23',  # Los Angeles Rams at Cleveland Browns
    '401776265': '2025-08-23',  # Houston Texans at Detroit Lions
    '401773015': '2025-08-23',  # Denver Broncos at New Orleans Saints
    '401773011': '2025-08-23',  # Seattle Seahawks at Green Bay Packers
    '401776565': '2025-08-23',  # Jacksonville Jaguars at Miami Dolphins
    '401774030': '2025-08-23',  # Buffalo Bills at Tampa Bay Buccaneers
    '401776567': '2025-08-23',  # Los Angeles Chargers at San Francisco 49ers
}

def collect_game_data(game_id, game_date):
    """Collect comprehensive game data for a specific game ID"""
    print(f"Collecting game {game_id} for {game_date}...")
    
    # ESPN API URLs for this game
    box_score_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}"
    play_by_play_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/playbyplay?event={game_id}"
    
    try:
        # Get box score data
        print(f"  Fetching box score...")
        box_response = requests.get(box_score_url)
        box_response.raise_for_status()
        box_data = box_response.json()
        
        time.sleep(1)  # Rate limiting
        
        # Get play by play data
        print(f"  Fetching play by play...")
        pbp_response = requests.get(play_by_play_url)
        pbp_response.raise_for_status()
        pbp_data = pbp_response.json()
        
        # Create comprehensive data structure
        comprehensive_data = {
            "game_id": game_id,
            "date": game_date,
            "box_score": box_data,
            "play_by_play": pbp_data,
            "processing_timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%f")
        }
        
        # Save to comprehensive file
        output_dir = Path("../FootballData/data/preseason/comprehensive")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{game_date}_{game_id}_complete.json"
        
        with open(output_file, 'w') as f:
            json.dump(comprehensive_data, f, indent=2)
        
        print(f"  ✅ Saved to {output_file}")
        return True
        
    except Exception as e:
        print(f"  ❌ Error collecting game {game_id}: {e}")
        return False

def main():
    """Collect all missing games"""
    print("Collecting missing NFL games...")
    
    success_count = 0
    total_count = len(MISSING_GAMES)
    
    for game_id, game_date in MISSING_GAMES.items():
        if collect_game_data(game_id, game_date):
            success_count += 1
        time.sleep(2)  # Rate limiting between games
    
    print(f"\nCollection complete: {success_count}/{total_count} games collected successfully")

if __name__ == "__main__":
    main()