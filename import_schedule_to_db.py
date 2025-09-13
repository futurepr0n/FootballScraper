#!/usr/bin/env python3
"""
Import NFL schedule from generated JSON files to PostgreSQL database
"""

import json
import psycopg2
import glob
import os
from datetime import datetime

# Database connection
DB_CONFIG = {
    'host': '192.168.1.23',
    'database': 'football_tracker',
    'user': 'postgres',
    'password': 'korn5676'
}

def get_team_id(cursor, team_abbr):
    """Get team ID from abbreviation"""
    cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr,))
    result = cursor.fetchone()
    return result[0] if result else None

def import_schedule_files():
    """Import all regular season schedule files"""
    
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Find all regular season summary files
        summary_files = glob.glob("regular_week*_2025_summary.json")
        summary_files.sort()
        
        total_imported = 0
        total_skipped = 0
        
        for file_path in summary_files:
            print(f"Processing {file_path}...")
            
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            for game in data['games']:
                game_id = game['game_id']
                home_team = game['home_team']
                away_team = game['away_team']
                date = game['date']
                week = game['week']
                
                # Get team IDs
                home_team_id = get_team_id(cursor, home_team)
                away_team_id = get_team_id(cursor, away_team)
                
                if not home_team_id or not away_team_id:
                    print(f"  Skipping {game_id}: Unknown teams {home_team}/{away_team}")
                    total_skipped += 1
                    continue
                
                # Check if game already exists
                cursor.execute("SELECT id FROM games WHERE game_id = %s", (game_id,))
                if cursor.fetchone():
                    print(f"  Skipping {game_id}: Already exists")
                    total_skipped += 1
                    continue
                
                # Insert game
                try:
                    cursor.execute("""
                        INSERT INTO games (game_id, home_team_id, away_team_id, date, week, season, season_type, completed)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (game_id, home_team_id, away_team_id, date, week, 2025, 'regular', False))
                    
                    print(f"  ✅ Imported {game_id}: {away_team} @ {home_team}")
                    total_imported += 1
                    
                except Exception as e:
                    print(f"  ❌ Error importing {game_id}: {e}")
                    total_skipped += 1
        
        # Commit all changes
        conn.commit()
        print(f"\n🎯 Import complete!")
        print(f"   Imported: {total_imported} games")
        print(f"   Skipped: {total_skipped} games")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import_schedule_files()