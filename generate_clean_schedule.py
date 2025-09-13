#!/usr/bin/env python3
"""
Generate clean regular season schedule files from database
"""

import psycopg2
import json
from collections import defaultdict

# Database connection
DB_CONFIG = {
    'host': '192.168.1.23',
    'database': 'football_tracker',
    'user': 'postgres',
    'password': 'korn5676'
}

def generate_clean_regular_season():
    """Generate clean regular season schedule files from database"""
    
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Get all regular season games for 2025, ordered by week and date
        cursor.execute("""
            SELECT 
                g.game_id,
                ht.abbreviation as home_team,
                at.abbreviation as away_team,
                g.date,
                g.week
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.season = 2025 
            AND g.season_type = 'regular'
            ORDER BY g.week, g.date
        """)
        
        games = cursor.fetchall()
        
        if not games:
            print("❌ No regular season games found in database")
            return
        
        # Group games by week
        weekly_games = defaultdict(list)
        
        for game_id, home_team, away_team, date, week in games:
            weekly_games[week].append({
                'game_id': game_id,
                'home_team': home_team,
                'away_team': away_team,
                'date': str(date),
                'week': week,
                'matchup': f"{away_team} @ {home_team}",
                'url': f"https://www.espn.com/nfl/game/_/gameId/{game_id}"
            })
        
        print(f"📊 Found {len(games)} games across {len(weekly_games)} weeks")
        
        # Generate files for each week
        for week in sorted(weekly_games.keys()):
            week_games = weekly_games[week]
            
            # Generate .txt file
            txt_filename = f"regular_week{week}_2025.txt"
            with open(txt_filename, 'w') as f:
                f.write(f"# NFL Regular Week {week}, 2025 Game URLs\n")
                f.write("# Format: One ESPN game URL per line\n")
                f.write(f"# Use: python process_nfl_game_file.py {txt_filename}\n")
                f.write("\n")
                f.write(f"# {len(week_games)} games scheduled for this week\n")
                
                for game in week_games:
                    f.write(f"# {game['matchup']} - {game['date']}\n")
                    f.write(f"{game['url']}\n")
            
            print(f"✅ Created {txt_filename} with {len(week_games)} games")
            
            # Generate summary JSON
            json_filename = f"regular_week{week}_2025_summary.json"
            summary = {
                'season_type': 'regular',
                'week': week,
                'year': 2025,
                'total_games': len(week_games),
                'generated_date': '2025-09-13T18:00:00.000000',
                'games': [
                    {
                        'url': game['url'],
                        'game_id': game['game_id'],
                        'date': game['date'] + 'T17:00:00Z',  # Add time format
                        'week': game['week'],
                        'season_type': 2,  # Regular season
                        'away_team': game['away_team'],
                        'home_team': game['home_team'],
                        'matchup': game['matchup']
                    }
                    for game in week_games
                ]
            }
            
            with open(json_filename, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"✅ Created {json_filename}")
        
        print(f"\n🎯 Regular season schedule generation complete!")
        print(f"📄 Generated {len(weekly_games)} weekly files")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    generate_clean_regular_season()