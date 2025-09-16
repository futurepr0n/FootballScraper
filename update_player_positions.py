#!/usr/bin/env python3
"""
Update player positions in database from scraped roster data
"""

import csv
import psycopg2
from pathlib import Path

# Database connection
conn = psycopg2.connect(
    host="192.168.1.23",
    database="football_tracker",
    user="postgres",
    password="korn5676"
)
cur = conn.cursor()

# Read roster CSV
roster_file = Path("../FootballData/rosters/nfl_rosters_all_20250915_140750.csv")

with open(roster_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    updates = 0
    not_found = 0
    
    for row in reader:
        team = row['team']
        name = row['name']
        position = row['position']
        
        if not position or position == 'Unknown':
            continue
            
        # First try to get team_id
        cur.execute("SELECT id FROM teams WHERE abbreviation = %s", (team,))
        team_result = cur.fetchone()
        
        if not team_result:
            print(f"Team not found: {team}")
            continue
            
        team_id = team_result[0]
        
        # Truncate position to 10 chars max (DB constraint)
        position = position[:10] if len(position) > 10 else position
        
        # Update player position
        cur.execute("""
            UPDATE players 
            SET position = %s
            WHERE name = %s AND team_id = %s
        """, (position, name, team_id))
        
        if cur.rowcount > 0:
            updates += 1
            print(f"Updated {name} ({team}) to position {position}")
        else:
            # Try without team constraint for players who might have moved
            cur.execute("""
                UPDATE players 
                SET position = %s
                WHERE name = %s
            """, (position, name))
            
            if cur.rowcount > 0:
                updates += 1
                print(f"Updated {name} to position {position} (any team)")
            else:
                not_found += 1
                print(f"Player not found: {name} ({team})")

# Commit changes
conn.commit()

print(f"\n✅ Position update complete!")
print(f"Updated: {updates} players")
print(f"Not found: {not_found} players")

# Verify some key players
print("\nVerifying key players:")
test_players = [
    'Jayden Daniels',
    'Baker Mayfield', 
    'Brian Robinson Jr.',
    'Terry McLaurin'
]

for player in test_players:
    cur.execute("""
        SELECT p.name, p.position, t.abbreviation 
        FROM players p
        JOIN teams t ON p.team_id = t.id
        WHERE p.name = %s
    """, (player,))
    result = cur.fetchone()
    if result:
        print(f"  {result[0]}: {result[1]} ({result[2]})")

cur.close()
conn.close()