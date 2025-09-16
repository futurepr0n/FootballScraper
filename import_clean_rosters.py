#!/usr/bin/env python3
"""
Import clean NFL roster data from CSV to database
"""

import csv
import psycopg2
from pathlib import Path
from datetime import datetime

# Database connection
conn = psycopg2.connect(
    host="192.168.1.23",
    database="football_tracker",
    user="postgres",
    password="korn5676"
)
cur = conn.cursor()

# Clear existing data
print("Clearing existing nfl_rosters table...")
cur.execute("TRUNCATE TABLE nfl_rosters RESTART IDENTITY CASCADE")
conn.commit()

# Read the clean roster CSV
roster_file = Path("../FootballData/rosters/nfl_rosters_all_20250915_182000.csv")
print(f"Reading roster data from {roster_file}")

with open(roster_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    total = 0
    errors = 0
    
    for row in reader:
        try:
            # Extract data
            team = row['team']
            name = row['name']
            jersey = row['jersey'] if row['jersey'] else None
            position = row['position'] if row['position'] else None
            
            # Parse age
            age = None
            if row['age'] and row['age'].isdigit():
                age = int(row['age'])
            
            height = row['height'] if row['height'] else None
            
            # Parse weight (already cleaned in scraper)
            weight = None
            if row['weight'] and row['weight'].isdigit():
                weight = int(row['weight'])
            
            # Parse experience
            experience = 0
            if row['experience']:
                if row['experience'] == 'R':
                    experience = 0
                elif row['experience'].isdigit():
                    experience = int(row['experience'])
            
            college = row['college'] if row['college'] else None
            roster_section = row['roster_section'] if row['roster_section'] else None
            
            # Limit position to 10 characters
            if position and len(position) > 10:
                position = position[:10]
            
            # Insert into database
            cur.execute("""
                INSERT INTO nfl_rosters (
                    team, name, jersey, position, age, height, weight, 
                    experience, college, roster_section, scraped_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name, team) DO UPDATE SET
                    jersey = EXCLUDED.jersey,
                    position = EXCLUDED.position,
                    age = EXCLUDED.age,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    experience = EXCLUDED.experience,
                    college = EXCLUDED.college,
                    roster_section = EXCLUDED.roster_section,
                    scraped_date = EXCLUDED.scraped_date,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                team, name, jersey, position, age, height, weight,
                experience, college, roster_section, datetime.now()
            ))
            
            total += 1
            if total % 100 == 0:
                print(f"  Processed {total} players...")
                
        except Exception as e:
            errors += 1
            print(f"Error processing {row.get('name', 'unknown')}: {e}")
            continue

# Commit all changes
conn.commit()

print(f"\n✅ Import complete!")
print(f"Successfully imported: {total} players")
print(f"Errors: {errors}")

# Verify the import
cur.execute("SELECT COUNT(*) FROM nfl_rosters")
count = cur.fetchone()[0]
print(f"Total players in nfl_rosters table: {count}")

# Show sample by team
print("\nPlayers per team:")
cur.execute("""
    SELECT team, COUNT(*) as player_count
    FROM nfl_rosters
    GROUP BY team
    ORDER BY team
""")

for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} players")

# Show sample of QBs
print("\nSample QBs:")
cur.execute("""
    SELECT team, name, jersey, position
    FROM nfl_rosters
    WHERE position = 'QB'
    ORDER BY team, name
    LIMIT 15
""")

for row in cur.fetchall():
    print(f"  {row[0]}: #{row[2] or 'XX'} {row[1]} - {row[3]}")

# Update players table positions from roster data
print("\nUpdating players table with positions from roster data...")
cur.execute("""
    UPDATE players p
    SET position = r.position
    FROM nfl_rosters r
    JOIN teams t ON t.abbreviation = r.team
    WHERE p.name = r.name 
    AND p.team_id = t.id
    AND r.position IS NOT NULL
    AND r.position != ''
""")

updated = cur.rowcount
conn.commit()
print(f"Updated {updated} player positions in players table")

cur.close()
conn.close()