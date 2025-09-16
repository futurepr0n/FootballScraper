#!/usr/bin/env python3
"""
Import NFL roster data from CSV to database
Properly handles the nfl_rosters table
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

# First, clear the existing bad data
print("Clearing existing nfl_rosters table...")
cur.execute("TRUNCATE TABLE nfl_rosters RESTART IDENTITY CASCADE")
conn.commit()

# Read the master roster CSV
roster_file = Path("../FootballData/rosters/nfl_rosters_all_20250915_140750.csv")
print(f"Reading roster data from {roster_file}")

with open(roster_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    total = 0
    errors = 0
    
    for row in reader:
        try:
            # Extract and clean data
            team = row['team']
            
            # Handle malformed CSV where name is in position field
            if not row['name'] and row['position']:
                # Name and jersey are combined in position field like "Josh Allen17"
                import re
                match = re.match(r'(.+?)(\d+)$', row['position'])
                if match:
                    name = match.group(1).strip()
                    jersey = match.group(2)
                else:
                    name = row['position'].strip()
                    jersey = None
                    
                # Fields are shifted - position is in age, age in height, etc.
                position = row['age'].strip() if row['age'] else None
                age_str = row['height'].strip() if row['height'] else None
                height = row['weight'].strip() if row['weight'] else None
                weight_str = row['experience'].strip() if row['experience'] else None
                exp_str = row['college'].strip() if row['college'] else None
                college = None  # Not in shifted data
                image_url = row['image_url'].strip() if row['image_url'] else None
                roster_section = row['roster_section'].strip() if row['roster_section'] else None
                
                # Parse shifted age
                age = None
                if age_str and age_str.isdigit():
                    age = int(age_str)
                
                # Parse shifted weight
                weight = None
                if weight_str:
                    weight_str = weight_str.replace(' lbs', '').strip()
                    if weight_str.isdigit():
                        weight = int(weight_str)
                
                # Parse shifted experience
                experience = 0
                if exp_str:
                    if exp_str == 'R':
                        experience = 0
                    elif exp_str.isdigit():
                        experience = int(exp_str)
            else:
                name = row['name'].strip()
                jersey = row['jersey'].strip() if row['jersey'] else None
                position = row['position'].strip() if row['position'] else None
                
                # Parse age - handle non-numeric values
                age = None
                if row['age'] and row['age'].isdigit():
                    age = int(row['age'])
                
                height = row['height'].strip() if row['height'] else None
                
                # Parse weight - remove 'lbs' and convert to integer
                weight = None
                if row['weight']:
                    weight_str = row['weight'].replace(' lbs', '').strip()
                    if weight_str.isdigit():
                        weight = int(weight_str)
                
                # Parse experience - handle 'R' for rookie
                experience = 0
                if row['experience']:
                    if row['experience'] == 'R':
                        experience = 0
                    elif row['experience'].isdigit():
                        experience = int(row['experience'])
                
                college = row['college'].strip() if row['college'] else None
                image_url = row['image_url'].strip() if row['image_url'] else None
                roster_section = row['roster_section'].strip() if row['roster_section'] else None
            
            # Skip empty names
            if not name:
                continue
            
            # Limit position to 10 characters (database constraint)
            if position and len(position) > 10:
                position = position[:10]
            
            # Insert into database
            cur.execute("""
                INSERT INTO nfl_rosters (
                    team, name, jersey, position, age, height, weight, 
                    experience, college, image_url, roster_section, scraped_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name, team) DO UPDATE SET
                    jersey = EXCLUDED.jersey,
                    position = EXCLUDED.position,
                    age = EXCLUDED.age,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    experience = EXCLUDED.experience,
                    college = EXCLUDED.college,
                    image_url = EXCLUDED.image_url,
                    roster_section = EXCLUDED.roster_section,
                    scraped_date = EXCLUDED.scraped_date,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                team, name, jersey, position, age, height, weight,
                experience, college, image_url, roster_section, datetime.now()
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

# Show sample of imported data
print("\nSample of imported players:")
cur.execute("""
    SELECT team, name, position, jersey 
    FROM nfl_rosters 
    WHERE position IN ('QB', 'RB', 'WR')
    ORDER BY team, position, name
    LIMIT 10
""")

for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} - {row[2]} #{row[3]}")

# Also update the players table with correct positions from rosters
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