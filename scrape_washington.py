#!/usr/bin/env python3
"""
Scrape Washington Commanders roster
"""

import requests
from bs4 import BeautifulSoup
import csv
import re
from datetime import datetime
from pathlib import Path

def parse_name_and_jersey(text):
    """Parse combined name and jersey like 'Jayden Daniels5' -> ('Jayden Daniels', '5')"""
    match = re.match(r'^(.+?)(\d+)$', text.strip())
    if match:
        return match.group(1).strip(), match.group(2)
    else:
        return text.strip(), None

url = 'https://www.espn.com/nfl/team/roster/_/name/wsh'
response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(response.content, 'html.parser')

players = []
tables = soup.find_all('table')

for table_idx, table in enumerate(tables):
    section = 'Unknown'
    if table_idx == 0:
        section = 'Offense'
    elif table_idx == 1:
        section = 'Defense'
    elif table_idx == 2:
        section = 'Special Teams'
    elif table_idx >= 3:
        section = 'Practice Squad'
    
    rows = table.find_all('tr')
    
    for row in rows[1:]:
        cells = row.find_all('td')
        
        if len(cells) >= 7:
            name_text = cells[1].get_text(strip=True)
            name, jersey = parse_name_and_jersey(name_text)
            
            if not name or name.lower() in ['name', 'player']:
                continue
            
            position = cells[2].get_text(strip=True) if len(cells) > 2 else None
            age = cells[3].get_text(strip=True) if len(cells) > 3 else None
            height = cells[4].get_text(strip=True) if len(cells) > 4 else None
            weight = cells[5].get_text(strip=True) if len(cells) > 5 else None
            experience = cells[6].get_text(strip=True) if len(cells) > 6 else None
            college = cells[7].get_text(strip=True) if len(cells) > 7 else None
            
            if weight:
                weight = weight.replace(' lbs', '').strip()
            
            player = {
                'team': 'WAS',
                'name': name,
                'jersey': jersey,
                'position': position,
                'age': age,
                'height': height,
                'weight': weight,
                'experience': experience,
                'college': college,
                'roster_section': section
            }
            
            players.append(player)

print(f"Found {len(players)} players for Washington")

# Save to CSV
output_dir = Path("../FootballData/rosters")
output_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = output_dir / f"nfl_roster_WAS_{timestamp}.csv"

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['team', 'name', 'jersey', 'position', 'age', 'height', 
                 'weight', 'experience', 'college', 'roster_section']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(players)

print(f"Saved to {output_file}")

# Show QBs
print("\nWashington QBs:")
for p in players:
    if p['position'] == 'QB':
        print(f"  #{p['jersey'] or 'XX'} {p['name']} - {p['position']}")

# Import to database
import psycopg2

conn = psycopg2.connect(
    host="192.168.1.23",
    database="football_tracker",
    user="postgres",
    password="korn5676"
)
cur = conn.cursor()

for player in players:
    try:
        age = None
        if player['age'] and player['age'].isdigit():
            age = int(player['age'])
        
        weight = None
        if player['weight'] and player['weight'].isdigit():
            weight = int(player['weight'])
        
        experience = 0
        if player['experience']:
            if player['experience'] == 'R':
                experience = 0
            elif player['experience'].isdigit():
                experience = int(player['experience'])
        
        position = player['position']
        if position and len(position) > 10:
            position = position[:10]
        
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
            player['team'], player['name'], player['jersey'], position, age, 
            player['height'], weight, experience, player['college'], 
            player['roster_section'], datetime.now()
        ))
    except Exception as e:
        print(f"Error inserting {player['name']}: {e}")

conn.commit()
print(f"\nInserted {len(players)} Washington players to database")

# Update players table
cur.execute("""
    UPDATE players p
    SET position = r.position
    FROM nfl_rosters r
    JOIN teams t ON t.abbreviation = r.team
    WHERE p.name = r.name 
    AND p.team_id = t.id
    AND r.position IS NOT NULL
    AND r.position != ''
    AND r.team = 'WAS'
""")

updated = cur.rowcount
conn.commit()
print(f"Updated {updated} player positions in players table")

cur.close()
conn.close()