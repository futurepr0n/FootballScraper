#!/usr/bin/env python3
"""
Simple ESPN NFL Roster Scraper
Extracts roster data from ESPN team pages
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
from pathlib import Path
from datetime import datetime
import time
import re

class SimpleRosterScraper:
    def __init__(self):
        self.base_url = "https://www.espn.com"
        self.output_dir = Path("../FootballData/rosters")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # NFL team abbreviations
        self.teams = [
            'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
            'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC',
            'LAC', 'LAR', 'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
            'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS'
        ]
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def parse_name_and_jersey(self, text):
        """Parse combined name and jersey like 'Josh Allen17' -> ('Josh Allen', '17')"""
        # Use regex to split name and number
        match = re.match(r'^(.+?)(\d+)$', text.strip())
        if match:
            return match.group(1).strip(), match.group(2)
        else:
            return text.strip(), None

    def scrape_team_roster(self, team_abbr):
        """Scrape roster for a specific team"""
        roster_url = f"{self.base_url}/nfl/team/roster/_/name/{team_abbr.lower()}"
        print(f"Scraping {team_abbr} from: {roster_url}")
        
        try:
            response = self.session.get(roster_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            players = []
            
            # Find all tables (offense, defense, special teams, etc.)
            tables = soup.find_all('table')
            
            for table_idx, table in enumerate(tables):
                # Determine section based on table index or nearby headers
                section = 'Unknown'
                if table_idx == 0:
                    section = 'Offense'
                elif table_idx == 1:
                    section = 'Defense'
                elif table_idx == 2:
                    section = 'Special Teams'
                elif table_idx >= 3:
                    section = 'Practice Squad'
                
                # Process rows in this table
                rows = table.find_all('tr')
                
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    
                    if len(cells) >= 7:  # Ensure we have enough data
                        # Column 1: Name with jersey number
                        name_text = cells[1].get_text(strip=True)
                        name, jersey = self.parse_name_and_jersey(name_text)
                        
                        # Skip if no valid name
                        if not name or name.lower() in ['name', 'player']:
                            continue
                        
                        # Other columns
                        position = cells[2].get_text(strip=True) if len(cells) > 2 else None
                        age = cells[3].get_text(strip=True) if len(cells) > 3 else None
                        height = cells[4].get_text(strip=True) if len(cells) > 4 else None
                        weight = cells[5].get_text(strip=True) if len(cells) > 5 else None
                        experience = cells[6].get_text(strip=True) if len(cells) > 6 else None
                        college = cells[7].get_text(strip=True) if len(cells) > 7 else None
                        
                        # Clean up weight (remove 'lbs')
                        if weight:
                            weight = weight.replace(' lbs', '').strip()
                        
                        # Create player record
                        player = {
                            'team': team_abbr,
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
            
            print(f"  Found {len(players)} players for {team_abbr}")
            return players
            
        except Exception as e:
            print(f"Error scraping {team_abbr}: {e}")
            return []

    def scrape_all_teams(self):
        """Scrape rosters for all NFL teams"""
        all_players = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for team in self.teams:
            players = self.scrape_team_roster(team)
            
            if players:
                all_players.extend(players)
                
                # Save individual team CSV
                team_file = self.output_dir / f"nfl_roster_{team}_{timestamp}.csv"
                self.save_to_csv(players, team_file)
            
            # Be nice to ESPN's servers
            time.sleep(2)
        
        # Save master CSV
        master_file = self.output_dir / f"nfl_rosters_all_{timestamp}.csv"
        self.save_to_csv(all_players, master_file)
        
        # Save JSON version
        json_file = self.output_dir / f"nfl_rosters_all_{timestamp}.json"
        self.save_to_json(all_players, json_file)
        
        print(f"\n{'='*60}")
        print(f"Scraping complete!")
        print(f"Total players: {len(all_players)}")
        print(f"Master CSV: {master_file}")
        print(f"Master JSON: {json_file}")
        
        return all_players, master_file

    def save_to_csv(self, players, filename):
        """Save player data to CSV"""
        if not players:
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['team', 'name', 'jersey', 'position', 'age', 'height', 
                         'weight', 'experience', 'college', 'roster_section']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(players)

    def save_to_json(self, players, filename):
        """Save player data to JSON"""
        if not players:
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(players, f, indent=2)

def main():
    scraper = SimpleRosterScraper()
    
    # Test with one team first
    print("Testing with Buffalo Bills first...")
    players = scraper.scrape_team_roster('BUF')
    
    if players:
        print(f"\nSample players from BUF:")
        for player in players[:5]:
            print(f"  #{player['jersey'] or 'XX':>2} {player['name']:<20} {player['position']:<5} {player['height']:<6} {player['weight']:<3} lbs")
        
        print(f"\nTest successful! Found {len(players)} players.")
        print("\nProceeding with full scrape...")
        all_players, csv_file = scraper.scrape_all_teams()
        return csv_file
    else:
        print("No players found in test. Check scraping logic.")
        return None

if __name__ == "__main__":
    csv_file = main()
    if csv_file:
        print(f"\nCSV file created: {csv_file}")