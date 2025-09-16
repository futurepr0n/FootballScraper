#!/usr/bin/env python3
"""
NFL Roster Scraper
Scrapes current NFL rosters from ESPN including player positions, physical stats, and images
Outputs to both CSV files and can be imported to database
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import os
from datetime import datetime
import time
import re
from pathlib import Path

class NFLRosterScraper:
    def __init__(self):
        self.base_url = "https://www.espn.com"
        self.teams_url = "https://www.espn.com/nfl/teams"
        self.output_dir = Path("../FootballData/rosters")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # NFL team mappings
        self.team_abbr_map = {
            'arizona-cardinals': 'ARI', 'atlanta-falcons': 'ATL', 'baltimore-ravens': 'BAL',
            'buffalo-bills': 'BUF', 'carolina-panthers': 'CAR', 'chicago-bears': 'CHI',
            'cincinnati-bengals': 'CIN', 'cleveland-browns': 'CLE', 'dallas-cowboys': 'DAL',
            'denver-broncos': 'DEN', 'detroit-lions': 'DET', 'green-bay-packers': 'GB',
            'houston-texans': 'HOU', 'indianapolis-colts': 'IND', 'jacksonville-jaguars': 'JAX',
            'kansas-city-chiefs': 'KC', 'las-vegas-raiders': 'LV', 'los-angeles-chargers': 'LAC',
            'los-angeles-rams': 'LAR', 'miami-dolphins': 'MIA', 'minnesota-vikings': 'MIN',
            'new-england-patriots': 'NE', 'new-orleans-saints': 'NO', 'new-york-giants': 'NYG',
            'new-york-jets': 'NYJ', 'philadelphia-eagles': 'PHI', 'pittsburgh-steelers': 'PIT',
            'san-francisco-49ers': 'SF', 'seattle-seahawks': 'SEA', 'tampa-bay-buccaneers': 'TB',
            'tennessee-titans': 'TEN', 'washington-commanders': 'WAS'
        }
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def get_team_roster_urls(self):
        """Get all team roster URLs from ESPN teams page"""
        print("Fetching team roster URLs...")
        response = self.session.get(self.teams_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        roster_urls = {}
        
        # Find all team links
        team_links = soup.find_all('a', class_='AnchorLink')
        
        for link in team_links:
            href = link.get('href', '')
            # Look for team name patterns in URLs
            if '/nfl/team/_/name/' in href:
                # Extract team abbreviation and name
                parts = href.split('/')
                if len(parts) >= 6:
                    team_abbr = parts[5].upper()
                    team_name = parts[6] if len(parts) > 6 else ''
                    
                    # Build roster URL
                    roster_url = f"{self.base_url}/nfl/team/roster/_/name/{parts[5]}/{team_name}"
                    roster_urls[team_abbr] = roster_url
                    print(f"  Found {team_abbr}: {roster_url}")
        
        # If we didn't find links dynamically, use known patterns
        if len(roster_urls) < 32:
            print("Using fallback URLs for all 32 teams...")
            roster_urls = {
                'ARI': 'https://www.espn.com/nfl/team/roster/_/name/ari/arizona-cardinals',
                'ATL': 'https://www.espn.com/nfl/team/roster/_/name/atl/atlanta-falcons',
                'BAL': 'https://www.espn.com/nfl/team/roster/_/name/bal/baltimore-ravens',
                'BUF': 'https://www.espn.com/nfl/team/roster/_/name/buf/buffalo-bills',
                'CAR': 'https://www.espn.com/nfl/team/roster/_/name/car/carolina-panthers',
                'CHI': 'https://www.espn.com/nfl/team/roster/_/name/chi/chicago-bears',
                'CIN': 'https://www.espn.com/nfl/team/roster/_/name/cin/cincinnati-bengals',
                'CLE': 'https://www.espn.com/nfl/team/roster/_/name/cle/cleveland-browns',
                'DAL': 'https://www.espn.com/nfl/team/roster/_/name/dal/dallas-cowboys',
                'DEN': 'https://www.espn.com/nfl/team/roster/_/name/den/denver-broncos',
                'DET': 'https://www.espn.com/nfl/team/roster/_/name/det/detroit-lions',
                'GB': 'https://www.espn.com/nfl/team/roster/_/name/gb/green-bay-packers',
                'HOU': 'https://www.espn.com/nfl/team/roster/_/name/hou/houston-texans',
                'IND': 'https://www.espn.com/nfl/team/roster/_/name/ind/indianapolis-colts',
                'JAX': 'https://www.espn.com/nfl/team/roster/_/name/jax/jacksonville-jaguars',
                'KC': 'https://www.espn.com/nfl/team/roster/_/name/kc/kansas-city-chiefs',
                'LV': 'https://www.espn.com/nfl/team/roster/_/name/lv/las-vegas-raiders',
                'LAC': 'https://www.espn.com/nfl/team/roster/_/name/lac/los-angeles-chargers',
                'LAR': 'https://www.espn.com/nfl/team/roster/_/name/lar/los-angeles-rams',
                'MIA': 'https://www.espn.com/nfl/team/roster/_/name/mia/miami-dolphins',
                'MIN': 'https://www.espn.com/nfl/team/roster/_/name/min/minnesota-vikings',
                'NE': 'https://www.espn.com/nfl/team/roster/_/name/ne/new-england-patriots',
                'NO': 'https://www.espn.com/nfl/team/roster/_/name/no/new-orleans-saints',
                'NYG': 'https://www.espn.com/nfl/team/roster/_/name/nyg/new-york-giants',
                'NYJ': 'https://www.espn.com/nfl/team/roster/_/name/nyj/new-york-jets',
                'PHI': 'https://www.espn.com/nfl/team/roster/_/name/phi/philadelphia-eagles',
                'PIT': 'https://www.espn.com/nfl/team/roster/_/name/pit/pittsburgh-steelers',
                'SF': 'https://www.espn.com/nfl/team/roster/_/name/sf/san-francisco-49ers',
                'SEA': 'https://www.espn.com/nfl/team/roster/_/name/sea/seattle-seahawks',
                'TB': 'https://www.espn.com/nfl/team/roster/_/name/tb/tampa-bay-buccaneers',
                'TEN': 'https://www.espn.com/nfl/team/roster/_/name/ten/tennessee-titans',
                'WAS': 'https://www.espn.com/nfl/team/roster/_/name/wsh/washington-commanders'
            }
        
        return roster_urls

    def scrape_team_roster(self, team_abbr, roster_url):
        """Scrape a single team's roster"""
        print(f"\nScraping {team_abbr} roster from {roster_url}")
        
        try:
            response = self.session.get(roster_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            players = []
            
            # Find roster tables (Offense, Defense, Special Teams)
            tables = soup.find_all('div', class_='ResponsiveTable')
            
            for table in tables:
                # Get section name (Offense, Defense, Special Teams)
                section_header = table.find_previous('div', class_='Table__Title')
                section = section_header.text if section_header else 'Unknown'
                
                # Find all player rows
                rows = table.find_all('tr', class_='Table__TR')
                
                for row in rows:
                    # Skip header rows
                    if row.find('th'):
                        continue
                    
                    cells = row.find_all('td')
                    if len(cells) < 7:  # Need at least 7 columns for valid data
                        continue
                    
                    try:
                        # Extract player data
                        player_cell = cells[0]
                        player_link = player_cell.find('a')
                        
                        if not player_link:
                            continue
                        
                        # Get player name
                        name = player_link.text.strip()
                        
                        # Get player image URL if available
                        img_tag = player_cell.find('img')
                        image_url = img_tag['src'] if img_tag else ''
                        
                        # Get jersey number
                        jersey_span = player_cell.find('span', class_='pl2')
                        jersey = jersey_span.text.strip() if jersey_span else ''
                        
                        # Extract other fields
                        position = cells[1].text.strip() if len(cells) > 1 else ''
                        age = cells[2].text.strip() if len(cells) > 2 else ''
                        height = cells[3].text.strip() if len(cells) > 3 else ''
                        weight = cells[4].text.strip() if len(cells) > 4 else ''
                        experience = cells[5].text.strip() if len(cells) > 5 else ''
                        college = cells[6].text.strip() if len(cells) > 6 else ''
                        
                        # Clean up experience field (might be 'R' for rookie or number)
                        if experience == 'R':
                            experience = '0'
                        elif experience == '--':
                            experience = '0'
                        
                        # Clean weight field
                        weight_clean = weight.replace(' lbs', '').strip() if weight else ''
                        
                        player_data = {
                            'team': team_abbr,
                            'name': name,
                            'jersey': jersey,
                            'position': position,
                            'age': age,
                            'height': height,
                            'weight': weight_clean,
                            'experience': experience,
                            'college': college,
                            'image_url': image_url,
                            'roster_section': section,
                            'scraped_date': datetime.now().isoformat()
                        }
                        
                        players.append(player_data)
                        print(f"  Found: {name} - {position} ({team_abbr})")
                        
                    except Exception as e:
                        print(f"  Error parsing player row: {e}")
                        continue
            
            return players
            
        except Exception as e:
            print(f"Error scraping {team_abbr}: {e}")
            return []
        
        # Rate limiting
        time.sleep(1)

    def save_to_csv(self, all_players):
        """Save all player data to CSV files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save master file with all players
        master_file = self.output_dir / f'nfl_rosters_all_{timestamp}.csv'
        
        fieldnames = ['team', 'name', 'jersey', 'position', 'age', 'height', 
                     'weight', 'experience', 'college', 'image_url', 
                     'roster_section', 'scraped_date']
        
        with open(master_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_players)
        
        print(f"\nSaved master roster to: {master_file}")
        
        # Save individual team files
        teams = {}
        for player in all_players:
            team = player['team']
            if team not in teams:
                teams[team] = []
            teams[team].append(player)
        
        for team, players in teams.items():
            team_file = self.output_dir / f'nfl_roster_{team}_{timestamp}.csv'
            with open(team_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(players)
            print(f"Saved {team} roster: {team_file}")

    def save_to_json(self, all_players):
        """Save all player data to JSON file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = self.output_dir / f'nfl_rosters_{timestamp}.json'
        
        # Group by team for better organization
        teams_data = {}
        for player in all_players:
            team = player['team']
            if team not in teams_data:
                teams_data[team] = []
            teams_data[team].append(player)
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(teams_data, f, indent=2)
        
        print(f"\nSaved JSON roster to: {json_file}")
        
        return json_file

    def generate_sql_script(self, all_players):
        """Generate SQL script to create and populate roster table"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sql_file = self.output_dir / f'nfl_rosters_{timestamp}.sql'
        
        with open(sql_file, 'w', encoding='utf-8') as f:
            # Create table
            f.write("""-- NFL Roster Table Creation and Population Script
-- Generated: {}

-- Drop existing table if needed
DROP TABLE IF EXISTS nfl_rosters CASCADE;

-- Create roster table
CREATE TABLE nfl_rosters (
    id SERIAL PRIMARY KEY,
    team VARCHAR(5) NOT NULL,
    name VARCHAR(100) NOT NULL,
    jersey VARCHAR(5),
    position VARCHAR(10) NOT NULL,
    age INTEGER,
    height VARCHAR(10),
    weight INTEGER,
    experience INTEGER,
    college VARCHAR(100),
    image_url TEXT,
    roster_section VARCHAR(20),
    scraped_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, team)
);

-- Create indexes
CREATE INDEX idx_rosters_team ON nfl_rosters(team);
CREATE INDEX idx_rosters_position ON nfl_rosters(position);
CREATE INDEX idx_rosters_name ON nfl_rosters(name);

-- Insert roster data
""".format(datetime.now().isoformat()))
            
            # Generate INSERT statements
            for player in all_players:
                age = player['age'] if player['age'] and player['age'].isdigit() else 'NULL'
                weight = player['weight'] if player['weight'] and player['weight'].isdigit() else 'NULL'
                exp = player['experience'] if player['experience'] and player['experience'].isdigit() else '0'
                
                f.write(f"""INSERT INTO nfl_rosters (team, name, jersey, position, age, height, weight, experience, college, image_url, roster_section, scraped_date)
VALUES ('{player['team']}', '{player['name'].replace("'", "''")}', '{player['jersey']}', '{player['position']}', {age}, '{player['height']}', {weight}, {exp}, '{player['college'].replace("'", "''")}', '{player['image_url']}', '{player['roster_section']}', '{player['scraped_date']}')
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
    updated_at = CURRENT_TIMESTAMP;
""")
        
        print(f"\nGenerated SQL script: {sql_file}")
        return sql_file

    def run(self):
        """Main execution method"""
        print("=" * 60)
        print("NFL ROSTER SCRAPER")
        print("=" * 60)
        
        # Get all team roster URLs
        roster_urls = self.get_team_roster_urls()
        print(f"\nFound {len(roster_urls)} team roster URLs")
        
        # Scrape all rosters
        all_players = []
        for team_abbr, roster_url in roster_urls.items():
            players = self.scrape_team_roster(team_abbr, roster_url)
            all_players.extend(players)
            print(f"  Scraped {len(players)} players from {team_abbr}")
            time.sleep(2)  # Be respectful with rate limiting
        
        print(f"\n{'=' * 60}")
        print(f"TOTAL PLAYERS SCRAPED: {len(all_players)}")
        print(f"{'=' * 60}")
        
        # Save to various formats
        if all_players:
            self.save_to_csv(all_players)
            self.save_to_json(all_players)
            sql_file = self.generate_sql_script(all_players)
            
            print("\n" + "=" * 60)
            print("SCRAPING COMPLETE!")
            print(f"Total players: {len(all_players)}")
            print(f"Output directory: {self.output_dir}")
            print("\nTo import to database, run:")
            print(f"  psql -h your_host -U your_user -d your_db < {sql_file}")
            print("=" * 60)
        else:
            print("\nNo players scraped. Please check the URLs and try again.")
        
        return all_players


if __name__ == "__main__":
    scraper = NFLRosterScraper()
    scraper.run()