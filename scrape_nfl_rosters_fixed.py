#!/usr/bin/env python3
"""
ESPN NFL Roster Scraper - Fixed Version
Properly extracts roster data from ESPN team pages
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
from pathlib import Path
from datetime import datetime
import time
import re

class NFLRosterScraperFixed:
    def __init__(self):
        self.base_url = "https://www.espn.com"
        self.teams_url = "https://www.espn.com/nfl/teams"
        self.output_dir = Path("../FootballData/rosters")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # NFL team mappings
        self.team_names = {
            'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons',
            'BAL': 'Baltimore Ravens', 'BUF': 'Buffalo Bills',
            'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
            'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns',
            'DAL': 'Dallas Cowboys', 'DEN': 'Denver Broncos',
            'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
            'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts',
            'JAX': 'Jacksonville Jaguars', 'KC': 'Kansas City Chiefs',
            'LAC': 'Los Angeles Chargers', 'LAR': 'Los Angeles Rams',
            'LV': 'Las Vegas Raiders', 'MIA': 'Miami Dolphins',
            'MIN': 'Minnesota Vikings', 'NE': 'New England Patriots',
            'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
            'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles',
            'PIT': 'Pittsburgh Steelers', 'SEA': 'Seattle Seahawks',
            'SF': 'San Francisco 49ers', 'TB': 'Tampa Bay Buccaneers',
            'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
        }
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def get_team_roster_url(self, team_page_url):
        """Get the roster URL from a team's main page"""
        try:
            response = self.session.get(team_page_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the roster link
            roster_link = soup.find('a', {'class': 'AnchorLink', 'href': re.compile(r'/roster')})
            if roster_link:
                return self.base_url + roster_link['href']
            
            # Alternative method - look for roster in navigation
            nav_links = soup.find_all('a', href=re.compile(r'/roster'))
            if nav_links:
                return self.base_url + nav_links[0]['href']
            
            return None
        except Exception as e:
            print(f"Error getting roster URL: {e}")
            return None

    def scrape_roster(self, roster_url, team_abbr):
        """Scrape roster data from ESPN roster page"""
        print(f"\nScraping {team_abbr} roster from: {roster_url}")
        
        try:
            response = self.session.get(roster_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            players = []
            
            # Find all table rows that contain player data
            # ESPN uses different table structures, so we need to be flexible
            
            # Method 1: Look for player rows in tables
            player_rows = soup.find_all('tr', class_=re.compile(r'Table__TR'))
            
            if not player_rows:
                # Method 2: Look for divs with player info
                player_rows = soup.find_all('div', class_=re.compile(r'Table__TR'))
            
            for row in player_rows:
                try:
                    # Skip header rows
                    if row.find('th') or 'Table__TH' in str(row.get('class', [])):
                        continue
                    
                    # Extract player data
                    cells = row.find_all(['td', 'div'], class_=re.compile(r'Table__TD'))
                    
                    if len(cells) >= 7:  # Ensure we have enough data
                        # Find the player name and jersey
                        name_cell = cells[0]
                        name_link = name_cell.find('a')
                        
                        if name_link:
                            name = name_link.text.strip()
                            
                            # Extract jersey number from the cell
                            jersey_span = name_cell.find('span', class_=re.compile(r'number|jersey'))
                            if jersey_span:
                                jersey = jersey_span.text.strip()
                            else:
                                # Sometimes jersey is just text in the cell
                                cell_text = name_cell.get_text(' ', strip=True)
                                # Extract number from text like "17 Josh Allen"
                                jersey_match = re.match(r'^(\d+)\s', cell_text)
                                if jersey_match:
                                    jersey = jersey_match.group(1)
                                else:
                                    jersey = None
                        else:
                            # Sometimes name is just text
                            name_text = cells[0].get_text(strip=True)
                            # Parse "17 Josh Allen" format
                            name_match = re.match(r'^(\d+)?\s*(.+)$', name_text)
                            if name_match:
                                jersey = name_match.group(1) if name_match.group(1) else None
                                name = name_match.group(2).strip()
                            else:
                                name = name_text
                                jersey = None
                        
                        # Skip if no valid name
                        if not name or name.lower() in ['name', 'player', 'offense', 'defense', 'special teams']:
                            continue
                        
                        # Extract other fields
                        position = cells[1].get_text(strip=True) if len(cells) > 1 else None
                        age = cells[2].get_text(strip=True) if len(cells) > 2 else None
                        height = cells[3].get_text(strip=True) if len(cells) > 3 else None
                        weight = cells[4].get_text(strip=True) if len(cells) > 4 else None
                        experience = cells[5].get_text(strip=True) if len(cells) > 5 else None
                        college = cells[6].get_text(strip=True) if len(cells) > 6 else None
                        
                        # Get player image if available
                        img = name_cell.find('img')
                        image_url = img.get('src', '') if img else None
                        
                        # Determine roster section (offense, defense, special teams)
                        roster_section = 'Unknown'
                        # Look for section headers above this player
                        prev_element = row.find_previous('div', class_=re.compile(r'title|header|section'))
                        if prev_element:
                            section_text = prev_element.get_text(strip=True).lower()
                            if 'offense' in section_text:
                                roster_section = 'Offense'
                            elif 'defense' in section_text:
                                roster_section = 'Defense'
                            elif 'special' in section_text:
                                roster_section = 'Special Teams'
                        
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
                            'image_url': image_url,
                            'roster_section': roster_section
                        }
                        
                        players.append(player)
                        
                except Exception as e:
                    # Skip problematic rows
                    continue
            
            print(f"  Found {len(players)} players for {team_abbr}")
            return players
            
        except Exception as e:
            print(f"Error scraping {team_abbr}: {e}")
            return []

    def get_all_teams(self):
        """Get all NFL team pages from ESPN"""
        print("Getting all NFL team pages...")
        
        try:
            response = self.session.get(self.teams_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            teams = []
            
            # Find all team links
            team_links = soup.find_all('a', class_=re.compile(r'AnchorLink'), href=re.compile(r'/nfl/team/_/'))
            
            for link in team_links:
                href = link.get('href', '')
                # Extract team abbreviation from URL
                # Format: /nfl/team/_/name/buf/buffalo-bills
                match = re.search(r'/name/([a-z]+)/', href)
                if match:
                    team_abbr = match.group(1).upper()
                    team_url = self.base_url + href
                    
                    # Skip duplicates
                    if not any(t['abbr'] == team_abbr for t in teams):
                        teams.append({
                            'abbr': team_abbr,
                            'url': team_url,
                            'name': self.team_names.get(team_abbr, team_abbr)
                        })
            
            print(f"Found {len(teams)} teams")
            return teams
            
        except Exception as e:
            print(f"Error getting teams: {e}")
            return []

    def scrape_all_teams(self):
        """Scrape rosters for all NFL teams"""
        teams = self.get_all_teams()
        
        if not teams:
            print("No teams found. Using hardcoded list...")
            # Fallback to hardcoded teams
            teams = [
                {'abbr': abbr, 'name': name, 'url': f"{self.base_url}/nfl/team/roster/_/name/{abbr.lower()}"}
                for abbr, name in self.team_names.items()
            ]
        
        all_players = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for team in teams:
            print(f"\nProcessing {team['name']} ({team['abbr']})...")
            
            # Get roster URL
            roster_url = self.get_team_roster_url(team['url'])
            if not roster_url:
                # Try direct roster URL format
                roster_url = f"{self.base_url}/nfl/team/roster/_/name/{team['abbr'].lower()}"
            
            # Scrape roster
            players = self.scrape_roster(roster_url, team['abbr'])
            
            if players:
                all_players.extend(players)
                
                # Save individual team CSV
                team_file = self.output_dir / f"nfl_roster_{team['abbr']}_{timestamp}.csv"
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
        
        return all_players

    def save_to_csv(self, players, filename):
        """Save player data to CSV"""
        if not players:
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['team', 'name', 'jersey', 'position', 'age', 'height', 
                         'weight', 'experience', 'college', 'image_url', 'roster_section']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(players)
        
        print(f"  Saved {len(players)} players to {filename}")

    def save_to_json(self, players, filename):
        """Save player data to JSON"""
        if not players:
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(players, f, indent=2)
        
        print(f"  Saved {len(players)} players to {filename}")

def main():
    scraper = NFLRosterScraperFixed()
    
    # Test with one team first
    print("Testing with Buffalo Bills first...")
    test_url = "https://www.espn.com/nfl/team/_/name/buf/buffalo-bills"
    roster_url = scraper.get_team_roster_url(test_url)
    
    if roster_url:
        players = scraper.scrape_roster(roster_url, 'BUF')
        if players:
            print(f"\nSample players from BUF:")
            for player in players[:5]:
                print(f"  {player['jersey'] or 'XX':>2} {player['name']:<20} {player['position']:<5} {player['height']:<6} {player['weight']:<7}")
            
            print(f"\nTest successful! Found {len(players)} players.")
            print("\nProceed with full scrape? (y/n): ", end='')
            
            # Auto-proceed for automation
            print("y")
            scraper.scrape_all_teams()
        else:
            print("No players found in test. Check scraping logic.")
    else:
        print("Could not find roster URL. Check URL patterns.")

if __name__ == "__main__":
    main()