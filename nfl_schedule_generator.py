#!/usr/bin/env python3
"""
NFL Schedule Generator
Creates comprehensive URL lists for all NFL weeks (preseason, regular season, playoffs)
"""

import json
import requests
from datetime import datetime, timedelta
import os
import time
from typing import Dict, List, Optional

class NFLScheduleGenerator:
    def __init__(self):
        self.base_url = "https://www.espn.com/nfl"
        self.current_year = 2025
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # NFL season structure
        self.season_structure = {
            'preseason': {
                'weeks': 3,
                'start_week': 1,
                'games_per_week': 16
            },
            'regular_season': {
                'weeks': 18,
                'start_week': 1,
                'games_per_week': 16
            },
            'playoffs': {
                'weeks': 4,
                'start_week': 1,
                'games_per_week': [6, 4, 2, 1]  # Wild Card, Divisional, Conference, Super Bowl
            }
        }
    
    def get_nfl_schedule_from_espn(self, season_type: str = 'regular') -> Dict:
        """
        Fetch NFL schedule from ESPN API
        season_type: 'preseason', 'regular', 'postseason'
        """
        try:
            # ESPN NFL scoreboard API endpoint
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
            
            params = {
                'dates': f'{self.current_year}',
                'seasontype': '2' if season_type == 'regular' else ('1' if season_type == 'preseason' else '3'),
                'limit': 1000
            }
            
            print(f"Fetching {season_type} schedule from ESPN API...")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            print(f"Error fetching ESPN schedule: {e}")
            return {}
    
    def extract_game_urls_from_schedule(self, schedule_data: Dict, target_season_type: str = 'regular') -> List[Dict]:
        """Extract game URLs and metadata from ESPN schedule data"""
        games = []
        
        if 'events' not in schedule_data:
            print("No events found in schedule data")
            return games
        
        # Map season type names to ESPN API values
        season_type_map = {
            'preseason': 1,
            'regular': 2,
            'postseason': 3
        }
        
        target_type_value = season_type_map.get(target_season_type, 2)
        
        for event in schedule_data['events']:
            try:
                game_id = event['id']
                game_url = f"https://www.espn.com/nfl/game/_/gameId/{game_id}"
                
                # Extract game metadata
                date = event.get('date', '')
                week = event.get('week', {}).get('number', 0)
                season_type = event.get('season', {}).get('type', 0)
                
                # Filter by season type - only include games matching the target season type
                if season_type != target_type_value:
                    continue
                
                # Get team information
                competitions = event.get('competitions', [])
                if competitions:
                    competitors = competitions[0].get('competitors', [])
                    if len(competitors) >= 2:
                        away_team = competitors[1].get('team', {}).get('abbreviation', 'TBD')
                        home_team = competitors[0].get('team', {}).get('abbreviation', 'TBD')
                        
                        games.append({
                            'url': game_url,
                            'game_id': game_id,
                            'date': date,
                            'week': week,
                            'season_type': season_type,
                            'away_team': away_team,
                            'home_team': home_team,
                            'matchup': f"{away_team} @ {home_team}"
                        })
                
            except Exception as e:
                print(f"Error processing game event: {e}")
                continue
        
        print(f"Filtered to {len(games)} games for {target_season_type} season")
        return games
    
    def create_weekly_files(self, games: List[Dict], season_type: str) -> Dict[str, List]:
        """Create weekly game URL files"""
        weekly_games = {}
        
        for game in games:
            week = game['week']
            if week not in weekly_games:
                weekly_games[week] = []
            weekly_games[week].append(game)
        
        # Sort weeks
        sorted_weeks = sorted(weekly_games.keys())
        
        for week in sorted_weeks:
            week_games = weekly_games[week]
            
            # Create filename
            if season_type == 'preseason':
                filename = f"preseason_week{week}_{self.current_year}.txt"
            elif season_type == 'regular':
                filename = f"regular_week{week}_{self.current_year}.txt"
            else:
                filename = f"playoffs_week{week}_{self.current_year}.txt"
            
            # Write file
            self.write_weekly_file(filename, week_games, season_type, week)
            
            # Create summary JSON
            summary_filename = filename.replace('.txt', '_summary.json')
            self.write_weekly_summary(summary_filename, week_games, season_type, week)
        
        return weekly_games
    
    def write_weekly_file(self, filename: str, games: List[Dict], season_type: str, week: int):
        """Write weekly game URLs to file"""
        filepath = os.path.join(os.getcwd(), filename)
        
        with open(filepath, 'w') as f:
            f.write(f"# NFL {season_type.title()} Week {week}, {self.current_year} Game URLs\n")
            f.write("# Format: One ESPN game URL per line\n")
            f.write(f"# Use: python process_nfl_game_file.py {filename}\n")
            f.write("\n")
            f.write(f"# {len(games)} games scheduled for this week\n")
            
            for game in games:
                f.write(f"# {game['matchup']} - {game['date']}\n")
                f.write(f"{game['url']}\n")
        
        print(f"Created {filepath} with {len(games)} games")
    
    def write_weekly_summary(self, filename: str, games: List[Dict], season_type: str, week: int):
        """Write weekly summary JSON file"""
        filepath = os.path.join(os.getcwd(), filename)
        
        summary = {
            'season_type': season_type,
            'week': week,
            'year': self.current_year,
            'total_games': len(games),
            'generated_date': datetime.now().isoformat(),
            'games': games
        }
        
        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Created summary {filepath}")
    
    def generate_full_season_schedule(self):
        """Generate complete NFL schedule for the season"""
        print(f"🏈 Generating NFL {self.current_year} Season Schedule")
        print("=" * 50)
        
        # Generate each season type
        season_types = ['preseason', 'regular', 'postseason']
        
        for season_type in season_types:
            print(f"\n📅 Processing {season_type} schedule...")
            
            # Get schedule data from ESPN
            schedule_data = self.get_nfl_schedule_from_espn(season_type)
            
            if not schedule_data:
                print(f"⚠️  No schedule data found for {season_type}")
                continue
            
            # Extract game URLs
            games = self.extract_game_urls_from_schedule(schedule_data, season_type)
            
            if not games:
                print(f"⚠️  No games found for {season_type}")
                continue
            
            print(f"✅ Found {len(games)} games for {season_type}")
            
            # Create weekly files
            weekly_games = self.create_weekly_files(games, season_type)
            
            print(f"📄 Created {len(weekly_games)} weekly files for {season_type}")
            
            # Rate limiting
            time.sleep(1)
        
        print(f"\n🎯 NFL Schedule generation complete!")
        print(f"📂 Files created in: {os.getcwd()}")
    
    def generate_sample_schedule(self):
        """Generate sample schedule files based on known structure"""
        print("🏈 Generating Sample NFL Schedule Structure")
        print("=" * 50)
        
        # Create sample preseason files (3 weeks)
        for week in range(1, 4):
            filename = f"preseason_week{week}_{self.current_year}.txt"
            self.create_sample_week_file(filename, 'preseason', week, 16)
        
        # Create sample regular season files (18 weeks)
        for week in range(1, 19):
            filename = f"regular_week{week}_{self.current_year}.txt"
            self.create_sample_week_file(filename, 'regular_season', week, 16)
        
        # Create sample playoff files (4 weeks)
        playoff_games = [6, 4, 2, 1]  # Wild Card, Divisional, Conference, Super Bowl
        for week in range(1, 5):
            filename = f"playoffs_week{week}_{self.current_year}.txt"
            self.create_sample_week_file(filename, 'playoffs', week, playoff_games[week-1])
        
        print("✅ Sample schedule structure created!")
    
    def create_sample_week_file(self, filename: str, season_type: str, week: int, num_games: int):
        """Create sample week file with placeholder URLs"""
        filepath = os.path.join(os.getcwd(), filename)
        
        with open(filepath, 'w') as f:
            f.write(f"# NFL {season_type.replace('_', ' ').title()} Week {week}, {self.current_year} Game URLs\n")
            f.write("# Format: One ESPN game URL per line\n")
            f.write(f"# Use: python process_nfl_game_file.py {filename}\n")
            f.write("\n")
            f.write(f"# {num_games} games scheduled for this week\n")
            f.write("# TODO: Replace with actual ESPN game URLs when available\n")
            f.write("\n")
            
            for i in range(num_games):
                f.write(f"# Game {i+1} - TBD vs TBD\n")
                f.write(f"# https://www.espn.com/nfl/game/_/gameId/TBD{i+1:03d}\n")
        
        print(f"Created sample {filepath} with {num_games} game placeholders")

def main():
    generator = NFLScheduleGenerator()
    
    # Try to generate full schedule from ESPN API
    try:
        generator.generate_full_season_schedule()
    except Exception as e:
        print(f"Error generating full schedule: {e}")
        print("Falling back to sample schedule structure...")
        generator.generate_sample_schedule()

if __name__ == "__main__":
    main()