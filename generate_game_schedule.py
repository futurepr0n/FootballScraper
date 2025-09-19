#!/usr/bin/env python3
"""
Generate game schedule data from processed URL files and CSV data
"""

import json
import os
from pathlib import Path
from datetime import datetime
import re

def get_game_info_from_url(url):
    """Extract game ID and teams from ESPN URL"""
    game_id_match = re.search(r'/(\d+)(?:/|$)', url)
    if game_id_match:
        return game_id_match.group(1)
    return None

def parse_url_file(file_path):
    """Parse a URL file to extract game information"""
    games = {}
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and line.startswith('http'):
                game_id = get_game_info_from_url(line)
                if game_id:
                    games[game_id] = {
                        'game_id': game_id,
                        'url': line,
                        'home_team': None,
                        'away_team': None,
                        'game_date': None,
                        'week': None
                    }
    
    return games

def extract_matchups_from_csv_files(csv_dir, games_dict, week):
    """Extract team matchups from CSV filenames"""
    csv_path = Path(csv_dir)
    
    # Track which teams played in each game
    game_teams = {}
    
    for csv_file in csv_path.glob(f'*week{week}_*.csv'):
        # Parse filename: nfl_TEAM_category_week#_date_gameid.csv
        parts = csv_file.stem.split('_')
        if len(parts) >= 6:
            team = parts[1]
            game_id = parts[-1]
            date_str = parts[-2]
            
            if game_id in games_dict:
                if game_id not in game_teams:
                    game_teams[game_id] = {
                        'teams': set(),
                        'date': date_str,
                        'week': week
                    }
                game_teams[game_id]['teams'].add(team)
    
    # Now match teams to create home/away assignments
    for game_id, info in game_teams.items():
        if game_id in games_dict:
            teams_list = list(info['teams'])
            if len(teams_list) >= 2:
                # For now, arbitrarily assign first team as home
                # TODO: Could improve by checking actual game data
                games_dict[game_id]['home_team'] = teams_list[0]
                games_dict[game_id]['away_team'] = teams_list[1]
                games_dict[game_id]['game_date'] = f"{info['date'][:4]}-{info['date'][4:6]}-{info['date'][6:8]}"
                games_dict[game_id]['week'] = info['week']
    
    return games_dict

def generate_schedule_files(output_dir):
    """Generate schedule JSON files for each week"""
    
    # Paths
    scraper_dir = Path(__file__).parent
    data_dir = scraper_dir.parent / 'FootballData'
    csv_dir = data_dir / 'BOXSCORE_CSV'
    output_path = data_dir / 'data' / 'schedule'
    
    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Process each week's URL file
    for url_file in scraper_dir.glob('regular_week*.txt'):
        # Extract week number from filename
        week_match = re.search(r'week(\d+)', url_file.name)
        if week_match:
            week = int(week_match.group(1))
            print(f"Processing Week {week}: {url_file.name}")
            
            # Parse URL file
            games = parse_url_file(url_file)
            
            # Extract matchups from CSV files
            games = extract_matchups_from_csv_files(csv_dir, games, week)
            
            # Create schedule data
            schedule = {
                'week': week,
                'season': 2025,
                'season_type': 'regular',
                'games': []
            }
            
            for game_id, game_info in games.items():
                if game_info['home_team'] and game_info['away_team']:
                    schedule['games'].append({
                        'game_id': game_info['game_id'],
                        'home_team': game_info['home_team'],
                        'away_team': game_info['away_team'],
                        'game_date': game_info['game_date'],
                        'game_time': '1:00 PM EST',
                        'status': 'completed',
                        'venue': 'TBD',
                        'source': 'generated'
                    })
            
            # Save schedule file
            output_file = output_path / f'regular_week_{week}_2025.json'
            with open(output_file, 'w') as f:
                json.dump(schedule, f, indent=2)
            
            print(f"  Created {output_file} with {len(schedule['games'])} games")

if __name__ == '__main__':
    generate_schedule_files('.')