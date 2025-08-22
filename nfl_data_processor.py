#!/usr/bin/env python3
"""
NFL Data Processor
Converts NFL CSV files to JSON format for FootballTracker consumption
Similar to BaseballTracker's data processing pipeline
"""

import csv
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import re

class NFLDataProcessor:
    def __init__(self):
        self.base_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Claude-Code")
        self.csv_dir = self.base_dir / "FootballData" / "CSV_BACKUPS"
        self.json_dir = self.base_dir / "FootballData" / "data"
        self.stats_dir = self.json_dir / "stats"
        self.team_stats_dir = self.json_dir / "team_stats"
        self.player_stats_dir = self.json_dir / "player_stats"
        
        # NFL teams mapping
        self.nfl_teams = {
            'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons', 'BAL': 'Baltimore Ravens',
            'BUF': 'Buffalo Bills', 'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
            'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys',
            'DEN': 'Denver Broncos', 'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
            'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts', 'JAX': 'Jacksonville Jaguars',
            'KC': 'Kansas City Chiefs', 'LV': 'Las Vegas Raiders', 'LAC': 'Los Angeles Chargers',
            'LAR': 'Los Angeles Rams', 'MIA': 'Miami Dolphins', 'MIN': 'Minnesota Vikings',
            'NE': 'New England Patriots', 'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
            'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles', 'PIT': 'Pittsburgh Steelers',
            'SF': 'San Francisco 49ers', 'SEA': 'Seattle Seahawks', 'TB': 'Tampa Bay Buccaneers',
            'TEN': 'Tennessee Titans', 'WSH': 'Washington Commanders'
        }
        
        # NFL statistical categories
        self.stat_categories = [
            'passing', 'rushing', 'receiving', 'fumbles', 'defense', 
            'interceptions', 'kick returns', 'punt returns', 'kicking', 'punting'
        ]
        
        # Ensure directories exist
        self.create_directories()
    
    def create_directories(self):
        """Create necessary directories for JSON output"""
        directories = [
            self.json_dir,
            self.stats_dir,
            self.team_stats_dir,
            self.player_stats_dir,
            self.json_dir / "weekly",
            self.json_dir / "rosters",
            self.json_dir / "predictions"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"✅ Directory ready: {directory}")
    
    def parse_csv_filename(self, filename: str) -> Optional[Dict[str, str]]:
        """
        Parse NFL CSV filename to extract metadata
        Format: nfl_TEAM_CATEGORY_weekN_YYYYMMDD_GAMEID.csv
        """
        try:
            # Remove .csv extension
            name = filename.replace('.csv', '')
            
            # Pattern: nfl_TEAM_CATEGORY_weekN_YYYYMMDD_GAMEID
            pattern = r'nfl_([A-Z]{2,3})_([^_]+)_week(\d+)_(\d{8})_(\d+)'
            match = re.match(pattern, name)
            
            if match:
                team, category, week, date, game_id = match.groups()
                return {
                    'team': team,
                    'category': category.lower(),
                    'week': int(week),
                    'date': date,
                    'game_id': game_id,
                    'filename': filename
                }
            else:
                print(f"⚠️  Could not parse filename: {filename}")
                return None
                
        except Exception as e:
            print(f"Error parsing filename {filename}: {e}")
            return None
    
    def read_csv_file(self, filepath: Path) -> List[Dict]:
        """Read CSV file and return as list of dictionaries"""
        try:
            data = []
            with open(filepath, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    # Clean up the data
                    cleaned_row = {}
                    for key, value in row.items():
                        # Clean key names
                        clean_key = key.strip().lower().replace(' ', '_')
                        # Clean values
                        clean_value = value.strip() if isinstance(value, str) else value
                        # Convert numeric values
                        if clean_value and clean_value.isdigit():
                            clean_value = int(clean_value)
                        elif clean_value and self.is_float(clean_value):
                            clean_value = float(clean_value)
                        
                        cleaned_row[clean_key] = clean_value
                    
                    data.append(cleaned_row)
            
            return data
        
        except Exception as e:
            print(f"Error reading CSV file {filepath}: {e}")
            return []
    
    def is_float(self, value: str) -> bool:
        """Check if string represents a float"""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def process_weekly_data(self, week: int, season: int = 2025) -> Dict[str, Any]:
        """Process all CSV files for a specific week"""
        print(f"🏈 Processing Week {week} data...")
        
        weekly_data = {
            'week': week,
            'season': season,
            'teams': {},
            'games': [],
            'stats_summary': {
                'total_files': 0,
                'teams_processed': 0,
                'categories_processed': {}
            }
        }
        
        # Find all CSV files for this week
        week_pattern = f"*week{week}_*.csv"
        csv_files = list(self.csv_dir.glob(week_pattern))
        
        weekly_data['stats_summary']['total_files'] = len(csv_files)
        
        for csv_file in csv_files:
            file_info = self.parse_csv_filename(csv_file.name)
            if not file_info:
                continue
            
            # Skip TBD files (old format)
            if file_info['team'] == 'TBD':
                print(f"⏭️  Skipping TBD file: {csv_file.name}")
                continue
            
            team = file_info['team']
            category = file_info['category']
            
            # Initialize team data if not exists
            if team not in weekly_data['teams']:
                weekly_data['teams'][team] = {
                    'team_name': self.nfl_teams.get(team, team),
                    'team_abbr': team,
                    'stats': {}
                }
            
            # Read CSV data
            csv_data = self.read_csv_file(csv_file)
            if csv_data:
                weekly_data['teams'][team]['stats'][category] = csv_data
                
                # Update category count
                if category not in weekly_data['stats_summary']['categories_processed']:
                    weekly_data['stats_summary']['categories_processed'][category] = 0
                weekly_data['stats_summary']['categories_processed'][category] += 1
                
                print(f"  ✅ {team} {category}: {len(csv_data)} player records")
        
        weekly_data['stats_summary']['teams_processed'] = len(weekly_data['teams'])
        
        return weekly_data
    
    def generate_player_aggregates(self, weekly_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate aggregated player statistics across all teams"""
        print("📊 Generating player aggregates...")
        
        player_aggregates = {
            'week': weekly_data['week'],
            'season': weekly_data['season'],
            'players': {},
            'position_leaders': {
                'passing': [],
                'rushing': [],
                'receiving': [],
                'defense': [],
                'kicking': []
            }
        }
        
        # Process each team's data
        for team_abbr, team_data in weekly_data['teams'].items():
            for category, players in team_data['stats'].items():
                for player in players:
                    player_name = player.get('name', player.get('player', ''))
                    if not player_name:
                        continue
                    
                    # Initialize player if not exists
                    if player_name not in player_aggregates['players']:
                        player_aggregates['players'][player_name] = {
                            'name': player_name,
                            'team': team_abbr,
                            'stats': {}
                        }
                    
                    # Add category stats
                    if category not in player_aggregates['players'][player_name]['stats']:
                        player_aggregates['players'][player_name]['stats'][category] = []
                    
                    player_aggregates['players'][player_name]['stats'][category].append(player)
        
        # Generate position leaders
        player_aggregates['position_leaders'] = self.generate_position_leaders(player_aggregates['players'])
        
        return player_aggregates
    
    def generate_position_leaders(self, players: Dict[str, Any]) -> Dict[str, List]:
        """Generate position leaders for key statistical categories"""
        leaders = {
            'passing_yards': [],
            'rushing_yards': [],
            'receiving_yards': [],
            'touchdowns': [],
            'field_goals': []
        }
        
        for player_name, player_data in players.items():
            stats = player_data['stats']
            
            # Passing leaders
            if 'passing' in stats:
                for passing_stat in stats['passing']:
                    yards = passing_stat.get('yards', passing_stat.get('yds', 0))
                    if isinstance(yards, (int, float)) and yards > 0:
                        leaders['passing_yards'].append({
                            'player': player_name,
                            'team': player_data['team'],
                            'yards': yards,
                            'category': 'passing'
                        })
            
            # Rushing leaders
            if 'rushing' in stats:
                for rushing_stat in stats['rushing']:
                    yards = rushing_stat.get('yards', rushing_stat.get('yds', 0))
                    if isinstance(yards, (int, float)) and yards > 0:
                        leaders['rushing_yards'].append({
                            'player': player_name,
                            'team': player_data['team'],
                            'yards': yards,
                            'category': 'rushing'
                        })
            
            # Receiving leaders
            if 'receiving' in stats:
                for receiving_stat in stats['receiving']:
                    yards = receiving_stat.get('yards', receiving_stat.get('yds', 0))
                    if isinstance(yards, (int, float)) and yards > 0:
                        leaders['receiving_yards'].append({
                            'player': player_name,
                            'team': player_data['team'],
                            'yards': yards,
                            'category': 'receiving'
                        })
        
        # Sort leaders by yards (top 10 each)
        for category in leaders:
            if leaders[category]:
                leaders[category] = sorted(leaders[category], 
                                         key=lambda x: x.get('yards', 0), 
                                         reverse=True)[:10]
        
        return leaders
    
    def save_weekly_json(self, weekly_data: Dict[str, Any], week: int, season: int = 2025):
        """Save weekly data as JSON files"""
        # Save main weekly file
        weekly_file = self.json_dir / "weekly" / f"week_{week}_{season}.json"
        with open(weekly_file, 'w') as f:
            json.dump(weekly_data, f, indent=2)
        print(f"📄 Saved weekly data: {weekly_file}")
        
        # Save player aggregates
        player_aggregates = self.generate_player_aggregates(weekly_data)
        player_file = self.player_stats_dir / f"players_week_{week}_{season}.json"
        with open(player_file, 'w') as f:
            json.dump(player_aggregates, f, indent=2)
        print(f"📄 Saved player aggregates: {player_file}")
        
        # Save team stats
        team_file = self.team_stats_dir / f"teams_week_{week}_{season}.json"
        team_stats = {
            'week': week,
            'season': season,
            'teams': weekly_data['teams']
        }
        with open(team_file, 'w') as f:
            json.dump(team_stats, f, indent=2)
        print(f"📄 Saved team stats: {team_file}")
    
    def update_nfl_teams_json(self):
        """Update the main NFL teams JSON file"""
        teams_file = self.stats_dir / "nfl_teams.json"
        teams_data = {
            'teams': self.nfl_teams,
            'total_teams': len(self.nfl_teams),
            'last_updated': datetime.now().isoformat()
        }
        
        with open(teams_file, 'w') as f:
            json.dump(teams_data, f, indent=2)
        print(f"📄 Updated NFL teams: {teams_file}")
    
    def process_all_available_weeks(self):
        """Process all available CSV data by week"""
        print("🏈 NFL Data Processing Pipeline Started")
        print("=" * 50)
        
        # Update teams JSON
        self.update_nfl_teams_json()
        
        # Find all unique weeks from CSV files
        csv_files = list(self.csv_dir.glob("nfl_*_week*.csv"))
        weeks = set()
        
        for csv_file in csv_files:
            file_info = self.parse_csv_filename(csv_file.name)
            if file_info and file_info['team'] != 'TBD':
                weeks.add(file_info['week'])
        
        weeks = sorted(weeks)
        print(f"📅 Found data for weeks: {weeks}")
        
        if not weeks:
            print("⚠️  No valid week data found in CSV files")
            return
        
        # Process each week
        for week in weeks:
            weekly_data = self.process_weekly_data(week)
            if weekly_data['teams']:
                self.save_weekly_json(weekly_data, week)
                print(f"✅ Week {week} processing complete")
            else:
                print(f"⚠️  No data found for Week {week}")
        
        # Generate summary
        self.generate_processing_summary(weeks)
        
        print("\n🎯 NFL Data Processing Pipeline Complete!")
    
    def generate_processing_summary(self, weeks: List[int]):
        """Generate a summary of the data processing"""
        summary = {
            'processed_date': datetime.now().isoformat(),
            'weeks_processed': weeks,
            'total_weeks': len(weeks),
            'data_directories': {
                'csv_source': str(self.csv_dir),
                'json_output': str(self.json_dir),
                'weekly_data': str(self.json_dir / "weekly"),
                'player_stats': str(self.player_stats_dir),
                'team_stats': str(self.team_stats_dir)
            },
            'nfl_teams': len(self.nfl_teams),
            'stat_categories': self.stat_categories
        }
        
        summary_file = self.json_dir / "processing_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"📋 Processing summary saved: {summary_file}")

def main():
    processor = NFLDataProcessor()
    processor.process_all_available_weeks()

if __name__ == "__main__":
    main()