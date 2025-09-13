#!/usr/bin/env python3
"""
Production NFL Scraper - Validation-First Approach
✅ Extracts actual game dates from ESPN (no more scraper run dates)
✅ Uses ESPN's working API endpoints for reliable data  
✅ Validates all data before proceeding
✅ Stops on any validation failure
✅ Handles duplicates and existing files properly
✅ Gets comprehensive player statistics (passing, rushing, receiving, etc.)
⚠️ Play-by-play requires browser automation (future enhancement)
"""

import subprocess
import json
import csv
import time
import random
import os
import re
from datetime import datetime
from pathlib import Path

class ProductionNFLScraper:
    def __init__(self):
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.csv_dir.mkdir(exist_ok=True)
        
        self.validation_errors = []
        
        self.valid_teams = {
            'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN',
            'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LAC', 'LAR', 'LV', 'MIA',
            'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB',
            'TEN', 'WAS', 'WSH'  # Include both WAS and WSH for Washington
        }
        
        print("🏈 Production NFL Scraper initialized")
        print(f"📂 CSV output directory: {self.csv_dir}")
    
    def log_error(self, error_msg):
        """Log validation error and add to error list"""
        self.validation_errors.append(error_msg)
        print(f"❌ VALIDATION ERROR: {error_msg}")
    
    def extract_actual_game_date(self, game_url):
        """Extract actual game date from ESPN page - CRITICAL for correct file naming"""
        try:
            print(f"📅 Extracting actual game date from: {game_url}")
            
            headers = [
                '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            ]
            
            cmd = ['curl', '-s', '--max-time', '15'] + headers + [game_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            
            if result.returncode != 0:
                self.log_error(f"Failed to fetch game page: curl returned {result.returncode}")
                return None
            
            html_content = result.stdout
            
            # Look for title with date pattern: "Team1 XX-YY Team2 (Sep 10, 2023) Final Score - ESPN"
            title_match = re.search(r'<title[^>]*>.*?\(([^)]+)\).*?</title>', html_content)
            
            if title_match:
                date_str = title_match.group(1).strip()
                print(f"📅 Found date string: '{date_str}'")
                
                # Parse "Sep 10, 2023" format
                date_pattern = r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})'
                date_match = re.match(date_pattern, date_str)
                
                if date_match:
                    month_str = date_match.group(1)
                    day = date_match.group(2).zfill(2)
                    year = date_match.group(3)
                    
                    months = {
                        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                    }
                    
                    month_num = months.get(month_str)
                    if not month_num:
                        self.log_error(f"Unknown month abbreviation: {month_str}")
                        return None
                    
                    actual_date = f"{year}{month_num}{day}"
                    print(f"✅ Extracted actual game date: {actual_date}")
                    return actual_date
            
            self.log_error("Could not find game date in ESPN page title")
            return None
            
        except Exception as e:
            self.log_error(f"Failed to extract game date: {e}")
            return None
    
    def fetch_espn_summary_api(self, game_id):
        """Fetch comprehensive game data from ESPN's summary API"""
        try:
            api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}"
            print(f"📡 Fetching ESPN API: {api_url}")
            
            cmd = ['curl', '-s', '--max-time', '15', '-H', 'Accept: application/json', api_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            
            if result.returncode != 0:
                self.log_error(f"ESPN API request failed: curl returned {result.returncode}")
                return None
            
            try:
                data = json.loads(result.stdout)
                print(f"✅ Successfully retrieved ESPN API data")
                return data
            except json.JSONDecodeError as e:
                self.log_error(f"Invalid JSON response from ESPN API: {e}")
                return None
                
        except Exception as e:
            self.log_error(f"Failed to fetch ESPN API data: {e}")
            return None
    
    def extract_team_info(self, api_data):
        """Extract and validate team information from API data"""
        try:
            if 'boxscore' not in api_data or 'teams' not in api_data['boxscore']:
                self.log_error("No team data found in API response")
                return None
            
            teams = api_data['boxscore']['teams']
            if len(teams) != 2:
                self.log_error(f"Expected 2 teams, found {len(teams)}")
                return None
            
            team_info = {}
            for team in teams:
                team_data = team.get('team', {})
                abbreviation = team_data.get('abbreviation', '')
                home_away = team.get('homeAway', '')
                
                # Validate team abbreviation
                if abbreviation not in self.valid_teams:
                    self.log_error(f"Invalid team abbreviation: {abbreviation}")
                    return None
                
                team_info[home_away] = {
                    'abbreviation': abbreviation,
                    'name': team_data.get('displayName', ''),
                    'location': team_data.get('location', ''),
                    'color': team_data.get('color', '')
                }
            
            if 'home' not in team_info or 'away' not in team_info:
                self.log_error("Could not identify home and away teams")
                return None
            
            print(f"✅ Teams: {team_info['away']['abbreviation']} @ {team_info['home']['abbreviation']}")
            return team_info
            
        except Exception as e:
            self.log_error(f"Failed to extract team info: {e}")
            return None
    
    def extract_player_statistics(self, api_data):
        """Extract comprehensive player statistics from API data"""
        try:
            if 'boxscore' not in api_data or 'players' not in api_data['boxscore']:
                self.log_error("No player statistics found in API response")
                return None
            
            players_data = api_data['boxscore']['players']
            all_stats = {}
            
            for team_data in players_data:
                team_info = team_data.get('team', {})
                team_abbr = team_info.get('abbreviation', '')
                
                statistics = team_data.get('statistics', [])
                
                for stat_category in statistics:
                    category_name = stat_category.get('name', '')
                    athletes = stat_category.get('athletes', [])
                    
                    if not athletes:
                        continue
                    
                    # Initialize category if not exists
                    if category_name not in all_stats:
                        all_stats[category_name] = []
                    
                    # Extract player stats
                    for athlete_data in athletes:
                        athlete = athlete_data.get('athlete', {})
                        stats = athlete_data.get('stats', [])
                        
                        if not athlete or not stats:
                            continue
                        
                        player_stat = {
                            'player': athlete.get('displayName', ''),
                            'team': team_abbr,
                            'stat_category': category_name,
                            'jersey': athlete.get('jersey', ''),
                            'position': athlete.get('position', {}).get('abbreviation', '') if athlete.get('position') else ''
                        }
                        
                        # Map stats to labels
                        labels = stat_category.get('labels', [])
                        for i, stat_value in enumerate(stats):
                            if i < len(labels):
                                label = labels[i].lower().replace('/', '_').replace(' ', '_')
                                player_stat[label] = stat_value
                        
                        all_stats[category_name].append(player_stat)
            
            # Validate we have meaningful statistics
            total_players = sum(len(category_stats) for category_stats in all_stats.values())
            if total_players < 10:  # Expect at least 10 player stat entries
                self.log_error(f"Suspiciously few player statistics: {total_players}")
                return None
            
            print(f"✅ Extracted statistics for {total_players} player entries across {len(all_stats)} categories")
            return all_stats
            
        except Exception as e:
            self.log_error(f"Failed to extract player statistics: {e}")
            return None
    
    def check_existing_files(self, game_id, game_date):
        """Check if properly dated files already exist for this game"""
        pattern = f"*{game_date}_{game_id}.csv"
        existing_files = list(self.csv_dir.glob(pattern))
        
        if existing_files:
            print(f"✅ Found {len(existing_files)} existing files for game {game_id} on {game_date}")
            for file in existing_files[:3]:  # Show first 3 files
                print(f"   - {file.name}")
            if len(existing_files) > 3:
                print(f"   ... and {len(existing_files) - 3} more")
            return True
        
        print(f"No existing files found for game {game_id} on {game_date}")
        return False
    
    def save_statistics_to_csv(self, game_id, game_date, team_info, player_stats):
        """Save player statistics to properly named CSV files"""
        try:
            saved_files = []
            
            for category, stats_list in player_stats.items():
                if not stats_list:
                    continue
                
                # Save stats for each team
                for team_type in ['home', 'away']:
                    team_abbr = team_info[team_type]['abbreviation']
                    team_stats = [stat for stat in stats_list if stat.get('team') == team_abbr]
                    
                    if not team_stats:
                        continue
                    
                    # Create properly formatted filename with ACTUAL game date
                    filename = f"nfl_{team_abbr}_{category}_week1_{game_date}_{game_id}.csv"
                    filepath = self.csv_dir / filename
                    
                    # Get fieldnames from first stat entry
                    fieldnames = list(team_stats[0].keys())
                    
                    with open(filepath, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(team_stats)
                    
                    saved_files.append(filename)
                    print(f"✅ Saved: {filename}")
            
            if not saved_files:
                self.log_error("No CSV files were saved")
                return False
            
            print(f"✅ Total files saved: {len(saved_files)}")
            return True
            
        except Exception as e:
            self.log_error(f"Failed to save CSV files: {e}")
            return False
    
    def validate_and_scrape_game(self, game_url):
        """Main validation-first scraping method"""
        print("\n" + "="*80)
        print("🏈 PRODUCTION NFL SCRAPER - VALIDATION-FIRST APPROACH")
        print("="*80)
        print(f"🎯 Target game: {game_url}")
        
        # Reset validation state
        self.validation_errors = []
        
        # Step 1: Extract game ID
        print("\n🔍 Step 1: Extracting game ID...")
        game_id = game_url.split('gameId/')[-1].split('/')[0]
        if not game_id or not game_id.isdigit():
            self.log_error(f"Invalid game ID extracted: {game_id}")
            return False
        print(f"✅ Game ID: {game_id}")
        
        # Step 2: Extract actual game date (CRITICAL)
        print("\n📅 Step 2: Extracting actual game date...")
        game_date = self.extract_actual_game_date(game_url)
        if not game_date:
            print("❌ CRITICAL FAILURE: Could not extract actual game date")
            print("   This would result in files named with scraper run date instead of game date")
            return False
        
        # Step 3: Check for existing files
        print("\n📁 Step 3: Checking for existing files...")
        if self.check_existing_files(game_id, game_date):
            print("✅ Game already processed with correct date - skipping")
            return True
        
        # Step 4: Fetch comprehensive game data
        print("\n📡 Step 4: Fetching comprehensive game data...")
        api_data = self.fetch_espn_summary_api(game_id)
        if not api_data:
            print("❌ CRITICAL FAILURE: Could not fetch game data from ESPN API")
            return False
        
        # Step 5: Extract and validate teams
        print("\n👥 Step 5: Extracting and validating teams...")
        team_info = self.extract_team_info(api_data)
        if not team_info:
            print("❌ CRITICAL FAILURE: Could not extract or validate team information")
            return False
        
        # Step 6: Extract player statistics
        print("\n📊 Step 6: Extracting player statistics...")
        player_stats = self.extract_player_statistics(api_data)
        if not player_stats:
            print("❌ CRITICAL FAILURE: Could not extract player statistics")
            return False
        
        # Step 7: Final validation check
        print("\n🔒 Step 7: Final validation...")
        if self.validation_errors:
            print(f"❌ VALIDATION FAILED with {len(self.validation_errors)} errors:")
            for error in self.validation_errors[:5]:  # Show first 5 errors
                print(f"   - {error}")
            if len(self.validation_errors) > 5:
                print(f"   ... and {len(self.validation_errors) - 5} more errors")
            return False
        
        # Step 8: Save validated data
        print("\n💾 Step 8: Saving validated data...")
        if not self.save_statistics_to_csv(game_id, game_date, team_info, player_stats):
            print("❌ CRITICAL FAILURE: Could not save data to CSV files")
            return False
        
        # Success summary
        print("\n" + "="*80)
        print("✅ SUCCESS: VALIDATION-FIRST SCRAPING COMPLETED")
        print("="*80)
        print(f"🎯 Game: {team_info['away']['abbreviation']} @ {team_info['home']['abbreviation']}")
        print(f"📅 Date: {game_date}")
        print(f"📊 Categories: {', '.join(player_stats.keys())}")
        print(f"📁 Files saved with ACTUAL game date (not scraper run date)")
        print("🔒 All validation checks passed")
        
        return True

def main():
    """Test the production scraper with the problematic WAS vs ARI game"""
    scraper = ProductionNFLScraper()
    
    # Test with the game that had incorrect file naming
    test_url = "https://www.espn.com/nfl/game/_/gameId/401547406"
    
    success = scraper.validate_and_scrape_game(test_url)
    
    if success:
        print("\n🎉 PRODUCTION SCRAPER TEST: SUCCESS")
        print("Expected outcome: CSV files with actual game date (20230910)")
        print("Previous issue: Files were created with scraper run date (20250909/20250910)")
    else:
        print("\n💥 PRODUCTION SCRAPER TEST: FAILED")
        print("Validation-first approach correctly stopped on validation failure")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())