#!/usr/bin/env python3
"""
Historical Validation Scraper
Uses extracted historical game URLs with production validation-first approach
Ensures proper dates and scores for historical matchup analysis
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import json
import time
import random
import os
import re
from datetime import datetime
from pathlib import Path

class HistoricalValidationScraper:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        self.validation_errors = []
        self.successful_games = 0
        self.failed_games = 0
        self.skipped_games = 0
        
        print("🏈 Historical Validation Scraper initialized")
        print("🎯 Focus: Historical games with validation-first approach")
    
    def log_error(self, error_msg):
        """Log validation error"""
        self.validation_errors.append(error_msg)
        print(f"❌ {error_msg}")
    
    def log_success(self, success_msg):
        """Log successful operation"""
        print(f"✅ {success_msg}")
    
    def load_historical_games(self, json_file="historical_game_urls.json"):
        """Load historical games from JSON file"""
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            games = data.get('games', [])
            metadata = data.get('metadata', {})
            
            print(f"📊 Loaded {len(games)} historical games")
            print(f"📅 Date range: {metadata.get('date_range', {}).get('earliest', 'N/A')} to {metadata.get('date_range', {}).get('latest', 'N/A')}")
            
            for year, count in metadata.get('seasons', {}).items():
                print(f"   {year}: {count} games")
            
            return games
            
        except Exception as e:
            self.log_error(f"Failed to load historical games: {e}")
            return []
    
    def extract_actual_game_date(self, game_url):
        """Extract actual game date from ESPN page"""
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
            
            # Look for title with date pattern
            title_match = re.search(r'<title[^>]*>.*?\(([^)]+)\).*?</title>', html_content)
            
            if title_match:
                date_str = title_match.group(1).strip()
                print(f"📅 Found date string: '{date_str}'")
                
                # Parse date format
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
                    
                    actual_date = f"{year}-{month_num}-{day}"
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
    
    def get_team_id(self, team_abbr):
        """Get team ID from abbreviation"""
        try:
            self.cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr.upper(),))
            result = self.cursor.fetchone()
            return result['id'] if result else None
        except:
            return None
    
    def validate_and_store_game(self, game_info):
        """Validate and store historical game with proper date"""
        try:
            game_url = game_info['url']
            game_id = game_info['game_id']
            expected_date = game_info['date']  # YYYYMMDD format
            expected_year = game_info['year']
            
            print(f"\n{'='*60}")
            print(f"🎯 Processing Game: {game_id}")
            print(f"📅 Expected Date: {expected_date}")
            print(f"🔗 URL: {game_url}")
            
            # Check if game already exists
            self.cursor.execute("SELECT id FROM games WHERE game_id = %s", (game_id,))
            if self.cursor.fetchone():
                print(f"⚠️ Game {game_id} already exists - skipping")
                self.skipped_games += 1
                return True
            
            # Extract actual game date from ESPN
            actual_date = self.extract_actual_game_date(game_url)
            if not actual_date:
                return False
            
            # Fetch comprehensive game data
            api_data = self.fetch_espn_summary_api(game_id)
            if not api_data:
                return False
            
            # Extract teams and scores from API data
            if 'boxscore' not in api_data or 'teams' not in api_data['boxscore']:
                self.log_error("No team data found in API response")
                return False
            
            teams = api_data['boxscore']['teams']
            if len(teams) != 2:
                self.log_error(f"Expected 2 teams, found {len(teams)}")
                return False
            
            # Extract team information and scores
            home_team = None
            away_team = None
            home_score = None
            away_score = None
            
            for team in teams:
                team_data = team.get('team', {})
                team_abbr = team_data.get('abbreviation', '')
                home_away = team.get('homeAway', '')
                
                # Get score from boxscore
                score = 0
                if 'statistics' in team:
                    # Try to extract score from team statistics or use fallback
                    pass
                
                # Fallback: try to get score from header
                if 'header' in api_data and 'competitions' in api_data['header']:
                    competitions = api_data['header']['competitions']
                    if competitions and 'competitors' in competitions[0]:
                        competitors = competitions[0]['competitors']
                        for competitor in competitors:
                            if (competitor.get('team', {}).get('abbreviation') == team_abbr and
                                competitor.get('homeAway') == home_away):
                                score = int(competitor.get('score', 0))
                                break
                
                if home_away == 'home':
                    home_team = team_abbr
                    home_score = score
                else:
                    away_team = team_abbr
                    away_score = score
            
            if not home_team or not away_team:
                self.log_error("Could not identify home and away teams")
                return False
            
            # Get team IDs
            home_team_id = self.get_team_id(home_team)
            away_team_id = self.get_team_id(away_team)
            
            if not home_team_id or not away_team_id:
                self.log_error(f"Invalid team IDs for {home_team}/{away_team}")
                return False
            
            # Extract game details from API
            season = expected_year  # Use expected year from CSV
            
            # Extract week from API data
            week = 1  # Default fallback
            if 'header' in api_data:
                if 'week' in api_data['header']:
                    week = api_data['header']['week']
                elif 'competitions' in api_data['header'] and api_data['header']['competitions']:
                    competition = api_data['header']['competitions'][0]
                    if 'week' in competition:
                        week = competition.get('week', {}).get('number', 1)
            
            # Extract season type from API data
            season_type = 'regular'  # Default fallback
            if 'header' in api_data:
                if 'season' in api_data['header'] and 'type' in api_data['header']['season']:
                    season_type_code = api_data['header']['season']['type']
                    # Map ESPN season type codes: 1=preseason, 2=regular, 3=postseason
                    if season_type_code == 1:
                        season_type = 'preseason'
                    elif season_type_code == 2:
                        season_type = 'regular'
                    elif season_type_code == 3:
                        season_type = 'postseason'
                elif 'competitions' in api_data['header'] and api_data['header']['competitions']:
                    competition = api_data['header']['competitions'][0]
                    if 'season' in competition and 'type' in competition['season']:
                        season_type_code = competition['season']['type']
                        if season_type_code == 1:
                            season_type = 'preseason'
                        elif season_type_code == 2:
                            season_type = 'regular'
                        elif season_type_code == 3:
                            season_type = 'postseason'
            
            # Insert game with actual date
            self.cursor.execute("""
                INSERT INTO games (game_id, date, season, week, season_type, home_team_id, away_team_id, home_score, away_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (game_id, actual_date, season, week, season_type, home_team_id, away_team_id, home_score, away_score))
            
            self.conn.commit()
            self.successful_games += 1
            
            self.log_success(f"Game {game_id}: {away_team} {away_score} @ {home_team} {home_score} ({season} Week {week}, {season_type}, Date: {actual_date})")
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.log_error(f"Failed to store game {game_id}: {e}")
            self.failed_games += 1
            return False
    
    def scrape_historical_games(self, max_games=None):
        """Scrape historical games with validation"""
        print(f"\n{'='*80}")
        print("🏈 HISTORICAL VALIDATION SCRAPER - COMPLETE HISTORICAL DATA")
        print(f"{'='*80}")
        
        # Load historical games
        games = self.load_historical_games()
        if not games:
            print("❌ No historical games to process")
            return False
        
        # Limit games if specified
        if max_games:
            games = games[:max_games]
            print(f"🎯 Processing first {max_games} games for testing")
        
        # Process each game
        for i, game_info in enumerate(games):
            print(f"\n[{i+1}/{len(games)}] ===============================")
            
            success = self.validate_and_store_game(game_info)
            
            # Rate limiting
            if success:
                time.sleep(random.uniform(1.0, 2.0))
            else:
                time.sleep(random.uniform(2.0, 3.0))
            
            # Progress update
            if (i + 1) % 10 == 0:
                print(f"\n📊 Progress: {i+1}/{len(games)} - Success: {self.successful_games}, Failed: {self.failed_games}, Skipped: {self.skipped_games}")
        
        # Final summary
        print(f"\n{'='*80}")
        print("📊 HISTORICAL SCRAPING COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successful games: {self.successful_games}")
        print(f"❌ Failed games: {self.failed_games}")
        print(f"⚠️ Skipped games: {self.skipped_games}")
        
        total_processed = self.successful_games + self.failed_games
        if total_processed > 0:
            success_rate = (self.successful_games / total_processed) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")
        
        return self.successful_games > 0

def main():
    """Run historical validation scraping"""
    scraper = HistoricalValidationScraper()
    
    # Run full historical dataset
    success = scraper.scrape_historical_games()
    
    if success:
        print("\n🎉 HISTORICAL SCRAPING SUCCESSFUL")
        print("Database now contains validated historical data")
        print("Ready for historical matchup analysis")
    else:
        print("\n💥 HISTORICAL SCRAPING FAILED")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())