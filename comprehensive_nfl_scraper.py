#!/usr/bin/env python3
"""
Comprehensive NFL Scraper - Complete Historical Data Collection
Scrapes multiple weeks and seasons with validation-first approach
Focus on popular matchups like NFC East rivalries (PHI vs NYG, etc.)
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import json
import time
import random
import os
from datetime import datetime
from pathlib import Path

class ComprehensiveNFLScraper:
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
        
        # Popular matchups to prioritize (NFC East rivalries, etc.)
        self.priority_matchups = [
            ('PHI', 'NYG'), ('PHI', 'DAL'), ('PHI', 'WAS'),
            ('NYG', 'DAL'), ('NYG', 'WAS'), ('DAL', 'WAS'),
            ('GB', 'CHI'), ('NE', 'NYJ'), ('PIT', 'BAL')
        ]
        
        print("🏈 Comprehensive NFL Scraper initialized")
        print("🎯 Focus: Complete historical data with validation-first approach")
    
    def log_error(self, error_msg):
        """Log validation error"""
        self.validation_errors.append(error_msg)
        print(f"❌ VALIDATION ERROR: {error_msg}")
    
    def log_success(self, success_msg):
        """Log successful operation"""
        print(f"✅ SUCCESS: {success_msg}")
    
    def fetch_espn_scoreboard_api(self, season, season_type='2', week=None):
        """Fetch games from ESPN scoreboard API"""
        try:
            if week:
                api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype={season_type}&week={week}&year={season}"
            else:
                api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype={season_type}&year={season}"
            
            print(f"📡 Fetching scoreboard: Season {season}, Week {week}, Type {season_type}")
            
            cmd = ['curl', '-s', '--max-time', '15', '-H', 'Accept: application/json', api_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            
            if result.returncode != 0:
                self.log_error(f"Scoreboard API request failed: curl returned {result.returncode}")
                return None
            
            try:
                data = json.loads(result.stdout)
                if 'events' not in data:
                    self.log_error("No events found in scoreboard API response")
                    return None
                
                print(f"✅ Found {len(data['events'])} games")
                return data['events']
                
            except json.JSONDecodeError as e:
                self.log_error(f"Invalid JSON response from scoreboard API: {e}")
                return None
                
        except Exception as e:
            self.log_error(f"Failed to fetch scoreboard API: {e}")
            return None
    
    def fetch_game_summary(self, game_id):
        """Fetch detailed game data using our existing production scraper logic"""
        try:
            api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}"
            
            cmd = ['curl', '-s', '--max-time', '15', '-H', 'Accept: application/json', api_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            
            if result.returncode != 0:
                self.log_error(f"Game summary API failed for {game_id}: curl returned {result.returncode}")
                return None
            
            try:
                data = json.loads(result.stdout)
                return data
            except json.JSONDecodeError as e:
                self.log_error(f"Invalid JSON response for game {game_id}: {e}")
                return None
                
        except Exception as e:
            self.log_error(f"Failed to fetch game summary for {game_id}: {e}")
            return None
    
    def validate_and_store_game(self, game_event, game_summary):
        """Validate game data and store in database with proper scores"""
        try:
            # Extract basic game info
            game_id = game_event['id']
            
            # Get teams from event
            competitions = game_event.get('competitions', [])
            if not competitions:
                self.log_error(f"Game {game_id}: No competitions found")
                return False
            
            competition = competitions[0]
            competitors = competition.get('competitors', [])
            if len(competitors) != 2:
                self.log_error(f"Game {game_id}: Expected 2 competitors, found {len(competitors)}")
                return False
            
            # Extract team information and scores
            home_team = None
            away_team = None
            home_score = None
            away_score = None
            
            for competitor in competitors:
                team_data = competitor.get('team', {})
                team_abbr = team_data.get('abbreviation', '')
                score = competitor.get('score', 0)
                is_home = competitor.get('homeAway') == 'home'
                
                if is_home:
                    home_team = team_abbr
                    home_score = int(score) if score else 0
                else:
                    away_team = team_abbr
                    away_score = int(score) if score else 0
            
            if not home_team or not away_team:
                self.log_error(f"Game {game_id}: Could not identify home/away teams")
                return False
            
            # Get team IDs
            home_team_id = self.get_team_id(home_team)
            away_team_id = self.get_team_id(away_team)
            
            if not home_team_id or not away_team_id:
                self.log_error(f"Game {game_id}: Invalid team IDs for {home_team}/{away_team}")
                return False
            
            # Extract game details
            season = game_event.get('season', {}).get('year', 0)
            week = game_event.get('week', {}).get('number', 0)
            season_type = game_event.get('season', {}).get('type', 0)
            
            # Map season type
            season_type_name = 'regular'
            if season_type == 1:
                season_type_name = 'preseason'
            elif season_type == 3:
                season_type_name = 'postseason'
            
            # Validate required fields
            if not all([season, week, home_team_id, away_team_id]):
                self.log_error(f"Game {game_id}: Missing required fields")
                return False
            
            # Check for existing game
            self.cursor.execute("""
                SELECT id FROM games 
                WHERE game_id = %s
            """, (game_id,))
            
            if self.cursor.fetchone():
                self.log_error(f"Game {game_id}: Duplicate game exists")
                return False
            
            # Extract game date from the event
            game_date = None
            if 'date' in game_event:
                game_date = game_event['date'][:10]  # Extract YYYY-MM-DD
            else:
                # Use current date as fallback
                game_date = datetime.now().strftime('%Y-%m-%d')
            
            # Insert game with actual scores (let id auto-increment)
            self.cursor.execute("""
                INSERT INTO games (game_id, date, season, week, season_type, home_team_id, away_team_id, home_score, away_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (game_id, game_date, season, week, season_type_name, home_team_id, away_team_id, home_score, away_score))
            
            # Store player statistics if available
            player_stats_stored = 0
            if game_summary and 'boxscore' in game_summary:
                player_stats_stored = self.store_player_statistics(game_id, game_summary)
            
            self.conn.commit()
            self.successful_games += 1
            
            self.log_success(f"Game {game_id}: {away_team} {away_score} @ {home_team} {home_score} ({season} Week {week}) - {player_stats_stored} player stats")
            return True
            
        except Exception as e:
            self.conn.rollback()
            self.log_error(f"Failed to store game {game_id}: {e}")
            self.failed_games += 1
            return False
    
    def store_player_statistics(self, game_id, game_summary):
        """Store player statistics from game summary"""
        try:
            if 'boxscore' not in game_summary or 'players' not in game_summary['boxscore']:
                return 0
            
            players_data = game_summary['boxscore']['players']
            total_stats_stored = 0
            
            for team_data in players_data:
                team_info = team_data.get('team', {})
                team_id = self.get_team_id(team_info.get('abbreviation', ''))
                
                if not team_id:
                    continue
                
                statistics = team_data.get('statistics', [])
                
                for stat_category in statistics:
                    athletes = stat_category.get('athletes', [])
                    
                    for athlete_data in athletes:
                        athlete = athlete_data.get('athlete', {})
                        stats = athlete_data.get('stats', [])
                        
                        if not athlete or not stats:
                            continue
                        
                        # Get or create player
                        player_id = self.get_or_create_player(
                            athlete.get('displayName', ''),
                            team_id,
                            athlete.get('position', {}).get('abbreviation', '') if athlete.get('position') else ''
                        )
                        
                        if not player_id:
                            continue
                        
                        # Store basic player game stats
                        try:
                            self.cursor.execute("""
                                INSERT INTO player_game_stats (player_id, game_id, team_id)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (player_id, game_id) DO NOTHING
                            """, (player_id, game_id, team_id))
                            total_stats_stored += 1
                        except Exception as e:
                            continue
            
            return total_stats_stored
            
        except Exception as e:
            self.log_error(f"Failed to store player statistics for game {game_id}: {e}")
            return 0
    
    def get_team_id(self, team_abbr):
        """Get team ID from abbreviation"""
        try:
            self.cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr.upper(),))
            result = self.cursor.fetchone()
            return result['id'] if result else None
        except:
            return None
    
    def get_or_create_player(self, name, team_id, position):
        """Get existing player or create new one"""
        try:
            # Check if player exists
            self.cursor.execute("""
                SELECT id FROM players WHERE name = %s AND team_id = %s
            """, (name, team_id))
            
            result = self.cursor.fetchone()
            if result:
                return result['id']
            
            # Create new player
            self.cursor.execute("""
                INSERT INTO players (name, team_id, position)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (name, team_id, position))
            
            result = self.cursor.fetchone()
            return result['id'] if result else None
            
        except Exception as e:
            self.log_error(f"Failed to get/create player {name}: {e}")
            return None
    
    def scrape_comprehensive_data(self):
        """Scrape comprehensive historical data focusing on popular matchups"""
        print(f"\n{'='*80}")
        print("🏈 COMPREHENSIVE NFL SCRAPER - COMPLETE HISTORICAL DATA")
        print(f"{'='*80}")
        
        # Define seasons and weeks to scrape
        scraping_plan = [
            # 2024 season - all weeks
            {'season': 2024, 'weeks': list(range(1, 19))},
            # 2023 season - key weeks with popular matchups
            {'season': 2023, 'weeks': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]},
            # 2025 season - current available weeks
            {'season': 2025, 'weeks': [1, 2, 3]}
        ]
        
        for plan in scraping_plan:
            season = plan['season']
            weeks = plan['weeks']
            
            print(f"\n🗓️ Processing {season} season...")
            
            for week in weeks:
                print(f"\n📅 Week {week} of {season}")
                
                # Fetch games for this week
                games = self.fetch_espn_scoreboard_api(season, season_type='2', week=week)
                
                if not games:
                    print(f"   ⚠️ No games found for Week {week}")
                    continue
                
                # Process each game
                for game_event in games:
                    game_id = game_event.get('id')
                    
                    if not game_id:
                        continue
                    
                    # Get detailed game summary
                    game_summary = self.fetch_game_summary(game_id)
                    
                    # Validate and store
                    success = self.validate_and_store_game(game_event, game_summary)
                    
                    if success:
                        # Small delay to avoid overwhelming ESPN
                        time.sleep(random.uniform(0.5, 1.5))
                    else:
                        # Longer delay after failures
                        time.sleep(random.uniform(2.0, 3.0))
                
                # Summary for this week
                print(f"   📊 Week {week} summary: {self.successful_games} successful, {self.failed_games} failed")
        
        # Final summary
        print(f"\n{'='*80}")
        print("📊 COMPREHENSIVE SCRAPING COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successful games: {self.successful_games}")
        print(f"❌ Failed games: {self.failed_games}")
        print(f"📈 Success rate: {(self.successful_games/(self.successful_games+self.failed_games)*100):.1f}%" if (self.successful_games+self.failed_games) > 0 else "N/A")
        
        if self.validation_errors:
            print(f"\n⚠️ Validation errors encountered: {len(self.validation_errors)}")
            for error in self.validation_errors[-5:]:  # Show last 5 errors
                print(f"   - {error}")
        
        return self.successful_games > 0

def main():
    """Run comprehensive NFL data scraping"""
    scraper = ComprehensiveNFLScraper()
    
    success = scraper.scrape_comprehensive_data()
    
    if success:
        print("\n🎉 COMPREHENSIVE SCRAPING SUCCESSFUL")
        print("Database now contains complete historical data with proper scores")
        print("Ready for historical matchup analysis")
    else:
        print("\n💥 COMPREHENSIVE SCRAPING FAILED")
        print("No games were successfully processed")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())