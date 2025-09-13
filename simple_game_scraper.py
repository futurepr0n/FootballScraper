#!/usr/bin/env python3
"""
Simple Game Scraper - Focus on Core Game Data
Gets game results with proper scores for historical matchup analysis
Skips complex player stats to avoid transaction issues
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import json
import time
import random
import os
from datetime import datetime

class SimpleGameScraper:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        self.successful_games = 0
        self.failed_games = 0
        
        print("🏈 Simple Game Scraper initialized")
        print("🎯 Focus: Core game data with proper scores")
    
    def fetch_espn_scoreboard_api(self, season, season_type='2', week=None):
        """Fetch games from ESPN scoreboard API"""
        try:
            if week:
                api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype={season_type}&week={week}&year={season}"
            else:
                api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype={season_type}&year={season}"
            
            print(f"📡 Fetching: Season {season}, Week {week}")
            
            cmd = ['curl', '-s', '--max-time', '15', '-H', 'Accept: application/json', api_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            
            if result.returncode != 0:
                print(f"❌ API request failed: {result.returncode}")
                return None
            
            try:
                data = json.loads(result.stdout)
                if 'events' not in data:
                    print("❌ No events found in response")
                    return None
                
                print(f"✅ Found {len(data['events'])} games")
                return data['events']
                
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON: {e}")
                return None
                
        except Exception as e:
            print(f"❌ API fetch failed: {e}")
            return None
    
    def get_team_id(self, team_abbr):
        """Get team ID from abbreviation"""
        try:
            self.cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr.upper(),))
            result = self.cursor.fetchone()
            return result['id'] if result else None
        except:
            return None
    
    def store_game(self, game_event):
        """Store game with validation"""
        try:
            # Extract basic info
            game_id = game_event['id']
            
            # Get teams and scores
            competitions = game_event.get('competitions', [])
            if not competitions:
                print(f"❌ {game_id}: No competitions")
                return False
            
            competition = competitions[0]
            competitors = competition.get('competitors', [])
            if len(competitors) != 2:
                print(f"❌ {game_id}: Expected 2 competitors, found {len(competitors)}")
                return False
            
            # Extract team info and scores
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
                print(f"❌ {game_id}: Could not identify teams")
                return False
            
            # Get team IDs
            home_team_id = self.get_team_id(home_team)
            away_team_id = self.get_team_id(away_team)
            
            if not home_team_id or not away_team_id:
                print(f"❌ {game_id}: Invalid team IDs for {home_team}/{away_team}")
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
            
            # Extract game date
            game_date = None
            if 'date' in game_event:
                game_date = game_event['date'][:10]  # Extract YYYY-MM-DD
            else:
                game_date = datetime.now().strftime('%Y-%m-%d')
            
            # Check for existing game
            self.cursor.execute("SELECT id FROM games WHERE game_id = %s", (game_id,))
            if self.cursor.fetchone():
                print(f"⚠️ {game_id}: Already exists")
                return False
            
            # Insert game
            self.cursor.execute("""
                INSERT INTO games (game_id, date, season, week, season_type, home_team_id, away_team_id, home_score, away_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (game_id, game_date, season, week, season_type_name, home_team_id, away_team_id, home_score, away_score))
            
            self.conn.commit()
            self.successful_games += 1
            
            print(f"✅ {game_id}: {away_team} {away_score} @ {home_team} {home_score} ({season} Week {week})")
            return True
            
        except Exception as e:
            self.conn.rollback()
            print(f"❌ {game_id}: Failed to store - {e}")
            self.failed_games += 1
            return False
    
    def scrape_season_games(self, season, weeks):
        """Scrape games for a specific season and weeks"""
        print(f"\n🗓️ Processing {season} season...")
        
        for week in weeks:
            print(f"\n📅 Week {week}")
            
            # Fetch games
            games = self.fetch_espn_scoreboard_api(season, season_type='2', week=week)
            
            if not games:
                print(f"   ⚠️ No games found")
                continue
            
            # Process each game
            for game_event in games:
                game_id = game_event.get('id')
                if game_id:
                    self.store_game(game_event)
                    time.sleep(random.uniform(0.5, 1.0))  # Rate limiting
            
            print(f"   📊 Week {week}: {self.successful_games} total successful")
    
    def run_comprehensive_scrape(self):
        """Run comprehensive scraping for multiple seasons"""
        print(f"\n{'='*60}")
        print("🏈 SIMPLE GAME SCRAPER - COMPREHENSIVE DATA")
        print(f"{'='*60}")
        
        # Define scraping plan with focus on recent complete seasons
        scraping_plan = [
            {'season': 2023, 'weeks': list(range(1, 19))},  # Full 2023 season
            {'season': 2024, 'weeks': list(range(1, 19))},  # Full 2024 season  
            {'season': 2025, 'weeks': [1, 2, 3]}           # Current 2025 games
        ]
        
        for plan in scraping_plan:
            self.scrape_season_games(plan['season'], plan['weeks'])
        
        # Final summary
        print(f"\n{'='*60}")
        print("📊 SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"✅ Successful games: {self.successful_games}")
        print(f"❌ Failed games: {self.failed_games}")
        
        if self.successful_games > 0:
            success_rate = (self.successful_games/(self.successful_games+self.failed_games)*100)
            print(f"📈 Success rate: {success_rate:.1f}%")
            return True
        
        return False

def main():
    """Run simple game scraping"""
    scraper = SimpleGameScraper()
    success = scraper.run_comprehensive_scrape()
    
    if success:
        print("\n🎉 SIMPLE SCRAPING SUCCESSFUL")
        print("Database now contains games with proper scores")
        print("Ready for historical matchup analysis")
    else:
        print("\n💥 SIMPLE SCRAPING FAILED")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())