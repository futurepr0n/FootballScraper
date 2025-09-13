#!/usr/bin/env python3
"""
Update Game Metadata - Fix Week and Season Type
Updates existing games in database with correct week and season_type values from ESPN API
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import json
import time
import random
import os

class GameMetadataUpdater:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        self.updated_games = 0
        self.failed_games = 0
        self.skipped_games = 0
        
        print("🔧 Game Metadata Updater initialized")
        print("🎯 Focus: Update week and season_type for existing games")
    
    def fetch_espn_summary_api(self, game_id):
        """Fetch comprehensive game data from ESPN's summary API"""
        try:
            api_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}"
            print(f"📡 Fetching ESPN API for game {game_id}")
            
            cmd = ['curl', '-s', '--max-time', '15', '-H', 'Accept: application/json', api_url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            
            if result.returncode != 0:
                print(f"❌ ESPN API request failed: curl returned {result.returncode}")
                return None
            
            try:
                data = json.loads(result.stdout)
                return data
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON response from ESPN API: {e}")
                return None
                
        except Exception as e:
            print(f"❌ Failed to fetch ESPN API data: {e}")
            return None
    
    def extract_week_and_season_type(self, api_data):
        """Extract week and season_type from ESPN API data"""
        week = 1  # Default fallback
        season_type = 'regular'  # Default fallback
        
        try:
            # Extract week from API data
            if 'header' in api_data:
                if 'week' in api_data['header']:
                    week = api_data['header']['week']
                elif 'competitions' in api_data['header'] and api_data['header']['competitions']:
                    competition = api_data['header']['competitions'][0]
                    if 'week' in competition:
                        week = competition.get('week', {}).get('number', 1)
            
            # Extract season type from API data
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
                            
        except Exception as e:
            print(f"⚠️ Error extracting metadata: {e}")
        
        return week, season_type
    
    def get_games_to_update(self):
        """Get all games that need metadata updates"""
        try:
            # Get games where week=1 and season_type='regular' (likely incorrect defaults)
            self.cursor.execute("""
                SELECT id, game_id, season, week, season_type, date
                FROM games 
                WHERE (week = 1 AND season_type = 'regular')
                ORDER BY date DESC
            """)
            
            games = self.cursor.fetchall()
            print(f"📊 Found {len(games)} games that need metadata updates")
            return games
            
        except Exception as e:
            print(f"❌ Failed to get games for update: {e}")
            return []
    
    def update_game_metadata(self, game_record):
        """Update a single game's metadata"""
        try:
            game_id = game_record['game_id']
            db_id = game_record['id']
            current_week = game_record['week']
            current_season_type = game_record['season_type']
            
            # Fetch ESPN API data
            api_data = self.fetch_espn_summary_api(game_id)
            if not api_data:
                print(f"❌ {game_id}: Could not fetch API data")
                self.failed_games += 1
                return False
            
            # Extract new metadata
            new_week, new_season_type = self.extract_week_and_season_type(api_data)
            
            # Check if update is needed
            if current_week == new_week and current_season_type == new_season_type:
                print(f"⚠️ {game_id}: No update needed (Week {new_week}, {new_season_type})")
                self.skipped_games += 1
                return True
            
            # Update the database
            self.cursor.execute("""
                UPDATE games 
                SET week = %s, season_type = %s 
                WHERE id = %s
            """, (new_week, new_season_type, db_id))
            
            self.conn.commit()
            self.updated_games += 1
            
            print(f"✅ {game_id}: Updated from Week {current_week}/{current_season_type} → Week {new_week}/{new_season_type}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            print(f"❌ {game_id}: Update failed - {e}")
            self.failed_games += 1
            return False
    
    def run_metadata_update(self, max_games=None):
        """Update metadata for all games"""
        print(f"\n{'='*80}")
        print("🔧 GAME METADATA UPDATER - FIX WEEK AND SEASON TYPE")
        print(f"{'='*80}")
        
        # Get games to update
        games_to_update = self.get_games_to_update()
        
        if not games_to_update:
            print("❌ No games found that need updates")
            return False
        
        # Limit games if specified
        if max_games:
            games_to_update = games_to_update[:max_games]
            print(f"🎯 Processing first {max_games} games for testing")
        
        # Process each game
        for i, game_record in enumerate(games_to_update):
            print(f"\n[{i+1}/{len(games_to_update)}] ===============================")
            
            success = self.update_game_metadata(game_record)
            
            # Rate limiting
            time.sleep(random.uniform(0.5, 1.0))
            
            # Progress update
            if (i + 1) % 10 == 0:
                print(f"\n📊 Progress: {i+1}/{len(games_to_update)} - Updated: {self.updated_games}, Failed: {self.failed_games}, Skipped: {self.skipped_games}")
        
        # Final summary
        print(f"\n{'='*80}")
        print("📊 METADATA UPDATE COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Updated games: {self.updated_games}")
        print(f"❌ Failed games: {self.failed_games}")
        print(f"⚠️ Skipped games: {self.skipped_games}")
        
        total_processed = self.updated_games + self.failed_games
        if total_processed > 0:
            success_rate = (self.updated_games / total_processed) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")
        
        return self.updated_games > 0

def main():
    """Run game metadata updates"""
    updater = GameMetadataUpdater()
    
    # Run metadata updates
    success = updater.run_metadata_update()
    
    if success:
        print("\n🎉 METADATA UPDATE SUCCESSFUL")
        print("Database now contains games with correct week and season_type")
        print("Ready for play-by-play scraping")
    else:
        print("\n💥 METADATA UPDATE FAILED")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())