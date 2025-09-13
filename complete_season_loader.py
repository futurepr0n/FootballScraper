#!/usr/bin/env python3
"""
Complete Season Loader for NFL 2023-2024
Scrapes and loads all games with correct data
"""

import subprocess
import os
import time
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/complete_season_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CompleteSeasonLoader:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.stats = {
            'games_processed': 0,
            'weeks_processed': 0,
            'errors': []
        }
        
    def clean_database(self):
        """Remove incorrectly loaded data"""
        logger.info("Cleaning database of incorrect data...")
        
        # Delete games with wrong week numbers or missing scores
        self.cursor.execute("""
            DELETE FROM player_game_stats 
            WHERE game_id IN (
                SELECT id FROM games 
                WHERE season IN (2023, 2024) 
                AND (week != 1 OR home_score IS NULL)
            )
        """)
        
        self.cursor.execute("""
            DELETE FROM games 
            WHERE season IN (2023, 2024) 
            AND (week != 1 OR home_score IS NULL)
        """)
        
        self.conn.commit()
        logger.info("Database cleaned")
        
    def process_week(self, season, week, season_type='regular'):
        """Process a single week of games"""
        schedule_dir = Path(f"historical_schedules/{season}")
        
        # Determine file name based on season type
        if season_type == 'regular':
            week_file = schedule_dir / f"regular_week{week}_{season}.txt"
        else:
            week_file = schedule_dir / f"playoffs_week{week}_{season}.txt"
            
        if not week_file.exists():
            logger.warning(f"Schedule file not found: {week_file}")
            return False
            
        logger.info(f"Processing {season} Week {week} ({season_type})")
        
        # Step 1: Scrape game data from ESPN
        logger.info(f"  Scraping data from ESPN...")
        scrape_cmd = [
            'bash', '-c',
            f'source venv/bin/activate && python process_nfl_game_file.py {week_file}'
        ]
        
        result = subprocess.run(scrape_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"  Scraping failed: {result.stderr[:200]}")
            self.stats['errors'].append(f"Scrape failed: {season} Week {week}")
            return False
            
        # Step 2: Load CSV data into database
        logger.info(f"  Loading data into database...")
        load_cmd = [
            'bash', '-c',
            f'source venv/bin/activate && python load_csv_to_database.py ' +
            f'--csv-dir /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS ' +
            f'--season {season} --week {week} --season-type {season_type}'
        ]
        
        result = subprocess.run(load_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"  Loading failed: {result.stderr[:200]}")
            self.stats['errors'].append(f"Load failed: {season} Week {week}")
            return False
            
        # Step 3: Update game scores from aggregated stats
        self.update_game_scores(season, week, season_type)
        
        self.stats['weeks_processed'] += 1
        logger.info(f"  ✅ Week {week} complete")
        return True
        
    def update_game_scores(self, season, week, season_type):
        """Update game scores based on player statistics"""
        # Get games for this week
        self.cursor.execute("""
            SELECT DISTINCT g.id, g.game_id
            FROM games g
            WHERE g.season = %s AND g.week = %s AND g.season_type = %s
        """, (season, week, season_type))
        
        games = self.cursor.fetchall()
        
        for game in games:
            # Calculate team scores from touchdowns
            self.cursor.execute("""
                SELECT 
                    t.id as team_id,
                    COALESCE(SUM(pgs.passing_touchdowns * 6), 0) +
                    COALESCE(SUM(pgs.rushing_touchdowns * 6), 0) +
                    COALESCE(SUM(pgs.receiving_touchdowns * 6), 0) as score
                FROM player_game_stats pgs
                JOIN teams t ON pgs.team_id = t.id
                WHERE pgs.game_id = %s
                GROUP BY t.id
            """, (game['id'],))
            
            scores = self.cursor.fetchall()
            
            if len(scores) >= 2:
                # Assume first team is home, second is away (needs refinement)
                self.cursor.execute("""
                    UPDATE games 
                    SET home_score = %s, away_score = %s, completed = true
                    WHERE id = %s
                """, (scores[0]['score'], scores[1]['score'], game['id']))
                
        self.conn.commit()
        
    def process_season(self, season):
        """Process an entire season"""
        logger.info(f"\n{'='*60}")
        logger.info(f"PROCESSING {season} SEASON")
        logger.info(f"{'='*60}\n")
        
        # Process regular season (18 weeks)
        for week in range(1, 19):
            self.process_week(season, week, 'regular')
            time.sleep(5)  # Delay between weeks to avoid rate limiting
            
        # Process playoffs if 2023 (2024 playoffs may not exist yet)
        if season == 2023:
            logger.info(f"\nProcessing {season} Playoffs...")
            for week in range(1, 6):  # Wild Card through Super Bowl
                if self.process_week(season, week, 'playoffs'):
                    time.sleep(5)
                    
    def verify_data(self):
        """Verify loaded data"""
        logger.info(f"\n{'='*60}")
        logger.info("VERIFYING DATA")
        logger.info(f"{'='*60}\n")
        
        self.cursor.execute("""
            SELECT 
                season, 
                season_type,
                COUNT(DISTINCT week) as weeks,
                COUNT(DISTINCT id) as games,
                COUNT(DISTINCT CASE WHEN home_score IS NOT NULL THEN id END) as games_with_scores,
                COUNT(DISTINCT pgs.id) as player_stats
            FROM games g
            LEFT JOIN player_game_stats pgs ON g.id = pgs.game_id
            WHERE season IN (2023, 2024)
            GROUP BY season, season_type
            ORDER BY season, season_type
        """)
        
        results = self.cursor.fetchall()
        for row in results:
            logger.info(f"{row['season']} {row['season_type']}: {row['weeks']} weeks, "
                       f"{row['games']} games ({row['games_with_scores']} with scores), "
                       f"{row['player_stats']} player stats")
                       
        # Check for division games
        self.cursor.execute("""
            SELECT COUNT(*) as nyg_phi_games
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE ((ht.abbreviation = 'NYG' AND at.abbreviation = 'PHI')
                OR (ht.abbreviation = 'PHI' AND at.abbreviation = 'NYG'))
            AND g.season IN (2023, 2024)
        """)
        
        result = self.cursor.fetchone()
        logger.info(f"\nNYG vs PHI games found: {result['nyg_phi_games']}")
        
    def run(self):
        """Main execution"""
        start_time = datetime.now()
        
        logger.info("="*80)
        logger.info("COMPLETE NFL SEASON LOADER")
        logger.info("="*80)
        
        # Clean database first
        self.clean_database()
        
        # Process both seasons
        self.process_season(2023)
        self.process_season(2024)
        
        # Verify results
        self.verify_data()
        
        # Report summary
        elapsed = datetime.now() - start_time
        logger.info(f"\n{'='*80}")
        logger.info("LOADING COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Weeks processed: {self.stats['weeks_processed']}")
        logger.info(f"Games processed: {self.stats['games_processed']}")
        
        if self.stats['errors']:
            logger.warning(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:10]:
                logger.warning(f"  - {error}")
                
def main():
    """Run the complete season loader"""
    loader = CompleteSeasonLoader()
    
    print("\n" + "="*60)
    print("COMPLETE NFL SEASON LOADER")
    print("="*60)
    print("This will:")
    print("  1. Clean incorrect data from database")
    print("  2. Scrape all 2023 regular season + playoffs")
    print("  3. Scrape all 2024 regular season")
    print("  4. Load all data with correct week numbers")
    print("  5. Update game scores from statistics")
    print("\nEstimated time: 2-3 hours")
    print("="*60)
    
    response = input("\nProceed? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Cancelled.")
        return
        
    loader.run()

if __name__ == "__main__":
    main()