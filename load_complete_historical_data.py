#!/usr/bin/env python3
"""
Complete Historical Data Loader for NFL 2023-2024 Seasons
Loads all regular season and playoff games with correct week numbers and play-by-play data
"""

import subprocess
import time
import logging
from pathlib import Path
import sys
import json
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/complete_historical_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CompleteHistoricalLoader:
    def __init__(self):
        self.historical_dir = Path("historical_schedules")
        self.stats = {
            'seasons_processed': [],
            'total_games_loaded': 0,
            'total_weeks_loaded': 0,
            'errors': []
        }
        
    def run_command(self, cmd: list, description: str) -> bool:
        """Run a command and return success status"""
        try:
            logger.info(f"Running: {description}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Failed: {description}")
                logger.error(f"Error: {result.stderr}")
                self.stats['errors'].append(f"{description}: {result.stderr[:200]}")
                return False
                
            # Parse output for game count if available
            try:
                for line in result.stdout.split('\n'):
                    if line.strip().startswith('{'):
                        data = json.loads(line)
                        if 'successful' in data:
                            self.stats['total_games_loaded'] += data['successful']
                            logger.info(f"Loaded {data['successful']} games")
                            break
            except:
                pass
                
            return True
            
        except Exception as e:
            logger.error(f"Exception running {description}: {e}")
            self.stats['errors'].append(f"{description}: {str(e)}")
            return False
    
    def fetch_season_schedules(self, year: int) -> bool:
        """Fetch all schedules for a season (regular + playoffs)"""
        logger.info(f"\n{'='*60}")
        logger.info(f"FETCHING SCHEDULES FOR {year} SEASON")
        logger.info(f"{'='*60}\n")
        
        success = True
        
        # Check if regular season already exists
        regular_dir = self.historical_dir / str(year)
        if regular_dir.exists() and len(list(regular_dir.glob("regular_week*.txt"))) >= 18:
            logger.info(f"Regular season schedules for {year} already exist, skipping fetch")
        else:
            # Fetch regular season
            cmd = [
                'bash', '-c',
                f'source venv/bin/activate && python historical_nfl_scraper.py --season {year} --season-type 2'
            ]
            if not self.run_command(cmd, f"Fetching {year} regular season schedules"):
                success = False
            
            # Wait a bit before fetching playoffs
            time.sleep(5)
        
        # Check if playoffs already exist
        if regular_dir.exists() and len(list(regular_dir.glob("playoffs_week*.txt"))) > 0:
            logger.info(f"Playoff schedules for {year} already exist, skipping fetch")
        else:
            # Fetch playoffs
            cmd = [
                'bash', '-c',
                f'source venv/bin/activate && python historical_nfl_scraper.py --season {year} --season-type 3'
            ]
            if not self.run_command(cmd, f"Fetching {year} playoffs schedules"):
                logger.warning(f"Could not fetch {year} playoffs (may not exist yet)")
        
        return success
    
    def load_season_data(self, year: int) -> bool:
        """Load all data for a season with correct week numbers"""
        logger.info(f"\n{'='*60}")
        logger.info(f"LOADING DATA FOR {year} SEASON")
        logger.info(f"{'='*60}\n")
        
        season_dir = self.historical_dir / str(year)
        if not season_dir.exists():
            logger.error(f"No data directory found for {year}")
            return False
        
        games_loaded = 0
        weeks_loaded = 0
        
        # Load regular season week by week for correct week numbers
        logger.info(f"Loading {year} regular season...")
        for week in range(1, 19):
            week_file = season_dir / f"regular_week{week}_{year}.txt"
            
            if week_file.exists():
                cmd = [
                    'bash', '-c',
                    f'source venv/bin/activate && python historical_data_loader.py --file {week_file}'
                ]
                
                if self.run_command(cmd, f"Loading {year} Week {week}"):
                    weeks_loaded += 1
                    self.stats['total_weeks_loaded'] += 1
                    
                # Small delay between weeks to avoid overwhelming the database
                time.sleep(3)
            else:
                logger.warning(f"Week {week} file not found for {year}")
        
        logger.info(f"Loaded {weeks_loaded} weeks of regular season for {year}")
        
        # Load playoffs
        logger.info(f"Loading {year} playoffs...")
        playoff_files = [
            f"playoffs_week1_{year}.txt",  # Wild Card
            f"playoffs_week2_{year}.txt",  # Divisional
            f"playoffs_week3_{year}.txt",  # Conference Championships
            f"playoffs_week4_{year}.txt",  # Pro Bowl (if exists)
            f"playoffs_week5_{year}.txt",  # Super Bowl
        ]
        
        playoffs_loaded = 0
        for playoff_file in playoff_files:
            file_path = season_dir / playoff_file
            if file_path.exists():
                cmd = [
                    'bash', '-c',
                    f'source venv/bin/activate && python historical_data_loader.py --file {file_path}'
                ]
                
                if self.run_command(cmd, f"Loading {playoff_file}"):
                    playoffs_loaded += 1
                    
                time.sleep(3)
        
        logger.info(f"Loaded {playoffs_loaded} playoff weeks for {year}")
        
        self.stats['seasons_processed'].append({
            'year': year,
            'regular_weeks': weeks_loaded,
            'playoff_weeks': playoffs_loaded
        })
        
        return True
    
    def verify_data(self):
        """Verify data in database"""
        logger.info(f"\n{'='*60}")
        logger.info("VERIFYING LOADED DATA")
        logger.info(f"{'='*60}\n")
        
        cmd = [
            'bash', '-c',
            '''PGPASSWORD=korn5676 psql -h 192.168.1.23 -U postgres -d football_tracker -c "
            SELECT season, season_type, 
                   COUNT(DISTINCT week) as weeks, 
                   COUNT(*) as games,
                   COUNT(DISTINCT pgs.id) as player_stats
            FROM games g 
            LEFT JOIN player_game_stats pgs ON g.id = pgs.game_id 
            WHERE season IN (2023, 2024) 
            GROUP BY season, season_type 
            ORDER BY season, season_type;"'''
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, shell=False)
        if result.returncode == 0:
            logger.info("Database verification:")
            logger.info(result.stdout)
        else:
            logger.error(f"Could not verify database: {result.stderr}")
    
    def run(self):
        """Main execution"""
        start_time = datetime.now()
        
        logger.info("="*80)
        logger.info("COMPLETE HISTORICAL DATA LOADER FOR NFL 2023-2024")
        logger.info("="*80)
        logger.info("This will:")
        logger.info("1. Fetch all 2023 & 2024 schedules (regular + playoffs)")
        logger.info("2. Load games with correct week numbers")
        logger.info("3. Include play-by-play data automatically")
        logger.info("="*80)
        
        # Process 2023
        if self.fetch_season_schedules(2023):
            self.load_season_data(2023)
        
        # Process 2024
        if self.fetch_season_schedules(2024):
            self.load_season_data(2024)
        
        # Verify what we loaded
        self.verify_data()
        
        # Report summary
        elapsed = datetime.now() - start_time
        logger.info(f"\n{'='*80}")
        logger.info("LOADING COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Seasons processed: {[s['year'] for s in self.stats['seasons_processed']]}")
        logger.info(f"Total weeks loaded: {self.stats['total_weeks_loaded']}")
        logger.info(f"Total games loaded: {self.stats['total_games_loaded']}")
        
        if self.stats['errors']:
            logger.warning(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  - {error[:100]}")
        
        logger.info("="*80)

def main():
    """Run the complete historical loader"""
    loader = CompleteHistoricalLoader()
    
    # Confirm before running
    print("\n" + "="*60)
    print("COMPLETE NFL HISTORICAL DATA LOADER")
    print("="*60)
    print("This will load:")
    print("  - 2023 Regular Season (18 weeks)")
    print("  - 2023 Playoffs (Wild Card through Super Bowl)")
    print("  - 2024 Regular Season (18 weeks)")
    print("  - 2024 Playoffs (if available)")
    print("\nEstimated time: 45-60 minutes")
    print("="*60)
    
    response = input("\nProceed? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Cancelled.")
        return
    
    loader.run()

if __name__ == "__main__":
    main()