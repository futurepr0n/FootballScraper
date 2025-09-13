#!/usr/bin/env python3
"""
Fix 2023 NFL Season Scraping
Re-scrape 2023 season properly using our working 2024 process
"""

import subprocess
import time
import logging
from pathlib import Path
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fix_2023_scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Fix2023Scraping:
    def __init__(self):
        self.stats = {
            'weeks_processed': 0,
            'games_scraped': 0,
            'errors': []
        }
        
    def process_week(self, week, season_type='regular'):
        """Process a single week using our proven working method"""
        schedule_dir = Path(f"historical_schedules/2023")
        
        if season_type == 'regular':
            week_file = schedule_dir / f"regular_week{week}_2023.txt"
        else:
            week_file = schedule_dir / f"playoffs_week{week}_2023.txt"
            
        if not week_file.exists():
            logger.warning(f"Schedule file not found: {week_file}")
            return False
            
        logger.info(f"Processing 2023 Week {week} ({season_type})")
        
        # Use our working process_nfl_game_file.py
        scrape_cmd = [
            'bash', '-c',
            f'source venv/bin/activate && python process_nfl_game_file.py {week_file}'
        ]
        
        result = subprocess.run(scrape_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Scraping failed: {result.stderr[:200]}")
            self.stats['errors'].append(f"Week {week} scraping failed")
            return False
            
        # Load into database
        load_cmd = [
            'bash', '-c',
            f'source venv/bin/activate && python load_csv_to_database.py ' +
            f'--csv-dir /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS ' +
            f'--season 2023 --week {week} --season-type {season_type}'
        ]
        
        result = subprocess.run(load_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Loading failed: {result.stderr[:200]}")
            self.stats['errors'].append(f"Week {week} loading failed")
            return False
            
        self.stats['weeks_processed'] += 1
        logger.info(f"✅ Week {week} complete")
        return True
        
    def run(self):
        """Re-scrape 2023 season properly"""
        logger.info("="*80)
        logger.info("FIXING 2023 NFL SEASON SCRAPING")
        logger.info("="*80)
        
        # Test with a single week first
        logger.info("Testing with 2023 Week 1...")
        if not self.process_week(1, 'regular'):
            logger.error("Week 1 test failed, aborting")
            return
            
        # Continue with remaining regular season
        for week in range(2, 19):
            if not self.process_week(week, 'regular'):
                logger.warning(f"Week {week} failed, continuing...")
            time.sleep(3)  # Rate limiting
            
        # Process playoffs
        logger.info("Processing 2023 playoffs...")
        for week in range(1, 6):
            if self.process_week(week, 'playoffs'):
                time.sleep(3)
                
        logger.info(f"Completed: {self.stats['weeks_processed']} weeks processed")
        if self.stats['errors']:
            logger.warning(f"Errors: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                logger.warning(f"  - {error}")

def main():
    fixer = Fix2023Scraping()
    fixer.run()

if __name__ == "__main__":
    main()