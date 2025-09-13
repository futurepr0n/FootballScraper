#!/usr/bin/env python3
"""
Historical NFL Data Backfill Script
Intelligently backfills historical NFL data with human-like scraping patterns
to avoid detection.
"""

import argparse
import random
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import json
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backfill_historical.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HistoricalBackfiller:
    """Manages intelligent backfilling of historical NFL data"""
    
    def __init__(self, start_year: int = 2002, end_year: int = 2023):
        self.start_year = start_year
        self.end_year = end_year
        self.status_file = Path("historical_schedules/backfill_status.json")
        self.load_status()
        
        # Human-like patterns
        self.work_hours = {
            'start': 9,  # Start after 9 AM
            'end': 23,   # Stop before 11 PM
            'break_lunch': (12, 13),  # Lunch break
            'break_dinner': (18, 19)  # Dinner break
        }
        
    def load_status(self):
        """Load backfill status from file"""
        if self.status_file.exists():
            with open(self.status_file, 'r') as f:
                self.status = json.load(f)
        else:
            self.status = {
                'completed_seasons': [],
                'in_progress': None,
                'last_run': None,
                'total_games_loaded': 0
            }
    
    def save_status(self):
        """Save backfill status to file"""
        self.status_file.parent.mkdir(exist_ok=True)
        with open(self.status_file, 'w') as f:
            json.dump(self.status, f, indent=2)
    
    def is_work_time(self) -> bool:
        """Check if current time is within 'work hours' to appear human"""
        now = datetime.now()
        hour = now.hour
        
        # Check if it's a break time
        if self.work_hours['break_lunch'][0] <= hour < self.work_hours['break_lunch'][1]:
            logger.info("Lunch break time - pausing scraping")
            return False
        if self.work_hours['break_dinner'][0] <= hour < self.work_hours['break_dinner'][1]:
            logger.info("Dinner break time - pausing scraping")
            return False
        
        # Check if within work hours
        if hour < self.work_hours['start'] or hour >= self.work_hours['end']:
            logger.info(f"Outside work hours ({hour}:00) - pausing scraping")
            return False
            
        return True
    
    def get_random_delay(self, min_seconds: float = 30, max_seconds: float = 180) -> float:
        """Get a random delay with occasional longer pauses"""
        # 10% chance of a longer pause (simulating distraction/other tasks)
        if random.random() < 0.1:
            delay = random.uniform(300, 600)  # 5-10 minute break
            logger.info(f"Taking a longer break ({delay/60:.1f} minutes)...")
        else:
            delay = random.uniform(min_seconds, max_seconds)
        
        return delay
    
    def fetch_season(self, year: int, season_type: int = 2) -> bool:
        """Fetch a single season's data"""
        try:
            logger.info(f"Fetching {year} season (type {season_type})...")
            
            # First, fetch the schedule (using virtual environment)
            cmd = [
                'bash', '-c',
                f'source venv/bin/activate && python historical_nfl_scraper.py --season {year} --season-type {season_type}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Failed to fetch schedule for {year}: {result.stderr}")
                return False
            
            # Random delay before loading data
            delay = self.get_random_delay(20, 60)
            logger.info(f"Waiting {delay:.1f} seconds before loading data...")
            time.sleep(delay)
            
            # Load the data into database
            season_type_name = {1: 'preseason', 2: 'regular', 3: 'playoffs'}[season_type]
            data_file = f"historical_schedules/{year}/{season_type_name}_season_{year}.txt"
            
            if Path(data_file).exists():
                # Activate virtual environment for database operations
                cmd = [
                    'bash', '-c',
                    f'source venv/bin/activate && python historical_data_loader.py --file {data_file}'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode != 0:
                    logger.error(f"Failed to load data for {year}: {result.stderr}")
                    return False
                
                # Parse result for game count
                try:
                    # Find the JSON line in output (starts with {)
                    for line in result.stdout.split('\n'):
                        if line.strip().startswith('{'):
                            result_json = json.loads(line)
                            games_loaded = result_json.get('successful', 0)
                            self.status['total_games_loaded'] += games_loaded
                            logger.info(f"Successfully loaded {games_loaded} games from {year}")
                            break
                except Exception as e:
                    logger.warning(f"Could not parse game count from output: {e}")
                    # Still count as success if command succeeded
                    logger.info(f"Data loading completed for {year} (game count unavailable)")
                
                return True
            else:
                logger.warning(f"No data file found for {year}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing {year}: {e}")
            return False
    
    def backfill_random_pattern(self):
        """Backfill with random pattern to avoid detection"""
        # Get list of years to process
        years_to_process = []
        for year in range(self.start_year, self.end_year + 1):
            if year not in self.status['completed_seasons']:
                years_to_process.append(year)
        
        # Randomize order to avoid sequential pattern
        random.shuffle(years_to_process)
        
        logger.info(f"Starting backfill for {len(years_to_process)} seasons")
        logger.info(f"Years to process: {sorted(years_to_process)}")
        
        sessions_completed = 0
        max_sessions_per_run = random.randint(3, 7)  # Process 3-7 seasons per run
        
        for year in years_to_process:
            # Check if still within work hours
            if not self.is_work_time():
                wait_time = self.calculate_wait_until_work()
                logger.info(f"Pausing until work hours resume (waiting {wait_time/3600:.1f} hours)")
                time.sleep(wait_time)
            
            # Update status
            self.status['in_progress'] = year
            self.status['last_run'] = datetime.now().isoformat()
            self.save_status()
            
            # Fetch the season
            success = self.fetch_season(year, season_type=2)  # Regular season
            
            if success:
                self.status['completed_seasons'].append(year)
                sessions_completed += 1
                logger.info(f"Completed {year} ({sessions_completed}/{max_sessions_per_run} in this session)")
            
            # Clear in-progress
            self.status['in_progress'] = None
            self.save_status()
            
            # Check if we should stop this session
            if sessions_completed >= max_sessions_per_run:
                logger.info(f"Completed {sessions_completed} seasons in this session. Taking extended break.")
                break
            
            # Random delay between seasons
            delay = self.get_random_delay(60, 300)  # 1-5 minutes between seasons
            logger.info(f"Waiting {delay/60:.1f} minutes before next season...")
            time.sleep(delay)
        
        logger.info(f"Backfill session complete. Processed {sessions_completed} seasons.")
        logger.info(f"Total games loaded: {self.status['total_games_loaded']}")
        logger.info(f"Remaining seasons: {len(years_to_process) - sessions_completed}")
    
    def calculate_wait_until_work(self) -> float:
        """Calculate seconds to wait until work hours resume"""
        now = datetime.now()
        
        # If it's before work starts
        if now.hour < self.work_hours['start']:
            target = now.replace(hour=self.work_hours['start'], minute=random.randint(0, 30))
        # If it's after work ends
        else:
            # Wait until tomorrow morning
            tomorrow = now + timedelta(days=1)
            target = tomorrow.replace(hour=self.work_hours['start'], minute=random.randint(0, 30))
        
        wait_seconds = (target - now).total_seconds()
        return max(0, wait_seconds)
    
    def run_continuous(self):
        """Run continuous backfill with human-like patterns"""
        while True:
            # Check remaining work
            remaining = len([y for y in range(self.start_year, self.end_year + 1) 
                           if y not in self.status['completed_seasons']])
            
            if remaining == 0:
                logger.info("All seasons have been backfilled!")
                break
            
            logger.info(f"{remaining} seasons remaining to backfill")
            
            # Run a backfill session
            self.backfill_random_pattern()
            
            # Extended break between sessions (30 min to 2 hours)
            break_time = random.uniform(1800, 7200)
            logger.info(f"Taking extended break between sessions ({break_time/3600:.1f} hours)")
            time.sleep(break_time)

def main():
    parser = argparse.ArgumentParser(description='Backfill historical NFL data with human-like patterns')
    parser.add_argument('--start-year', type=int, default=2020,
                       help='Start year for backfill (default: 2020)')
    parser.add_argument('--end-year', type=int, default=2023,
                       help='End year for backfill (default: 2023)')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuously until all data is backfilled')
    parser.add_argument('--single', action='store_true',
                       help='Run a single backfill session')
    
    args = parser.parse_args()
    
    backfiller = HistoricalBackfiller(args.start_year, args.end_year)
    
    if args.continuous:
        logger.info("Starting continuous backfill mode")
        backfiller.run_continuous()
    elif args.single:
        logger.info("Running single backfill session")
        backfiller.backfill_random_pattern()
    else:
        # Default: run single session
        backfiller.backfill_random_pattern()
    
    logger.info("Backfill complete!")

if __name__ == "__main__":
    main()