#!/usr/bin/env python3
"""
Historical NFL Schedule and Data Scraper
Fetches historical NFL game schedules from ESPN and generates game URL files
for bulk processing. Supports seasons from 2002 onwards.

Usage:
    python historical_nfl_scraper.py --season 2023 --season-type 2
    python historical_nfl_scraper.py --season 2023 --season-type 2 --weeks 1-9
    python historical_nfl_scraper.py --start-season 2020 --end-season 2023
"""

import argparse
import requests
from bs4 import BeautifulSoup
import json
import time
import random
import logging
from pathlib import Path
from datetime import datetime
import re
from typing import List, Dict, Optional, Tuple
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/historical_nfl_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HistoricalNFLScraper:
    """Scraper for historical NFL schedules and game data from ESPN"""
    
    BASE_URL = "https://www.espn.com/nfl/schedule/_/week/{week}/year/{year}/seasontype/{season_type}"
    GAME_URL_BASE = "https://www.espn.com/nfl/game/_/gameId/{game_id}"
    
    # Season type mappings
    SEASON_TYPES = {
        'preseason': 1,
        'regular': 2,
        'playoffs': 3,
        'postseason': 3
    }
    
    # Week limits by season type
    WEEK_LIMITS = {
        1: 4,   # Preseason: 4 weeks
        2: 18,  # Regular season: 18 weeks (17 prior to 2021)
        3: 5    # Playoffs: up to 5 weeks (Wild Card, Divisional, Conference, Pro Bowl, Super Bowl)
    }
    
    def __init__(self, output_dir: str = "historical_schedules"):
        """Initialize the scraper with output directory"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def get_season_weeks(self, year: int, season_type: int) -> int:
        """Determine number of weeks for a season"""
        if season_type == 2:  # Regular season
            # NFL expanded to 18 weeks (17 games + bye) in 2021
            return 18 if year >= 2021 else 17
        return self.WEEK_LIMITS.get(season_type, 1)
    
    def fetch_week_schedule(self, year: int, week: int, season_type: int) -> List[Dict]:
        """Fetch all games for a specific week"""
        url = self.BASE_URL.format(week=week, year=year, season_type=season_type)
        logger.info(f"Fetching {year} Week {week} (type {season_type}): {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            games = []
            
            # ESPN's schedule page structure
            # Look for game links
            game_links = soup.find_all('a', href=re.compile(r'/nfl/game/_/gameId/\d+'))
            
            for link in game_links:
                href = link.get('href')
                game_id_match = re.search(r'gameId/(\d+)', href)
                if game_id_match:
                    game_id = game_id_match.group(1)
                    
                    # Try to extract team info from nearby elements
                    parent = link.find_parent('div', class_=['Table__TR', 'Table__TD', 'gameblock'])
                    teams_text = parent.text if parent else ""
                    
                    game_info = {
                        'game_id': game_id,
                        'url': f"https://www.espn.com{href}",
                        'year': year,
                        'week': week,
                        'season_type': season_type,
                        'teams_text': teams_text[:100]  # Truncate for logging
                    }
                    
                    # Avoid duplicates
                    if game_id not in [g['game_id'] for g in games]:
                        games.append(game_info)
            
            # Alternative parsing method if no games found
            if not games:
                logger.warning(f"No games found with primary method, trying alternative parsing")
                # Look for game containers with different class names
                game_containers = soup.find_all(['div', 'article'], class_=re.compile(r'game|schedule|matchup', re.I))
                
                for container in game_containers:
                    links = container.find_all('a', href=re.compile(r'gameId/(\d+)'))
                    for link in links:
                        href = link.get('href')
                        game_id_match = re.search(r'gameId/(\d+)', href)
                        if game_id_match:
                            game_id = game_id_match.group(1)
                            game_info = {
                                'game_id': game_id,
                                'url': f"https://www.espn.com{href}",
                                'year': year,
                                'week': week,
                                'season_type': season_type,
                                'teams_text': container.text[:100]
                            }
                            if game_id not in [g['game_id'] for g in games]:
                                games.append(game_info)
            
            logger.info(f"Found {len(games)} games for Week {week}")
            return games
            
        except requests.RequestException as e:
            logger.error(f"Error fetching week {week}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error processing week {week}: {e}")
            return []
    
    def fetch_season_schedule(self, year: int, season_type: int, weeks: Optional[List[int]] = None) -> Dict:
        """Fetch all games for a complete season"""
        season_type_name = {1: 'preseason', 2: 'regular', 3: 'playoffs'}.get(season_type, 'unknown')
        logger.info(f"Fetching {year} {season_type_name} season schedule")
        
        max_weeks = self.get_season_weeks(year, season_type)
        if weeks:
            weeks_to_fetch = [w for w in weeks if 1 <= w <= max_weeks]
        else:
            weeks_to_fetch = list(range(1, max_weeks + 1))
        
        season_games = {
            'year': year,
            'season_type': season_type,
            'season_type_name': season_type_name,
            'weeks': {}
        }
        
        for week in weeks_to_fetch:
            games = self.fetch_week_schedule(year, week, season_type)
            if games:
                season_games['weeks'][week] = games
            
            # Random delay between requests to appear human
            delay = random.uniform(2.5, 7.5)  # Random 2.5-7.5 seconds
            logger.info(f"Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)
        
        total_games = sum(len(games) for games in season_games['weeks'].values())
        logger.info(f"Fetched {total_games} total games for {year} {season_type_name} season")
        
        return season_games
    
    def save_season_urls(self, season_data: Dict) -> str:
        """Save season game URLs to organized text files"""
        year = season_data['year']
        season_type_name = season_data['season_type_name']
        
        # Create season directory
        season_dir = self.output_dir / str(year)
        season_dir.mkdir(exist_ok=True)
        
        saved_files = []
        
        # Save each week to a separate file
        for week, games in season_data['weeks'].items():
            filename = f"{season_type_name}_week{week}_{year}.txt"
            filepath = season_dir / filename
            
            with open(filepath, 'w') as f:
                f.write(f"# NFL {season_type_name.title()} Week {week}, {year} Game URLs\n")
                f.write(f"# Generated: {datetime.now().isoformat()}\n")
                f.write(f"# Format: One ESPN game URL per line\n")
                f.write(f"# Use: python process_nfl_game_file.py {filename}\n\n")
                f.write(f"# {len(games)} games scheduled for this week\n")
                
                for game in games:
                    f.write(f"{game['url']}\n")
            
            saved_files.append(str(filepath))
            logger.info(f"Saved {len(games)} game URLs to {filepath}")
        
        # Also save a combined file for the entire season
        combined_filename = f"{season_type_name}_season_{year}.txt"
        combined_filepath = season_dir / combined_filename
        
        with open(combined_filepath, 'w') as f:
            f.write(f"# NFL {season_type_name.title()} Season {year} - All Game URLs\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n\n")
            
            for week, games in sorted(season_data['weeks'].items()):
                f.write(f"\n# Week {week} ({len(games)} games)\n")
                for game in games:
                    f.write(f"{game['url']}\n")
        
        logger.info(f"Saved combined season file to {combined_filepath}")
        
        # Save JSON metadata
        metadata_file = season_dir / f"{season_type_name}_metadata_{year}.json"
        with open(metadata_file, 'w') as f:
            json.dump(season_data, f, indent=2)
        
        return str(season_dir)
    
    def fetch_multiple_seasons(self, start_year: int, end_year: int, 
                             season_types: List[int] = None) -> None:
        """Fetch data for multiple seasons"""
        if season_types is None:
            season_types = [2]  # Default to regular season only
        
        logger.info(f"Fetching seasons from {start_year} to {end_year}")
        
        for year in range(start_year, end_year + 1):
            for season_type in season_types:
                try:
                    season_data = self.fetch_season_schedule(year, season_type)
                    if season_data['weeks']:
                        self.save_season_urls(season_data)
                        
                        # Save progress
                        self.update_import_status(year, season_type, 'completed')
                    else:
                        logger.warning(f"No data found for {year} season type {season_type}")
                        self.update_import_status(year, season_type, 'no_data')
                        
                except Exception as e:
                    logger.error(f"Error processing {year} season type {season_type}: {e}")
                    self.update_import_status(year, season_type, 'failed', str(e))
                
                # Longer random delay between seasons
                season_delay = random.uniform(10, 30)  # 10-30 seconds between seasons
                logger.info(f"Waiting {season_delay:.1f} seconds before next season...")
                time.sleep(season_delay)
    
    def update_import_status(self, year: int, season_type: int, status: str, error: str = None):
        """Update import status in tracking file"""
        status_file = self.output_dir / "import_status.json"
        
        try:
            if status_file.exists():
                with open(status_file, 'r') as f:
                    status_data = json.load(f)
            else:
                status_data = {}
            
            key = f"{year}_{season_type}"
            status_data[key] = {
                'year': year,
                'season_type': season_type,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'error': error
            }
            
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error updating status file: {e}")

def parse_week_range(week_str: str) -> List[int]:
    """Parse week range string like '1-9' or '1,3,5-7'"""
    weeks = []
    parts = week_str.split(',')
    
    for part in parts:
        if '-' in part:
            start, end = map(int, part.split('-'))
            weeks.extend(range(start, end + 1))
        else:
            weeks.append(int(part))
    
    return sorted(set(weeks))

def main():
    parser = argparse.ArgumentParser(description='Fetch historical NFL schedules from ESPN')
    parser.add_argument('--season', type=int, help='Single season year to fetch')
    parser.add_argument('--start-season', type=int, help='Start year for multiple seasons')
    parser.add_argument('--end-season', type=int, help='End year for multiple seasons')
    parser.add_argument('--season-type', type=int, default=2, 
                       help='Season type: 1=preseason, 2=regular, 3=playoffs')
    parser.add_argument('--weeks', type=str, help='Week range (e.g., "1-9" or "1,3,5-7")')
    parser.add_argument('--output-dir', type=str, default='historical_schedules',
                       help='Output directory for game URL files')
    parser.add_argument('--all-types', action='store_true',
                       help='Fetch all season types (preseason, regular, playoffs)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.season and not (args.start_season and args.end_season):
        parser.error('Either --season or both --start-season and --end-season required')
    
    # Initialize scraper
    scraper = HistoricalNFLScraper(args.output_dir)
    
    # Parse weeks if provided
    weeks = None
    if args.weeks:
        weeks = parse_week_range(args.weeks)
    
    # Determine season types
    if args.all_types:
        season_types = [1, 2, 3]
    else:
        season_types = [args.season_type]
    
    # Execute scraping
    if args.season:
        # Single season
        for season_type in season_types:
            season_data = scraper.fetch_season_schedule(args.season, season_type, weeks)
            if season_data['weeks']:
                scraper.save_season_urls(season_data)
    else:
        # Multiple seasons
        scraper.fetch_multiple_seasons(args.start_season, args.end_season, season_types)
    
    logger.info("Historical scraping completed!")

if __name__ == "__main__":
    main()