#!/usr/bin/env python3
"""
Production NFL Boxscore Scraper
Clean implementation with proper game date extraction and official team abbreviations.

Features:
- Extracts actual game dates (not current date)
- Uses official NFL team abbreviations (NYG/NYJ differentiation)
- Generates clean CSV filenames
- Saves to BOXSCORE_CSV directory
"""

import os
import re
import time
import random
import logging
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('production_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Official NFL Team Abbreviations (32 teams)
OFFICIAL_NFL_TEAMS = {
    # AFC East
    "Buffalo Bills": "BUF", "Buffalo": "BUF", "Bills": "BUF",
    "Miami Dolphins": "MIA", "Miami": "MIA", "Dolphins": "MIA", 
    "New England Patriots": "NE", "New England": "NE", "Patriots": "NE",
    "New York Jets": "NYJ", "Jets": "NYJ",
    
    # AFC North  
    "Baltimore Ravens": "BAL", "Baltimore": "BAL", "Ravens": "BAL",
    "Cincinnati Bengals": "CIN", "Cincinnati": "CIN", "Bengals": "CIN",
    "Cleveland Browns": "CLE", "Cleveland": "CLE", "Browns": "CLE",
    "Pittsburgh Steelers": "PIT", "Pittsburgh": "PIT", "Steelers": "PIT",
    
    # AFC South
    "Houston Texans": "HOU", "Houston": "HOU", "Texans": "HOU",
    "Indianapolis Colts": "IND", "Indianapolis": "IND", "Colts": "IND",
    "Jacksonville Jaguars": "JAX", "Jacksonville": "JAX", "Jaguars": "JAX",
    "Tennessee Titans": "TEN", "Tennessee": "TEN", "Titans": "TEN",
    
    # AFC West
    "Denver Broncos": "DEN", "Denver": "DEN", "Broncos": "DEN",
    "Kansas City Chiefs": "KC", "Kansas City": "KC", "Chiefs": "KC",
    "Los Angeles Chargers": "LAC", "Chargers": "LAC",
    "Las Vegas Raiders": "LV", "Las Vegas": "LV", "Raiders": "LV",
    
    # NFC East
    "Dallas Cowboys": "DAL", "Dallas": "DAL", "Cowboys": "DAL",
    "New York Giants": "NYG", "Giants": "NYG",
    "Philadelphia Eagles": "PHI", "Philadelphia": "PHI", "Eagles": "PHI", 
    "Washington Commanders": "WSH", "Washington": "WSH", "Commanders": "WSH",
    
    # NFC North
    "Chicago Bears": "CHI", "Chicago": "CHI", "Bears": "CHI",
    "Detroit Lions": "DET", "Detroit": "DET", "Lions": "DET",
    "Green Bay Packers": "GB", "Green Bay": "GB", "Packers": "GB",
    "Minnesota Vikings": "MIN", "Minnesota": "MIN", "Vikings": "MIN",
    
    # NFC South
    "Atlanta Falcons": "ATL", "Atlanta": "ATL", "Falcons": "ATL",
    "Carolina Panthers": "CAR", "Carolina": "CAR", "Panthers": "CAR", 
    "New Orleans Saints": "NO", "New Orleans": "NO", "Saints": "NO",
    "Tampa Bay Buccaneers": "TB", "Tampa Bay": "TB", "Buccaneers": "TB",
    
    # NFC West
    "Arizona Cardinals": "ARI", "Arizona": "ARI", "Cardinals": "ARI",
    "Los Angeles Rams": "LAR", "Rams": "LAR",
    "San Francisco 49ers": "SF", "San Francisco": "SF", "49ers": "SF",
    "Seattle Seahawks": "SEA", "Seattle": "SEA", "Seahawks": "SEA",
}

# Special handling for compound city names that ESPN mangles
TEAM_CITY_MAPPING = {
    "new orleans": "NO", "orleans": "NO",
    "new england": "NE", "england": "NE", 
    "new york giants": "NYG", "york giants": "NYG", "giants": "NYG",
    "new york jets": "NYJ", "york jets": "NYJ", "jets": "NYJ",
    "los angeles rams": "LAR", "angeles rams": "LAR",
    "los angeles chargers": "LAC", "angeles chargers": "LAC",
    "las vegas": "LV", "vegas": "LV",
    "kansas city": "KC", "city": "KC",
    "tampa bay": "TB", "bay": "TB",
    "green bay": "GB",
    "san francisco": "SF", "francisco": "SF",
}

class ProductionNFLBoxscoreScraper:
    def __init__(self, output_dir: str = None):
        """Initialize the production scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Set output directory to BOXSCORE_CSV
        if output_dir is None:
            current_dir = Path(__file__).parent
            self.output_dir = current_dir.parent.parent / 'FootballData' / 'BOXSCORE_CSV'
        else:
            self.output_dir = Path(output_dir)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")
        
        # Track processing
        self.processed_games = []
        self.failed_games = []
        
    def get_team_abbreviation(self, team_name: str, context: str = "", game_id: str = "") -> str:
        """
        Get official NFL team abbreviation with special handling for same-city teams
        """
        if not team_name:
            return "UNK"
            
        team_name = team_name.strip()
        
        # Direct lookup in official mapping
        if team_name in OFFICIAL_NFL_TEAMS:
            return OFFICIAL_NFL_TEAMS[team_name]
        
        # Handle compound names that ESPN mangles
        team_lower = team_name.lower()
        
        # Special handling for New York teams using context
        if "york" in team_lower:
            if "giants" in team_lower or "giants" in context.lower():
                return "NYG"
            elif "jets" in team_lower or "jets" in context.lower():
                return "NYJ"
            else:
                # Try to determine from context or default to Giants
                logger.warning(f"Ambiguous NY team: {team_name}, context: {context}")
                return "NYG"  # Default to Giants if unclear
        
        # Special handling for Los Angeles teams
        if "angeles" in team_lower:
            if "rams" in team_lower or "rams" in context.lower():
                return "LAR"
            elif "chargers" in team_lower or "chargers" in context.lower():
                return "LAC"
            else:
                logger.warning(f"Ambiguous LA team: {team_name}, context: {context}")
                return "LAR"  # Default to Rams if unclear
        
        # Check compound city mappings
        for city, abbr in TEAM_CITY_MAPPING.items():
            if city in team_lower:
                return abbr
        
        # Fuzzy matching for partial names
        for full_name, abbr in OFFICIAL_NFL_TEAMS.items():
            if team_name.lower() in full_name.lower() or full_name.lower() in team_name.lower():
                return abbr
        
        logger.warning(f"Could not map team name: {team_name}")
        return team_name[:3].upper()
    
    def extract_game_date(self, soup: BeautifulSoup, game_url: str) -> str:
        """
        Extract actual game date from ESPN page (not current date)
        """
        try:
            # Method 0: Look for GameInfo__Meta class (most reliable for ESPN)
            game_info_meta = soup.find('div', class_='GameInfo__Meta')
            if game_info_meta:
                date_div = game_info_meta.find('div')
                if date_div:
                    date_text = date_div.get_text().strip()
                    # Parse format like "Friday, September 5, 2025"
                    try:
                        # Remove day of week if present
                        if ',' in date_text:
                            # Handle "Friday, September 5, 2025" format
                            parts = date_text.split(',')
                            if len(parts) >= 2:
                                # Get "September 5, 2025" part
                                date_str = ','.join(parts[1:]).strip()
                                parsed_date = datetime.strptime(date_str, '%B %d, %Y')
                                return parsed_date.strftime('%Y%m%d')
                    except ValueError:
                        pass
            
            # Method 1: Look for game info date
            game_info_sections = soup.find_all(['div', 'span'], class_=re.compile(r'game.*date|date.*game', re.I))
            for section in game_info_sections:
                text = section.get_text().strip()
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4}|\w+\s+\d{1,2},\s+\d{4})', text)
                if date_match:
                    date_str = date_match.group(1)
                    try:
                        if '/' in date_str:
                            parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
                        else:
                            parsed_date = datetime.strptime(date_str, '%B %d, %Y')
                        return parsed_date.strftime('%Y%m%d')
                    except ValueError:
                        continue
            
            # Method 2: Check breadcrumb or navigation
            breadcrumbs = soup.find_all(['nav', 'div'], class_=re.compile(r'breadcrumb|nav', re.I))
            for breadcrumb in breadcrumbs:
                text = breadcrumb.get_text().strip()
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
                if date_match:
                    try:
                        parsed_date = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                        return parsed_date.strftime('%Y%m%d')
                    except ValueError:
                        continue
            
            # Method 3: Check meta tags
            meta_tags = soup.find_all('meta', attrs={'property': re.compile(r'date', re.I)})
            for meta in meta_tags:
                content = meta.get('content', '')
                if content and re.match(r'\d{4}-\d{2}-\d{2}', content):
                    try:
                        parsed_date = datetime.strptime(content[:10], '%Y-%m-%d')
                        return parsed_date.strftime('%Y%m%d')
                    except ValueError:
                        continue
            
            # Method 4: URL pattern analysis
            if 'date=' in game_url:
                url_date = parse_qs(urlparse(game_url).query).get('date', [''])[0]
                if url_date and re.match(r'\d{8}', url_date):
                    return url_date
            
            logger.warning(f"Could not extract game date from {game_url}")
            
        except Exception as e:
            logger.error(f"Error extracting game date: {e}")
        
        # Fallback: use a recognizable placeholder instead of current date
        return "UNKNOWN_DATE"
    
    def clean_stat_category(self, category: str) -> str:
        """
        Clean stat category names for consistent CSV filenames
        """
        if not category:
            return "unknown"
            
        # Convert to lowercase and replace spaces with underscores
        cleaned = category.lower().strip()
        cleaned = re.sub(r'[^a-z0-9]+', '_', cleaned)
        cleaned = cleaned.strip('_')
        
        # Standardize common categories
        category_mapping = {
            'kick_returns': 'kick_returns',
            'punt_returns': 'punt_returns', 
            'field_goals': 'kicking',
            'fg': 'kicking',
            'interceptions': 'interceptions',
            'int': 'interceptions',
            'sacks': 'defensive',
            'tackles': 'defensive'
        }
        
        return category_mapping.get(cleaned, cleaned)
    
    def scrape_game_boxscore(self, game_info: Dict) -> Optional[Dict]:
        """
        Scrape individual game boxscore with improved team and date extraction
        """
        game_id = game_info['game_id']
        boxscore_url = f"https://www.espn.com/nfl/boxscore/_/gameId/{game_id}"
        
        logger.info(f"Scraping game {game_id}: {boxscore_url}")
        
        try:
            # Add delay to respect ESPN servers
            time.sleep(random.uniform(2, 4))
            
            response = self.session.get(boxscore_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract actual game date
            game_date = self.extract_game_date(soup, boxscore_url)
            game_info['extracted_game_date'] = game_date
            
            # Extract team statistics
            all_teams_data = self._extract_team_statistics(soup, game_info)
            
            if all_teams_data:
                self.processed_games.append(game_info)
                return all_teams_data
            else:
                logger.warning(f"No statistics found for game {game_id}")
                self.failed_games.append(game_info)
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch game {game_id}: {e}")
            self.failed_games.append(game_info)
            return None
        except Exception as e:
            logger.error(f"Error processing game {game_id}: {e}")
            self.failed_games.append(game_info)
            return None
    
    def _extract_team_statistics(self, soup: BeautifulSoup, game_info: Dict) -> Optional[Dict]:
        """
        Extract team statistics from ESPN boxscore page
        """
        all_teams_data = {}
        
        # Find all team title sections (ESPN's boxscore structure)
        team_sections = soup.find_all('div', class_='TeamTitle', attrs={'data-testid': 'teamTitle'})
        
        if not team_sections:
            logger.error("Could not find TeamTitle sections")
            return None
        
        logger.info(f"Found {len(team_sections)} team sections")
        
        for section in team_sections:
            team_name_tag = section.find('div', class_='TeamTitle__Name')
            if not team_name_tag:
                continue
            
            team_section_text = team_name_tag.get_text(strip=True)
            logger.debug(f"Processing section: {team_section_text}")
            
            # Parse team name and stat category (e.g., "New York Giants Passing")
            parts = team_section_text.rsplit(' ', 1)  # Split from right to handle compound names
            if len(parts) < 2:
                logger.warning(f"Could not parse team section: {team_section_text}")
                continue
            
            team_name = parts[0]
            stat_category = self.clean_stat_category(parts[1])
            
            # Get team abbreviation with context
            team_abbr = self.get_team_abbreviation(team_name, team_section_text)
            
            logger.debug(f"Mapped '{team_name}' -> '{team_abbr}', category: '{stat_category}'")
            
            # Find associated stats table
            stats_table = section.find_next_sibling('div', class_='ResponsiveTable')
            if not stats_table:
                logger.warning(f"Could not find stats table for {team_section_text}")
                continue
            
            # Extract player statistics
            team_stats = self._extract_player_stats(stats_table, team_abbr, stat_category, game_info)
            
            if team_stats:
                if team_abbr not in all_teams_data:
                    all_teams_data[team_abbr] = {}
                all_teams_data[team_abbr][stat_category] = team_stats
                logger.debug(f"Added {len(team_stats)} {stat_category} records for {team_abbr}")
        
        logger.info(f"Extracted data for {len(all_teams_data)} teams")
        return all_teams_data if all_teams_data else None
    
    def _extract_player_stats(self, stats_table, team_abbr: str, stat_category: str, game_info: Dict) -> List[Dict]:
        """
        Extract player statistics from responsive table
        """
        try:
            # Find player name table and stats table
            player_table = stats_table.find('table', class_='Table--fixed-left')
            stats_scroller = stats_table.find('div', class_='Table__Scroller')
            data_table = stats_scroller.find('table') if stats_scroller else None
            
            if not player_table or not data_table:
                logger.warning(f"Could not find both tables for {team_abbr} {stat_category}")
                return []
            
            # Extract player names
            player_rows = player_table.find_all('tr')[1:]  # Skip header
            players = []
            for row in player_rows:
                name_cell = row.find('td')
                if name_cell:
                    players.append(name_cell.get_text(strip=True))
            
            # Extract stats headers
            header_row = data_table.find('tr')
            if not header_row:
                return []
            
            headers = [th.get_text(strip=True).lower().replace(' ', '_') for th in header_row.find_all(['th', 'td'])]
            
            # Extract stats data
            data_rows = data_table.find_all('tr')[1:]  # Skip header
            
            player_stats = []
            for i, row in enumerate(data_rows):
                if i >= len(players):
                    break
                
                cells = row.find_all(['td', 'th'])
                player_data = {
                    'player': players[i],
                    'team': team_abbr,
                    'stat_category': stat_category,
                    'game_id': game_info['game_id']
                }
                
                for j, cell in enumerate(cells):
                    if j < len(headers):
                        player_data[headers[j]] = cell.get_text(strip=True)
                
                player_stats.append(player_data)
            
            return player_stats
            
        except Exception as e:
            logger.error(f"Error extracting player stats for {team_abbr} {stat_category}: {e}")
            return []
    
    def save_statistics_to_csv(self, all_teams_data: Dict, game_info: Dict) -> List[str]:
        """
        Save statistics to clean CSV files with proper naming
        """
        created_files = []
        # Use URL file date first, then extracted ESPN date, finally fallback to unknown
        game_date = game_info.get('game_date') or game_info.get('extracted_game_date') or 'UNKNOWN_DATE'
        
        for team_abbr, team_data in all_teams_data.items():
            for stat_category, player_stats in team_data.items():
                if not player_stats:
                    continue
                
                # Generate clean filename
                filename = f"nfl_{team_abbr}_{stat_category}_week{game_info['week']}_{game_date}_{game_info['game_id']}.csv"
                csv_path = self.output_dir / filename
                
                try:
                    # Create DataFrame and save
                    df = pd.DataFrame(player_stats)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    created_files.append(str(csv_path))
                    
                    logger.info(f"Saved {len(player_stats)} {stat_category} records for {team_abbr} to {filename}")
                    
                except Exception as e:
                    logger.error(f"Error saving CSV {filename}: {e}")
        
        return created_files
    
    def process_games_from_urls(self, game_urls: List[str], season: int = 2025, week: int = 1) -> Dict:
        """
        Process multiple games from URL list
        """
        logger.info(f"Processing {len(game_urls)} games for Season {season}, Week {week}")
        
        all_created_files = []
        
        for i, game_url in enumerate(game_urls, 1):
            logger.info(f"Processing game {i}/{len(game_urls)}: {game_url}")
            
            # Extract game ID from URL
            game_id_match = re.search(r'/gameId/(\d+)', game_url)
            if not game_id_match:
                logger.warning(f"Could not extract game ID from URL: {game_url}")
                continue
            
            game_info = {
                'game_id': game_id_match.group(1),
                'game_url': game_url,
                'season': season,
                'week': week,
                'status': 'completed'
            }
            
            # Scrape game
            teams_data = self.scrape_game_boxscore(game_info)
            if teams_data:
                created_files = self.save_statistics_to_csv(teams_data, game_info)
                all_created_files.extend(created_files)
        
        # Generate summary
        summary = {
            'success': True,
            'season': season,
            'week': week,
            'total_games': len(game_urls),
            'processed': len(self.processed_games),
            'failed': len(self.failed_games),
            'created_files': all_created_files,
            'processed_games': self.processed_games,
            'failed_games': self.failed_games
        }
        
        logger.info(f"Processing complete: {summary['processed']}/{summary['total_games']} games processed")
        logger.info(f"Created {len(all_created_files)} CSV files")
        
        return summary
    
    def process_games_with_dates(self, url_data: List[Dict], season: int = 2025, week: int = 1) -> Dict:
        """
        Process multiple games from URL list with associated dates from URL file comments
        """
        logger.info(f"Processing {len(url_data)} games for Season {season}, Week {week}")
        
        all_created_files = []
        
        for i, url_info in enumerate(url_data, 1):
            game_url = url_info['url']
            game_date = url_info.get('date')  # May be None
            
            logger.info(f"Processing game {i}/{len(url_data)}: {game_url}")
            if game_date:
                logger.info(f"Using game date: {game_date}")
            
            # Extract game ID from URL
            game_id_match = re.search(r'/gameId/(\d+)', game_url)
            if not game_id_match:
                logger.warning(f"Could not extract game ID from URL: {game_url}")
                continue
            
            game_info = {
                'game_id': game_id_match.group(1),
                'game_url': game_url,
                'season': season,
                'week': week,
                'status': 'completed',
                'game_date': game_date  # Pass the date from URL file
            }
            
            # Scrape game
            teams_data = self.scrape_game_boxscore(game_info)
            if teams_data:
                created_files = self.save_statistics_to_csv(teams_data, game_info)
                all_created_files.extend(created_files)
        
        # Generate summary
        summary = {
            'success': True,
            'season': season,
            'week': week,
            'total_games': len(url_data),
            'processed': len(self.processed_games),
            'failed': len(self.failed_games),
            'created_files': all_created_files,
            'processed_games': self.processed_games,
            'failed_games': self.failed_games
        }
        
        logger.info(f"Processing complete: {summary['processed']}/{summary['total_games']} games processed")
        logger.info(f"Created {len(all_created_files)} CSV files")
        
        return summary

def main():
    """Main entry point for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Production NFL Boxscore Scraper')
    parser.add_argument('--urls', nargs='+', help='Game URLs to scrape')
    parser.add_argument('--url-file', help='File containing game URLs (one per line)')
    parser.add_argument('--season', type=int, default=2025, help='Season year')
    parser.add_argument('--week', type=int, default=1, help='Week number')
    parser.add_argument('--output-dir', help='Output directory (defaults to BOXSCORE_CSV)')
    
    args = parser.parse_args()
    
    # Get URLs from file or command line
    game_urls = []
    if args.url_file:
        try:
            with open(args.url_file, 'r') as f:
                game_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        except FileNotFoundError:
            logger.error(f"URL file not found: {args.url_file}")
            return
    elif args.urls:
        game_urls = args.urls
    else:
        logger.error("Must provide either --urls or --url-file")
        return
    
    # Initialize scraper
    scraper = ProductionNFLBoxscoreScraper(output_dir=args.output_dir)
    
    # Process games
    try:
        summary = scraper.process_games_from_urls(game_urls, args.season, args.week)
        
        if summary['success']:
            print(f"\n✅ Successfully processed NFL games")
            print(f"   Season: {summary['season']}, Week: {summary['week']}")
            print(f"   Games processed: {summary['processed']}/{summary['total_games']}")
            print(f"   Files created: {len(summary['created_files'])}")
            
            if summary['failed']:
                print(f"   ❌ Failed games: {len(summary['failed'])}")
                
        else:
            print(f"\n❌ Failed to process games")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Fatal error: {e}")

if __name__ == "__main__":
    main()