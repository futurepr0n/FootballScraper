#!/usr/bin/env python3
"""
Enhanced NFL Scraper - ESPN Schedule and Game Data Collection
Adapted from BaseballScraper architecture for NFL weekly structure and ESPN integration.

This scraper:
1. Fetches NFL schedule from ESPN for specified weeks
2. Processes individual game box scores for player statistics
3. Outputs CSV files with comprehensive player data
4. Supports preseason, regular season, and playoff games
5. Handles postponed/delayed games gracefully

Usage:
  python enhanced_nfl_scraper.py                    # Process current week
  python enhanced_nfl_scraper.py --week 1 --season 2024  # Process specific week
  python enhanced_nfl_scraper.py --preseason --week 2    # Process preseason week 2
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import random
import time
from urllib.parse import urlparse, urljoin
from datetime import datetime, timedelta
import os
import json
import argparse
import sys
import logging
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# Import centralized configuration
from config import PATHS, ESPN_NFL_BASE, ESPN_NFL_SCORES, NFL_REGULAR_SEASON_WEEKS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/nfl_scrape.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# NFL team abbreviations and name mappings
NFL_TEAM_ABBREVIATIONS = {
    "Arizona Cardinals": "ARI", "Cardinals": "ARI", "ARI": "ARI",
    "Atlanta Falcons": "ATL", "Falcons": "ATL", "ATL": "ATL", 
    "Baltimore Ravens": "BAL", "Baltimore": "BAL", "Ravens": "BAL", "BAL": "BAL",
    "Buffalo Bills": "BUF", "Bills": "BUF", "BUF": "BUF",
    "Carolina Panthers": "CAR", "Panthers": "CAR", "CAR": "CAR",
    "Chicago Bears": "CHI", "Bears": "CHI", "CHI": "CHI",
    "Cincinnati Bengals": "CIN", "Bengals": "CIN", "CIN": "CIN",
    "Cleveland Browns": "CLE", "Browns": "CLE", "CLE": "CLE",
    "Dallas Cowboys": "DAL", "Cowboys": "DAL", "DAL": "DAL",
    "Denver Broncos": "DEN", "Broncos": "DEN", "DEN": "DEN",
    "Detroit Lions": "DET", "Lions": "DET", "DET": "DET",
    "Green Bay Packers": "GB", "Packers": "GB", "GB": "GB", "GNB": "GB",
    "Houston Texans": "HOU", "Texans": "HOU", "HOU": "HOU",
    "Indianapolis Colts": "IND", "Indianapolis": "IND", "Colts": "IND", "IND": "IND",
    "Jacksonville Jaguars": "JAX", "Jaguars": "JAX", "JAX": "JAX", "JAC": "JAX",
    "Kansas City Chiefs": "KC", "Chiefs": "KC", "KC": "KC",
    "Las Vegas Raiders": "LV", "Raiders": "LV", "LV": "LV", "OAK": "LV",
    "Los Angeles Chargers": "LAC", "Chargers": "LAC", "LAC": "LAC",
    "Los Angeles Rams": "LAR", "Rams": "LAR", "LAR": "LAR",
    "Miami Dolphins": "MIA", "Dolphins": "MIA", "MIA": "MIA",
    "Minnesota Vikings": "MIN", "Vikings": "MIN", "MIN": "MIN",
    "New England Patriots": "NE", "Patriots": "NE", "NE": "NE", "NEP": "NE",
    "New Orleans Saints": "NO", "Saints": "NO", "NO": "NO",
    "New York Giants": "NYG", "Giants": "NYG", "NYG": "NYG",
    "New York Jets": "NYJ", "Jets": "NYJ", "NYJ": "NYJ",
    "Philadelphia Eagles": "PHI", "Eagles": "PHI", "PHI": "PHI",
    "Pittsburgh Steelers": "PIT", "Steelers": "PIT", "PIT": "PIT",
    "San Francisco 49ers": "SF", "49ers": "SF", "SF": "SF",
    "Seattle Seahawks": "SEA", "Seahawks": "SEA", "SEA": "SEA",
    "Tampa Bay Buccaneers": "TB", "Buccaneers": "TB", "TB": "TB", "TAM": "TB",
    "Tennessee Titans": "TEN", "Titans": "TEN", "TEN": "TEN",
    "Washington Commanders": "WAS", "Commanders": "WAS", "WAS": "WAS", "WSH": "WAS"
}

def get_team_abbr(team_name: str) -> str:
    """Get standard team abbreviation from various team name formats"""
    if not team_name:
        return "UNK"
    
    # Clean the team name
    cleaned_name = team_name.strip()
    
    # Handle known ESPN malformed names first
    cleaned_lower = cleaned_name.lower()
    
    # Special handling for angeles (distinguish LAC from LAR)
    if 'angeles' in cleaned_lower:
        # If it already starts with LAC, keep it as LAC (Chargers)
        if cleaned_name.upper().startswith('LAC'):
            return 'LAC'
        else:
            return 'LAR'  # Default angeles to Rams
    
    # Other ESPN malformed mappings
    espn_malformed_mappings = {
        'orleans': 'NO',  # "NE orleans" → "NO"
        'vegas': 'LV',    # "DAL vegas" → "LV" 
        'england': 'NE',  # "NE england" → "NE"
    }
    
    # Check for other ESPN malformed patterns
    for malformed_key, correct_abbr in espn_malformed_mappings.items():
        if malformed_key in cleaned_lower:
            return correct_abbr
    
    # Direct lookup
    if cleaned_name in NFL_TEAM_ABBREVIATIONS:
        return NFL_TEAM_ABBREVIATIONS[cleaned_name]
    
    # Try team nickname (last word)
    words = cleaned_name.split()
    if words:
        nickname = words[-1]
        if nickname in NFL_TEAM_ABBREVIATIONS:
            return NFL_TEAM_ABBREVIATIONS[nickname]
    
    # Try first word (for cases like "Dallas Cowboys")
    if words:
        first_word = words[0]
        if first_word in NFL_TEAM_ABBREVIATIONS:
            return NFL_TEAM_ABBREVIATIONS[first_word]
    
    # Enhanced fuzzy matching for city/team combinations
    for full_name, abbr in NFL_TEAM_ABBREVIATIONS.items():
        full_lower = full_name.lower()
        # Check if any word in the team name matches
        for word in words:
            if word.lower() in full_lower:
                return abbr
        # Check reverse - if full name contains our word
        if cleaned_lower in full_lower or full_lower in cleaned_lower:
            return abbr
    
    logger.warning(f"Team abbreviation not found for: '{team_name}', using default")
    return team_name[:3].upper()

class EnhancedNFLScraper:
    """Enhanced NFL scraper with ESPN integration and centralized output"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Ensure required directories exist
        self._ensure_directories()
        
        # Track processed games and errors
        self.processed_games = []
        self.failed_games = []
        self.postponed_games = []
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        directories = [
            PATHS['csv_backups'],
            PATHS['scanned'],
            Path('logs')
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_current_nfl_week(self, season: int = None) -> Tuple[int, int, int]:
        """
        Determine current NFL week based on date
        Returns (season_year, season_type, week_number)
        """
        now = datetime.now()
        year = season or now.year
        
        # Determine season type and week based on current date
        month = now.month
        day = now.day
        
        if month == 8 or (month == 9 and day <= 10):
            # Preseason (August - early September)
            return year, 1, min(4, max(1, (now - datetime(year, 8, 1)).days // 7 + 1))
        elif month >= 9 and month <= 12:
            # Regular season (September - December)
            season_start = datetime(year, 9, 7)  # Approximate NFL season start
            week = min(NFL_REGULAR_SEASON_WEEKS, max(1, (now - season_start).days // 7 + 1))
            return year, 2, week
        elif month <= 2:
            # Playoffs/off-season (January - February)
            if month == 1 or (month == 2 and day <= 14):
                # Playoffs
                return year - 1, 3, min(5, max(1, (datetime(year, 1, 1) - datetime(year - 1, 12, 31)).days // 7 + 1))
            else:
                # Off-season
                return year, 1, 1
        else:
            # Off-season
            return year, 1, 1
    
    def fetch_nfl_schedule(self, season: int, season_type: int, week: int) -> List[Dict]:
        """
        Fetch NFL schedule from ESPN for specified week
        
        Args:
            season: Season year
            season_type: 1=Preseason, 2=Regular Season, 3=Playoffs
            week: Week number within season type
            
        Returns:
            List of game info dictionaries
        """
        logger.info(f"Fetching NFL schedule for {season} season, type {season_type}, week {week}")
        
        # Construct ESPN URL using the schedule format
        url = f"https://www.espn.com/nfl/schedule/_/week/{week}/year/{season}/seasontype/{season_type}"
        logger.info(f"Fetching from URL: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            games = self._parse_espn_schedule(soup, season, season_type, week)
            
            logger.info(f"Found {len(games)} games for week {week}")
            return games
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch NFL schedule: {e}")
            return []
    
    def _parse_espn_schedule(self, soup: BeautifulSoup, season: int, season_type: int, week: int) -> List[Dict]:
        """Parse ESPN schedule page for game information"""
        games = []
        
        # Look for table rows containing game information
        # ESPN schedule uses table structure with team matchups
        table_rows = soup.find_all('tr')
        
        for row in table_rows:
            try:
                # Look for rows that contain team links (game matchups)
                team_links = row.find_all('a', href=re.compile(r'/nfl/team/'))
                
                if len(team_links) >= 2:
                    # Extract teams from the links
                    away_team = self._extract_team_from_link(team_links[0])
                    home_team = self._extract_team_from_link(team_links[1])
                    
                    if away_team and home_team:
                        # Look for game result/status and extract game ID
                        game_link = row.find('a', href=re.compile(r'/nfl/game/'))
                        if game_link:
                            game_url = game_link.get('href')
                            if game_url and not game_url.startswith('http'):
                                game_url = urljoin(ESPN_NFL_BASE, game_url)
                            
                            # Extract game ID from URL
                            game_id_match = re.search(r'/game/[^/]*?(\d+)', game_url)
                            game_id = game_id_match.group(1) if game_id_match else None
                            
                            if game_id:
                                game_info = {
                                    'game_id': game_id,
                                    'game_url': game_url,
                                    'away_team': away_team,
                                    'home_team': home_team,
                                    'season': season,
                                    'season_type': season_type,
                                    'week': week,
                                    'status': 'completed'  # Most preseason games are completed
                                }
                                
                                games.append(game_info)
                                logger.debug(f"Found game: {away_team} @ {home_team} (ID: {game_id})")
                        
            except Exception as e:
                logger.warning(f"Error parsing game row: {e}")
                continue
        
        # Alternative parsing: Look for direct game links
        if not games:
            logger.info("Trying alternative parsing method...")
            game_links = soup.find_all('a', href=re.compile(r'/nfl/game/'))
            
            for link in game_links:
                try:
                    game_url = link.get('href')
                    if game_url and not game_url.startswith('http'):
                        game_url = urljoin(ESPN_NFL_BASE, game_url)
                    
                    # Extract game ID
                    game_id_match = re.search(r'/game/[^/]*?(\d+)', game_url)
                    game_id = game_id_match.group(1) if game_id_match else None
                    
                    if game_id:
                        # Try to extract teams from surrounding context
                        parent = link.parent
                        teams_found = []
                        
                        # Look for team elements in the parent container
                        team_elements = parent.find_all('a', href=re.compile(r'/nfl/team/'))
                        for team_elem in team_elements:
                            team = self._extract_team_from_link(team_elem)
                            if team and team not in teams_found:
                                teams_found.append(team)
                        
                        if len(teams_found) >= 2:
                            game_info = {
                                'game_id': game_id,
                                'game_url': game_url,
                                'away_team': teams_found[0],
                                'home_team': teams_found[1],
                                'season': season,
                                'season_type': season_type,
                                'week': week,
                                'status': 'completed'
                            }
                            
                            games.append(game_info)
                            logger.debug(f"Found game (alt method): {teams_found[0]} @ {teams_found[1]} (ID: {game_id})")
                            
                except Exception as e:
                    logger.warning(f"Error in alternative parsing: {e}")
                    continue
        
        return games
    
    def _extract_team_from_link(self, team_link) -> Optional[str]:
        """Extract team abbreviation from ESPN team link"""
        try:
            href = team_link.get('href', '')
            
            # Extract team from URL path: /nfl/team/_/name/teamname
            team_match = re.search(r'/nfl/team/_/name/([^/]+)', href)
            if team_match:
                team_name = team_match.group(1)
                return get_team_abbr(team_name)
            
            # Try extracting from link text
            link_text = team_link.get_text(strip=True)
            if link_text:
                return get_team_abbr(link_text)
            
            # Try extracting from team ID in URL
            team_id_match = re.search(r'/nfl/team/_/id/(\d+)', href)
            if team_id_match:
                # Map ESPN team IDs to abbreviations (partial mapping)
                espn_id_map = {
                    '1': 'ATL', '2': 'BUF', '3': 'CHI', '4': 'CIN', '5': 'CLE',
                    '6': 'DAL', '7': 'DEN', '8': 'DET', '9': 'GB', '10': 'HOU',
                    '11': 'IND', '12': 'KC', '13': 'LV', '14': 'LAC', '15': 'LAR',
                    '16': 'MIA', '17': 'MIN', '18': 'NE', '19': 'NO', '20': 'NYG',
                    '21': 'NYJ', '22': 'PHI', '23': 'PIT', '24': 'ARI', '25': 'SF',
                    '26': 'SEA', '27': 'TB', '28': 'TEN', '29': 'WAS', '30': 'CAR',
                    '33': 'BAL', '34': 'JAX'
                }
                team_id = team_id_match.group(1)
                if team_id in espn_id_map:
                    return espn_id_map[team_id]
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting team from link: {e}")
            return None
    
    def _extract_game_info(self, container, season: int, season_type: int, week: int) -> Optional[Dict]:
        """Extract game information from a container element"""
        try:
            # Look for game link
            game_link = None
            if container.name == 'a' and 'href' in container.attrs:
                game_link = container['href']
            else:
                link_elem = container.find('a', href=re.compile(r'/nfl/game/'))
                if link_elem:
                    game_link = link_elem['href']
            
            if not game_link:
                return None
            
            # Ensure absolute URL
            if game_link.startswith('/'):
                game_link = urljoin(ESPN_NFL_BASE, game_link)
            
            # Extract game ID from URL
            game_id_match = re.search(r'/game/[^/]*?(\d+)', game_link)
            game_id = game_id_match.group(1) if game_id_match else None
            
            if not game_id:
                return None
            
            # Extract team information
            teams = self._extract_teams_from_container(container)
            if len(teams) < 2:
                return None
            
            # Extract game status and time
            status_info = self._extract_game_status(container)
            
            game_info = {
                'game_id': game_id,
                'game_url': game_link,
                'away_team': teams[0],
                'home_team': teams[1], 
                'season': season,
                'season_type': season_type,
                'week': week,
                'status': status_info.get('status', 'scheduled'),
                'game_date': status_info.get('date'),
                'game_time': status_info.get('time')
            }
            
            return game_info
            
        except Exception as e:
            logger.warning(f"Error extracting game info: {e}")
            return None
    
    def _extract_teams_from_container(self, container) -> List[str]:
        """Extract team names from game container"""
        teams = []
        
        # Look for team name elements
        team_elements = container.find_all(['span', 'div', 'abbr'], 
                                         class_=re.compile(r'(team|abbreviation)', re.I))
        
        for elem in team_elements:
            team_text = elem.get_text(strip=True)
            if team_text and len(team_text) <= 4:  # Team abbreviations
                teams.append(get_team_abbr(team_text))
        
        # Alternative: look for team logos with alt text
        if len(teams) < 2:
            logo_elements = container.find_all('img', alt=True)
            for img in logo_elements:
                alt_text = img.get('alt', '')
                if any(keyword in alt_text.lower() for keyword in ['logo', 'team']):
                    team_name = re.sub(r'\s+(logo|team).*', '', alt_text, flags=re.I).strip()
                    if team_name:
                        teams.append(get_team_abbr(team_name))
        
        return teams[:2]  # Return max 2 teams
    
    def _extract_game_status(self, container) -> Dict[str, str]:
        """Extract game status, date, and time information"""
        status_info = {'status': 'scheduled'}
        
        # Look for status indicators
        status_elements = container.find_all(['span', 'div'], 
                                           class_=re.compile(r'(status|time|date)', re.I))
        
        for elem in status_elements:
            text = elem.get_text(strip=True).lower()
            if any(keyword in text for keyword in ['final', 'completed']):
                status_info['status'] = 'final'
            elif any(keyword in text for keyword in ['live', 'progress', 'quarter', 'qtr']):
                status_info['status'] = 'in_progress' 
            elif any(keyword in text for keyword in ['postponed', 'delayed', 'cancelled']):
                status_info['status'] = 'postponed'
                
            # Extract time if present
            time_match = re.search(r'(\d{1,2}:\d{2})', text)
            if time_match:
                status_info['time'] = time_match.group(1)
        
        return status_info
    
    def scrape_game_boxscore(self, game_info: Dict) -> Optional[Dict]:
        """
        Scrape individual game boxscore for detailed statistics
        
        Args:
            game_info: Game information dictionary
            
        Returns:
            Dictionary with game statistics or None if failed
        """
        # Construct boxscore URL from game ID
        game_id = game_info['game_id']
        boxscore_url = f"https://www.espn.com/nfl/boxscore/_/gameId/{game_id}"
        
        logger.info(f"Scraping game: {game_info['away_team']} @ {game_info['home_team']} ({game_id})")
        logger.info(f"Using boxscore URL: {boxscore_url}")
        
        try:
            # Add delay to respect ESPN servers
            time.sleep(random.uniform(2, 5))
            
            response = self.session.get(boxscore_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if game is postponed/cancelled
            postponed_info = self._detect_postponed_game(soup, boxscore_url)
            if postponed_info['is_postponed']:
                logger.warning(f"Game {game_info['game_id']} is postponed/cancelled: {postponed_info['reason']}")
                self.postponed_games.append({
                    'game_info': game_info,
                    'postponed_info': postponed_info
                })
                return None
            
            # Extract player statistics
            game_stats = self._extract_game_statistics(soup, game_info)
            
            if game_stats:
                self.processed_games.append(game_info)
                return game_stats
            else:
                logger.warning(f"No statistics found for game {game_info['game_id']}")
                self.failed_games.append(game_info)
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch game {game_info['game_id']}: {e}")
            self.failed_games.append(game_info)
            return None
        except Exception as e:
            logger.error(f"Error processing game {game_info['game_id']}: {e}")
            self.failed_games.append(game_info)
            return None
    
    def _detect_postponed_game(self, soup: BeautifulSoup, game_url: str) -> Dict[str, any]:
        """Detect if game is postponed or cancelled"""
        postponed_indicators = [
            'postponed', 'cancelled', 'canceled', 'delayed',
            'weather delay', 'rain delay', 'suspended'
        ]
        
        # Check page text for postponement keywords
        page_text = soup.get_text().lower()
        
        for indicator in postponed_indicators:
            if indicator in page_text:
                return {
                    'is_postponed': True,
                    'reason': indicator,
                    'status': 'postponed',
                    'detected_text': indicator
                }
        
        # Check for empty boxscore (another postponement indicator)
        stats_tables = soup.find_all('table')
        boxscore_sections = soup.find_all(['section', 'div'], class_=re.compile(r'boxscore', re.I))
        
        # NFL boxscore pages should have multiple tables and boxscore sections
        if len(stats_tables) < 5 or len(boxscore_sections) < 10:
            return {
                'is_postponed': True,
                'reason': 'insufficient_stats_data',
                'status': 'postponed'
            }
        
        return {'is_postponed': False}
    
    def _extract_game_statistics(self, soup: BeautifulSoup, game_info: Dict) -> Optional[Dict]:
        """Extract comprehensive NFL player statistics by team (similar to baseball structure)"""
        all_teams_data = {}
        
        # Find all TeamTitle elements (same as baseball structure)
        team_title_divs = soup.find_all('div', class_='TeamTitle', attrs={'data-testid': 'teamTitle'})
        
        if not team_title_divs:
            logger.error("Could not find any 'div.TeamTitle' elements on NFL boxscore page")
            return None
        
        logger.info(f"Found {len(team_title_divs)} TeamTitle divs for NFL processing")
        
        for title_div in team_title_divs:
            team_name_tag = title_div.find('div', class_='TeamTitle__Name')
            if not team_name_tag:
                logger.warning("Found TeamTitle div but no TeamTitle__Name inside")
                continue
            
            team_name_full = team_name_tag.get_text(strip=True)
            logger.debug(f"Processing section: {team_name_full}")
            
            # Parse team name and stat category (e.g., "New Orleans Passing", "Las Vegas Rushing")
            if ' ' not in team_name_full:
                logger.warning(f"Could not parse team and category from: '{team_name_full}'")
                continue
            
            # Handle compound team names properly
            stat_keywords = ['passing', 'rushing', 'receiving', 'kicking', 'punting', 'punt returns', 
                           'kick returns', 'fumbles', 'interceptions', 'defensive', 'defense']
            
            team_name = team_name_full
            stat_category = ""
            
            # Find the stat category by checking for known keywords from the end
            for keyword in stat_keywords:
                if team_name_full.lower().endswith(' ' + keyword):
                    # Remove the keyword from the end to get team name
                    team_name = team_name_full[:-len(' ' + keyword)].strip()
                    stat_category = keyword.lower()
                    break
            
            if not stat_category:
                # Fallback to original split method if no keyword found
                parts = team_name_full.rsplit(' ', 1)  # Split from right to get last word
                team_name = parts[0]
                stat_category = parts[1].lower()
            
            # Get team abbreviation using full team name
            team_abbr = get_team_abbr(team_name)
            
            logger.debug(f"Processing team: {team_name} ({team_abbr}), category: {stat_category}")
            
            # Find the stats table for this section
            responsive_table_div = title_div.find_next_sibling('div', class_='ResponsiveTable')
            if not responsive_table_div:
                logger.warning(f"Could not find ResponsiveTable for {team_name_full}")
                continue
            
            # Extract player statistics using the same method as baseball
            team_stats = self._extract_nfl_team_section_stats(responsive_table_div, team_name, team_abbr, stat_category, game_info)
            
            if team_stats:
                # Initialize team data structure if needed
                if team_abbr not in all_teams_data:
                    all_teams_data[team_abbr] = {}
                
                # Add stats to team's data
                all_teams_data[team_abbr][stat_category] = team_stats
                logger.debug(f"Added {len(team_stats)} {stat_category} records for {team_abbr}")
        
        logger.info(f"Successfully processed data for {len(all_teams_data)} teams")
        for team, data in all_teams_data.items():
            categories = list(data.keys())
            logger.info(f"  {team}: {categories}")
        
        return all_teams_data if all_teams_data else None
    
    def _extract_nfl_team_section_stats(self, responsive_table_div, team_name: str, team_abbr: str, stat_category: str, game_info: Dict) -> Optional[List[Dict]]:
        """Extract NFL team statistics from ResponsiveTable section (similar to baseball extraction)"""
        try:
            # Find player name table and stats table (same structure as baseball)
            player_name_table = responsive_table_div.find('table', class_='Table--fixed-left')
            stats_scroller_div = responsive_table_div.find('div', class_='Table__Scroller')
            stats_table = stats_scroller_div.find('table') if stats_scroller_div else None
            
            if not player_name_table or not stats_table:
                logger.warning(f"Could not find both player name table and stats table for {team_name} {stat_category}")
                return None
            
            # Extract stat headers
            stat_headers = []
            stats_head = stats_table.find('thead')
            if stats_head:
                header_tags = stats_head.find_all('th')
                stat_headers = [th.get_text(strip=True).lower().replace('.', '').replace('-', '_').replace('/', '_') for th in header_tags]
            
            if not stat_headers:
                logger.warning(f"Could not extract stat headers for {team_name} {stat_category}")
                return None
            
            logger.debug(f"Headers for {stat_category}: {stat_headers}")
            
            # Extract player names
            player_names_map = {}
            name_body = player_name_table.find('tbody')
            if name_body:
                name_rows = name_body.find_all('tr', attrs={'data-idx': True})
                for row in name_rows:
                    idx = row['data-idx']
                    name_cell = row.find('td')
                    if name_cell:
                        # Look for player name link or text
                        name_link = name_cell.find('a')
                        player_name = name_link.get_text(strip=True) if name_link else name_cell.get_text(strip=True)
                        
                        # Clean player name (remove jersey numbers, positions)
                        player_name = re.sub(r'^[a-z]-', '', player_name).strip()  # Remove position prefixes
                        player_name = re.sub(r'\s*\([^)]+\)$', '', player_name).strip()  # Remove parenthetical info
                        player_name = re.sub(r'\s*#\d+.*$', '', player_name).strip()  # Remove jersey numbers
                        
                        if player_name and player_name.lower() != 'team':
                            player_names_map[idx] = player_name
            
            # Extract player statistics
            player_stats_map = {}
            stats_body = stats_table.find('tbody')
            if stats_body:
                stats_rows = stats_body.find_all('tr', attrs={'data-idx': True})
                for row in stats_rows:
                    idx = row['data-idx']
                    stat_cells = row.find_all('td')
                    stats_list = [cell.get_text(strip=True) for cell in stat_cells]
                    if len(stats_list) == len(stat_headers):
                        player_stats_map[idx] = stats_list
            
            # Combine player names and statistics
            combined_player_data = []
            processed_indices = set(player_names_map.keys()) & set(player_stats_map.keys())
            sorted_indices = sorted(list(processed_indices), key=int)
            
            logger.debug(f"Combining data for {len(sorted_indices)} players for {team_abbr} {stat_category}")
            
            for idx in sorted_indices:
                player_data = {
                    'player': player_names_map[idx],
                    'team': team_abbr,
                    'stat_category': stat_category,
                    'game_id': game_info['game_id']
                }
                
                stats = player_stats_map[idx]
                for i, header in enumerate(stat_headers):
                    player_data[header] = stats[i] if i < len(stats) else ''
                
                combined_player_data.append(player_data)
                logger.debug(f"Added {stat_category} stats for {player_names_map[idx]}")
            
            return combined_player_data if combined_player_data else None
            
        except Exception as e:
            logger.error(f"Error extracting NFL team section stats for {team_name} {stat_category}: {e}")
            return None
    
    def _extract_nfl_category_stats(self, section, stats_data: Dict, game_info: Dict):
        """Extract NFL statistics from a Boxscore__Category section"""
        try:
            # Find all tables in this category
            tables = section.find_all('table')
            
            if len(tables) < 2:
                return
            
            # NFL structure: First table has player names, second table has stats
            player_table = None
            stats_table = None
            
            for table in tables:
                classes = table.get('class', [])
                if 'Table--fixed' in classes and 'Table--fixed-left' in classes:
                    player_table = table
                elif 'Table--align-right' in classes and 'Table--fixed' not in classes:
                    stats_table = table
                    break
            
            if not player_table or not stats_table:
                logger.warning("Could not find player and stats table pair in category section")
                return
            
            # Extract headers from stats table to determine stat type
            headers = stats_table.find_all(['th', 'td'])
            header_text = ' '.join(h.get_text(strip=True).lower() for h in headers)
            
            # Determine stat type from headers
            stat_type = self._determine_nfl_stat_type(header_text)
            if not stat_type:
                logger.warning(f"Could not determine stat type from headers: {header_text[:50]}...")
                return
            
            logger.debug(f"Processing {stat_type} statistics")
            
            # Extract player names
            player_names = {}
            player_rows = player_table.find_all('tr')
            for i, row in enumerate(player_rows):
                cells = row.find_all(['td', 'th'])
                for j, cell in enumerate(cells):
                    player_text = cell.get_text(strip=True)
                    if player_text and '#' in player_text and player_text.lower() != 'team':
                        # Clean player name (remove position, jersey number formatting)
                        clean_name = re.sub(r'#\d+.*$', '', player_text).strip()
                        player_names[f"{i}_{j}"] = clean_name
            
            # Extract statistics
            stats_rows = stats_table.find_all('tr')
            
            # Get actual headers
            if stats_rows:
                header_row = stats_rows[0]
                stat_headers = [th.get_text(strip=True).lower().replace('/', '_') for th in header_row.find_all(['th', 'td'])]
                
                # Process data rows
                for i, row in enumerate(stats_rows[1:], 1):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) != len(stat_headers):
                        continue
                    
                    # Find corresponding player name
                    player_name = None
                    for key, name in player_names.items():
                        row_idx, col_idx = key.split('_')
                        if int(row_idx) == i:  # Match row index
                            player_name = name
                            break
                    
                    if not player_name:
                        continue
                    
                    # Build stat record
                    player_stats = {
                        'player': player_name,
                        'stat_type': stat_type,
                        'game_id': game_info['game_id']
                    }
                    
                    for j, cell in enumerate(cells):
                        if j < len(stat_headers):
                            value = cell.get_text(strip=True)
                            player_stats[stat_headers[j]] = value
                    
                    # Add to appropriate stats list
                    if stat_type in stats_data:
                        stats_data[stat_type].append(player_stats)
                        logger.debug(f"Added {stat_type} stats for {player_name}")
                        
        except Exception as e:
            logger.error(f"Error extracting NFL category stats: {e}")
    
    def _determine_nfl_stat_type(self, header_text: str) -> Optional[str]:
        """Determine NFL stat type from table headers"""
        header_text = header_text.lower()
        
        if any(kw in header_text for kw in ['c_att', 'comp', 'pass', 'qb']):
            return 'passing'
        elif any(kw in header_text for kw in ['car', 'rush', 'att', 'yds']) and 'rec' not in header_text:
            return 'rushing'  
        elif any(kw in header_text for kw in ['rec', 'tgt', 'catch', 'receiving']):
            return 'receiving'
        elif any(kw in header_text for kw in ['tackle', 'sack', 'int', 'def', 'fumble']):
            return 'defensive'
        elif any(kw in header_text for kw in ['fg', 'xp', 'kick', 'punt']):
            return 'kicking'
        
        return None
    
    def _extract_passing_stats(self, section, stats_data: Dict, game_info: Dict):
        """Extract passing statistics"""
        passing_tables = section.find_all('table')
        
        for table in passing_tables:
            # Look for passing headers
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join(h.get_text(strip=True).lower() for h in headers)
            
            if any(keyword in header_text for keyword in ['pass', 'comp', 'yds', 'td', 'int']):
                self._parse_stats_table(table, stats_data['passing'], 'passing', game_info)
    
    def _extract_rushing_stats(self, section, stats_data: Dict, game_info: Dict):
        """Extract rushing statistics"""
        rushing_tables = section.find_all('table')
        
        for table in rushing_tables:
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join(h.get_text(strip=True).lower() for h in headers)
            
            if any(keyword in header_text for keyword in ['rush', 'car', 'att', 'yds']):
                self._parse_stats_table(table, stats_data['rushing'], 'rushing', game_info)
    
    def _extract_receiving_stats(self, section, stats_data: Dict, game_info: Dict):
        """Extract receiving statistics"""
        receiving_tables = section.find_all('table')
        
        for table in receiving_tables:
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join(h.get_text(strip=True).lower() for h in headers)
            
            if any(keyword in header_text for keyword in ['rec', 'tgt', 'catch']):
                self._parse_stats_table(table, stats_data['receiving'], 'receiving', game_info)
    
    def _extract_defensive_stats(self, section, stats_data: Dict, game_info: Dict):
        """Extract defensive statistics"""
        defensive_tables = section.find_all('table')
        
        for table in defensive_tables:
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join(h.get_text(strip=True).lower() for h in headers)
            
            if any(keyword in header_text for keyword in ['tackle', 'sack', 'int', 'def']):
                self._parse_stats_table(table, stats_data['defensive'], 'defensive', game_info)
    
    def _extract_kicking_stats(self, section, stats_data: Dict, game_info: Dict):
        """Extract kicking statistics"""
        kicking_tables = section.find_all('table')
        
        for table in kicking_tables:
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join(h.get_text(strip=True).lower() for h in headers)
            
            if any(keyword in header_text for keyword in ['kick', 'fg', 'xp', 'punt']):
                self._parse_stats_table(table, stats_data['kicking'], 'kicking', game_info)
    
    def _parse_stats_table(self, table, stats_list: List, stat_type: str, game_info: Dict):
        """Parse individual statistics table"""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return
        
        # Get headers
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < len(headers):
                continue
            
            player_data = {'stat_type': stat_type, 'game_id': game_info['game_id']}
            
            for i, cell in enumerate(cells):
                if i < len(headers):
                    value = cell.get_text(strip=True)
                    header = headers[i].lower().replace(' ', '_')
                    player_data[header] = value
            
            # Extract player name and team
            if 'name' in player_data or 'player' in player_data:
                stats_list.append(player_data)
    
    def save_statistics_to_csv(self, all_teams_data: Dict, game_info: Dict, output_dir: Path = None) -> List[str]:
        """
        Save NFL statistics to CSV files (one file per team per stat category)
        
        Args:
            all_teams_data: Dictionary containing team statistics data
            game_info: Game information dictionary
            output_dir: Output directory (defaults to CSV_BACKUPS)
            
        Returns:
            List of created file paths
        """
        if output_dir is None:
            output_dir = PATHS['csv_backups']
        
        if not all_teams_data:
            logger.warning(f"No team data to save for game {game_info['game_id']}")
            return []
        
        created_files = []
        
        # Use game date if available, otherwise fall back to current date
        if 'game_date' in game_info and game_info['game_date']:
            try:
                # Parse the game date and format it as YYYYMMDD
                game_date = game_info['game_date']
                if isinstance(game_date, str):
                    # Try parsing common date formats
                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y']:
                        try:
                            parsed_date = datetime.strptime(game_date, fmt)
                            date_str = parsed_date.strftime('%Y%m%d')
                            break
                        except ValueError:
                            continue
                    else:
                        # If no format worked, extract numbers from string
                        import re
                        date_nums = re.findall(r'\d+', game_date)
                        if len(date_nums) >= 3:
                            # Try to construct date from numbers
                            year = int(date_nums[0]) if len(date_nums[0]) == 4 else int(date_nums[2])
                            month = int(date_nums[1]) if len(date_nums[0]) == 4 else int(date_nums[0])
                            day = int(date_nums[2]) if len(date_nums[0]) == 4 else int(date_nums[1])
                            date_str = f"{year:04d}{month:02d}{day:02d}"
                        else:
                            date_str = datetime.now().strftime('%Y%m%d')
                elif hasattr(game_date, 'strftime'):
                    date_str = game_date.strftime('%Y%m%d')
                else:
                    date_str = datetime.now().strftime('%Y%m%d')
            except Exception as e:
                logger.warning(f"Error parsing game date '{game_info.get('game_date')}': {e}")
                date_str = datetime.now().strftime('%Y%m%d')
        else:
            date_str = datetime.now().strftime('%Y%m%d')
        
        # Save data for each team and stat category
        for team_abbr, team_data in all_teams_data.items():
            for stat_category, player_stats in team_data.items():
                if isinstance(player_stats, list) and player_stats:
                    # Create filename: nfl_TEAM_CATEGORY_week#_DATE_GAMEID.csv
                    # Use proper team abbreviation (no compound names in filename)
                    clean_team_abbr = team_abbr.upper().replace(' ', '_')
                    filename = f"nfl_{clean_team_abbr}_{stat_category}_week{game_info['week']}_{date_str}_{game_info['game_id']}.csv"
                    csv_path = output_dir / filename
                    
                    # Create DataFrame and save
                    df = pd.DataFrame(player_stats)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    created_files.append(str(csv_path))
                    
                    logger.info(f"Saved {len(player_stats)} {stat_category} records for {team_abbr} to {csv_path}")
        
        logger.info(f"Created {len(created_files)} CSV files for game {game_info['game_id']}")
        return created_files
    
    def process_week(self, season: int, season_type: int, week: int) -> Dict[str, any]:
        """
        Process all games for a specific week
        
        Args:
            season: Season year
            season_type: 1=Preseason, 2=Regular Season, 3=Playoffs  
            week: Week number
            
        Returns:
            Summary of processing results
        """
        logger.info(f"Processing NFL Week {week}, Season {season}, Type {season_type}")
        
        # Reset tracking lists
        self.processed_games = []
        self.failed_games = []
        self.postponed_games = []
        
        # Fetch schedule
        games = self.fetch_nfl_schedule(season, season_type, week)
        
        if not games:
            logger.warning(f"No games found for Week {week}, Season {season}")
            return {
                'success': False,
                'message': 'No games found',
                'processed': 0,
                'failed': 0,
                'postponed': 0
            }
        
        # Process each game
        all_created_files = []
        
        for game_info in games:
            all_teams_data = self.scrape_game_boxscore(game_info)
            if all_teams_data:
                created_files = self.save_statistics_to_csv(all_teams_data, game_info)
                all_created_files.extend(created_files)
        
        # Generate summary
        summary = {
            'success': True,
            'week': week,
            'season': season,
            'season_type': season_type,
            'total_games': len(games),
            'processed': len(self.processed_games),
            'failed': len(self.failed_games),
            'postponed': len(self.postponed_games),
            'created_files': all_created_files,
            'processed_games': self.processed_games,
            'failed_games': self.failed_games,
            'postponed_games': self.postponed_games
        }
        
        logger.info(f"Week {week} processing complete: {summary['processed']}/{summary['total_games']} games processed")
        
        # Save processing summary
        self._save_processing_summary(summary)
        
        return summary
    
    def _save_processing_summary(self, summary: Dict):
        """Save processing summary to JSON file"""
        summary_file = PATHS['scanned'] / f"nfl_week_{summary['week']}_s{summary['season']}_summary.json"
        
        try:
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            logger.info(f"Processing summary saved to {summary_file}")
        except Exception as e:
            logger.error(f"Failed to save processing summary: {e}")


def main():
    """Main entry point for command line usage"""
    parser = argparse.ArgumentParser(description='Enhanced NFL Scraper')
    parser.add_argument('--week', type=int, help='Week number to process')
    parser.add_argument('--season', type=int, help='Season year')
    parser.add_argument('--preseason', action='store_true', help='Process preseason games')
    parser.add_argument('--playoffs', action='store_true', help='Process playoff games')
    parser.add_argument('--current', action='store_true', help='Process current week (default)')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = EnhancedNFLScraper()
    
    # Determine parameters
    if args.current or (not args.week and not args.season):
        season, season_type, week = scraper.get_current_nfl_week()
        logger.info(f"Auto-detected current week: {week}, season: {season}, type: {season_type}")
    else:
        season = args.season or datetime.now().year
        week = args.week or 1
        
        if args.preseason:
            season_type = 1
        elif args.playoffs:
            season_type = 3
        else:
            season_type = 2  # Regular season
    
    # Process the week
    try:
        summary = scraper.process_week(season, season_type, week)
        
        if summary['success']:
            print(f"\n✅ Successfully processed NFL Week {week}")
            print(f"   Games processed: {summary['processed']}/{summary['total_games']}")
            print(f"   Files created: {len(summary['created_files'])}")
            if summary['postponed'] > 0:
                print(f"   ⚠️  Postponed games: {summary['postponed']}")
            if summary['failed'] > 0:
                print(f"   ❌ Failed games: {summary['failed']}")
        else:
            print(f"\n❌ Failed to process NFL Week {week}: {summary.get('message', 'Unknown error')}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error processing week: {e}")
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()