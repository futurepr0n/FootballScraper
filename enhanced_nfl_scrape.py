#!/usr/bin/env python3
"""
Enhanced NFL Scraper - Centralized ESPN NFL Data Collection
Scrapes NFL game data from ESPN boxscores and generates CSV files for the football analytics pipeline.
Based on BaseballScraper architecture but adapted for NFL weekly schedule structure.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
from datetime import datetime, timedelta
import logging
from pathlib import Path
import json
from config import PATHS, DATA_PATH, ESPN_NFL_BASE, ESPN_NFL_SCORES

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

class NFLScraper:
    """
    Enhanced NFL data scraper with ESPN integration and centralized output
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Ensure required directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        for path_name, path in PATHS.items():
            if isinstance(path, Path):
                path.mkdir(parents=True, exist_ok=True)
        
        # Ensure logs directory exists
        logs_dir = Path(__file__).parent / 'logs'
        logs_dir.mkdir(exist_ok=True)
    
    def _random_delay(self, min_seconds=5, max_seconds=15):
        """Add random delay between requests to be respectful to ESPN servers"""
        delay = random.uniform(min_seconds, max_seconds)
        logger.info(f"Waiting {delay:.1f} seconds...")
        time.sleep(delay)
    
    def get_nfl_scoreboard(self, date=None, week=None, season=None):
        """
        Get NFL scoreboard for specific date, week, or current week
        
        Args:
            date: Specific date (YYYY-MM-DD format)
            week: NFL week number (1-22)
            season: NFL season year
        
        Returns:
            dict: Scoreboard data with game URLs and basic info
        """
        try:
            # Build ESPN NFL scoreboard URL
            url_params = []
            if date:
                # ESPN uses date format for scoreboard
                url_params.append(f"date={date.replace('-', '')}")
            elif week and season:
                # ESPN NFL schedule by week
                url_params.append(f"week={week}")
                url_params.append(f"year={season}")
            
            url = ESPN_NFL_SCORES
            if url_params:
                url += "?" + "&".join(url_params)
            
            logger.info(f"Fetching NFL scoreboard: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract game information
            games = []
            game_containers = soup.find_all('section', class_='Card')
            
            for container in game_containers:
                try:
                    # Extract game link
                    game_link = container.find('a', href=True)
                    if not game_link:
                        continue
                    
                    game_url = game_link['href']
                    if not game_url.startswith('http'):
                        game_url = f"https://www.espn.com{game_url}"
                    
                    # Extract team information
                    teams = container.find_all('div', class_='team-name')
                    if len(teams) >= 2:
                        away_team = teams[0].get_text(strip=True)
                        home_team = teams[1].get_text(strip=True)
                    else:
                        # Fallback team extraction
                        team_elements = container.find_all('span', class_='abbrev')
                        if len(team_elements) >= 2:
                            away_team = team_elements[0].get_text(strip=True)
                            home_team = team_elements[1].get_text(strip=True)
                        else:
                            logger.warning(f"Could not extract teams from game container")
                            continue
                    
                    # Extract game ID from URL
                    game_id = self._extract_game_id(game_url)
                    
                    games.append({
                        'game_id': game_id,
                        'game_url': game_url,
                        'away_team': away_team,
                        'home_team': home_team,
                        'matchup': f"{away_team} @ {home_team}"
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing game container: {str(e)}")
                    continue
            
            logger.info(f"Found {len(games)} NFL games")
            return {
                'date': date or datetime.now().strftime('%Y-%m-%d'),
                'week': week,
                'season': season,
                'games': games,
                'total_games': len(games)
            }
            
        except Exception as e:
            logger.error(f"Error fetching NFL scoreboard: {str(e)}")
            return {'games': [], 'total_games': 0}
    
    def _extract_game_id(self, game_url):
        """Extract game ID from ESPN NFL game URL"""
        try:
            # ESPN NFL game URLs typically end with gameId parameter
            if 'gameId=' in game_url:
                return game_url.split('gameId=')[1].split('&')[0]
            elif '/game/' in game_url:
                # Alternative format: /nfl/game/_/gameId/401671716
                return game_url.split('/game/_/gameId/')[1].split('/')[0]
            else:
                # Extract from URL path
                url_parts = game_url.split('/')
                for part in reversed(url_parts):
                    if part.isdigit() and len(part) >= 9:
                        return part
                
                # Fallback: generate from URL hash
                return str(hash(game_url))[-9:]
        except Exception as e:
            logger.warning(f"Could not extract game ID from {game_url}: {str(e)}")
            return str(hash(game_url))[-9:]
    
    def scrape_nfl_game(self, game_url, game_id):
        """
        Scrape individual NFL game data from ESPN boxscore
        
        Args:
            game_url: ESPN game URL
            game_id: Unique game identifier
        
        Returns:
            dict: Game data including team stats and player stats
        """
        try:
            logger.info(f"Scraping NFL game: {game_url}")
            
            # Add random delay
            self._random_delay(3, 8)
            
            response = self.session.get(game_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract team stats and player stats
            game_data = {
                'game_id': game_id,
                'game_url': game_url,
                'scrape_time': datetime.now().isoformat(),
                'team_stats': self._extract_team_stats(soup),
                'passing_stats': self._extract_passing_stats(soup),
                'rushing_stats': self._extract_rushing_stats(soup),
                'receiving_stats': self._extract_receiving_stats(soup),
                'defensive_stats': self._extract_defensive_stats(soup),
                'kicking_stats': self._extract_kicking_stats(soup)
            }
            
            return game_data
            
        except Exception as e:
            logger.error(f"Error scraping NFL game {game_url}: {str(e)}")
            return None
    
    def _extract_team_stats(self, soup):
        """Extract team-level statistics"""
        team_stats = {}
        try:
            # ESPN team stats typically in table format
            stats_tables = soup.find_all('table', class_='Table')
            
            for table in stats_tables:
                # Look for team stats indicators
                table_caption = table.find('caption')
                if table_caption and 'team' in table_caption.get_text().lower():
                    # Process team stats table
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            stat_name = cells[0].get_text(strip=True)
                            stat_value = cells[1].get_text(strip=True)
                            team_stats[stat_name] = stat_value
            
        except Exception as e:
            logger.error(f"Error extracting team stats: {str(e)}")
        
        return team_stats
    
    def _extract_passing_stats(self, soup):
        """Extract quarterback passing statistics"""
        passing_stats = []
        try:
            # Look for passing stats tables
            tables = soup.find_all('table')
            
            for table in tables:
                header_row = table.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Check if this is a passing stats table
                    if any(header.lower() in ['c/att', 'yds', 'td', 'int', 'qbr'] for header in headers):
                        # Process passing stats
                        for row in table.find_all('tr')[1:]:  # Skip header
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= len(headers):
                                player_data = {}
                                for i, header in enumerate(headers):
                                    if i < len(cells):
                                        player_data[header] = cells[i].get_text(strip=True)
                                
                                if player_data.get('Player') or player_data.get('Name'):
                                    passing_stats.append(player_data)
            
        except Exception as e:
            logger.error(f"Error extracting passing stats: {str(e)}")
        
        return passing_stats
    
    def _extract_rushing_stats(self, soup):
        """Extract rushing statistics"""
        rushing_stats = []
        try:
            # Similar pattern for rushing stats
            tables = soup.find_all('table')
            
            for table in tables:
                header_row = table.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Check if this is a rushing stats table
                    if any(header.lower() in ['car', 'yds', 'avg', 'lng', 'td'] for header in headers):
                        # Process rushing stats
                        for row in table.find_all('tr')[1:]:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= len(headers):
                                player_data = {}
                                for i, header in enumerate(headers):
                                    if i < len(cells):
                                        player_data[header] = cells[i].get_text(strip=True)
                                
                                if player_data.get('Player') or player_data.get('Name'):
                                    rushing_stats.append(player_data)
            
        except Exception as e:
            logger.error(f"Error extracting rushing stats: {str(e)}")
        
        return rushing_stats
    
    def _extract_receiving_stats(self, soup):
        """Extract receiving statistics"""
        receiving_stats = []
        try:
            tables = soup.find_all('table')
            
            for table in tables:
                header_row = table.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Check if this is a receiving stats table
                    if any(header.lower() in ['rec', 'yds', 'avg', 'lng', 'td', 'tgts'] for header in headers):
                        # Process receiving stats
                        for row in table.find_all('tr')[1:]:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= len(headers):
                                player_data = {}
                                for i, header in enumerate(headers):
                                    if i < len(cells):
                                        player_data[header] = cells[i].get_text(strip=True)
                                
                                if player_data.get('Player') or player_data.get('Name'):
                                    receiving_stats.append(player_data)
            
        except Exception as e:
            logger.error(f"Error extracting receiving stats: {str(e)}")
        
        return receiving_stats
    
    def _extract_defensive_stats(self, soup):
        """Extract defensive statistics"""
        defensive_stats = []
        try:
            tables = soup.find_all('table')
            
            for table in tables:
                header_row = table.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Check if this is a defensive stats table
                    if any(header.lower() in ['tot', 'solo', 'sck', 'tfl', 'int', 'pd'] for header in headers):
                        # Process defensive stats
                        for row in table.find_all('tr')[1:]:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= len(headers):
                                player_data = {}
                                for i, header in enumerate(headers):
                                    if i < len(cells):
                                        player_data[header] = cells[i].get_text(strip=True)
                                
                                if player_data.get('Player') or player_data.get('Name'):
                                    defensive_stats.append(player_data)
            
        except Exception as e:
            logger.error(f"Error extracting defensive stats: {str(e)}")
        
        return defensive_stats
    
    def _extract_kicking_stats(self, soup):
        """Extract kicking and special teams statistics"""
        kicking_stats = []
        try:
            tables = soup.find_all('table')
            
            for table in tables:
                header_row = table.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Check if this is a kicking stats table
                    if any(header.lower() in ['fg', 'pct', 'lng', 'xp', 'pts'] for header in headers):
                        # Process kicking stats
                        for row in table.find_all('tr')[1:]:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= len(headers):
                                player_data = {}
                                for i, header in enumerate(headers):
                                    if i < len(cells):
                                        player_data[header] = cells[i].get_text(strip=True)
                                
                                if player_data.get('Player') or player_data.get('Name'):
                                    kicking_stats.append(player_data)
            
        except Exception as e:
            logger.error(f"Error extracting kicking stats: {str(e)}")
        
        return kicking_stats
    
    def save_game_data(self, game_data, output_format='both'):
        """
        Save game data to centralized location in CSV and/or JSON format
        
        Args:
            game_data: Game data dictionary
            output_format: 'csv', 'json', or 'both'
        """
        try:
            game_id = game_data['game_id']
            date_str = datetime.now().strftime('%Y-%m-%d')
            
            # Ensure output directory exists
            csv_backup_path = PATHS['csv_backups']
            csv_backup_path.mkdir(parents=True, exist_ok=True)
            
            if output_format in ['csv', 'both']:
                # Save each stat type as separate CSV
                stat_types = ['passing_stats', 'rushing_stats', 'receiving_stats', 'defensive_stats', 'kicking_stats']
                
                for stat_type in stat_types:
                    if stat_type in game_data and game_data[stat_type]:
                        df = pd.DataFrame(game_data[stat_type])
                        csv_filename = f"nfl_{stat_type}_{date_str}_{game_id}.csv"
                        csv_path = csv_backup_path / csv_filename
                        
                        df.to_csv(csv_path, index=False)
                        logger.info(f"Saved {stat_type} to {csv_path}")
            
            if output_format in ['json', 'both']:
                # Save complete game data as JSON
                json_filename = f"nfl_game_{date_str}_{game_id}.json"
                json_path = csv_backup_path / json_filename
                
                with open(json_path, 'w') as f:
                    json.dump(game_data, f, indent=2)
                logger.info(f"Saved complete game data to {json_path}")
            
        except Exception as e:
            logger.error(f"Error saving game data: {str(e)}")
    
    def scrape_weekly_games(self, week=None, season=None):
        """
        Scrape all games for a specific NFL week
        
        Args:
            week: NFL week number (defaults to current week)
            season: NFL season year (defaults to current season)
        
        Returns:
            dict: Summary of scraping results
        """
        try:
            # Default to current week/season if not specified
            if not season:
                season = datetime.now().year
            if not week:
                # Simple week calculation - would need more sophisticated logic for actual use
                week = 1
            
            logger.info(f"Scraping NFL Week {week}, {season} season")
            
            # Get scoreboard for the week
            scoreboard = self.get_nfl_scoreboard(week=week, season=season)
            
            if not scoreboard['games']:
                logger.warning(f"No games found for Week {week}, {season}")
                return {'success': False, 'message': 'No games found'}
            
            # Scrape each game
            results = {
                'week': week,
                'season': season,
                'total_games': len(scoreboard['games']),
                'successful_scrapes': 0,
                'failed_scrapes': 0,
                'games_processed': []
            }
            
            for game in scoreboard['games']:
                try:
                    game_data = self.scrape_nfl_game(game['game_url'], game['game_id'])
                    
                    if game_data:
                        self.save_game_data(game_data)
                        results['successful_scrapes'] += 1
                        results['games_processed'].append({
                            'game_id': game['game_id'],
                            'matchup': game['matchup'],
                            'status': 'success'
                        })
                    else:
                        results['failed_scrapes'] += 1
                        results['games_processed'].append({
                            'game_id': game['game_id'],
                            'matchup': game['matchup'],
                            'status': 'failed'
                        })
                
                except Exception as e:
                    logger.error(f"Error processing game {game['game_id']}: {str(e)}")
                    results['failed_scrapes'] += 1
                    results['games_processed'].append({
                        'game_id': game['game_id'],
                        'matchup': game.get('matchup', 'Unknown'),
                        'status': 'error',
                        'error': str(e)
                    })
            
            logger.info(f"Scraping complete: {results['successful_scrapes']}/{results['total_games']} games successful")
            return results
            
        except Exception as e:
            logger.error(f"Error in weekly scraping: {str(e)}")
            return {'success': False, 'error': str(e)}

def main():
    """Main execution function"""
    scraper = NFLScraper()
    
    # Example usage: scrape current week
    results = scraper.scrape_weekly_games()
    
    print(f"NFL Scraping Results:")
    print(f"  Total Games: {results.get('total_games', 0)}")
    print(f"  Successful: {results.get('successful_scrapes', 0)}")
    print(f"  Failed: {results.get('failed_scrapes', 0)}")

if __name__ == "__main__":
    main()