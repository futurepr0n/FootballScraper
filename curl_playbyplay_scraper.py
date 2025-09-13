#!/usr/bin/env python3
"""
Curl-based Play-by-Play Scraper for ESPN NFL Games
Uses curl and BeautifulSoup to extract play-by-play data without requests dependency
"""

from bs4 import BeautifulSoup
import json
import csv
import subprocess
import time
import os
import re
from datetime import datetime
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CurlPlayByPlayScraper:
    def __init__(self):
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.csv_dir.mkdir(exist_ok=True)
        
    def check_for_existing_pbp(self, game_id):
        """Check if play-by-play data already exists for this game"""
        pattern = f"*playbyplay*{game_id}*.csv"
        existing_files = list(self.csv_dir.glob(pattern))
        
        if existing_files:
            logger.info(f"✅ Play-by-play already exists: {existing_files[0].name}")
            return existing_files[0]
        
        logger.info(f"No existing play-by-play found for game {game_id}")
        return None
    
    def fetch_playbyplay_html(self, game_url):
        """Fetch play-by-play HTML using curl with browser simulation"""
        try:
            pbp_url = game_url.replace('/game/_/', '/playbyplay/_/')
            
            # Enhanced headers to simulate real browser
            headers = [
                '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                '-H', 'Accept-Language: en-US,en;q=0.9',
                '-H', 'Accept-Encoding: gzip, deflate, br',
                '-H', 'Cache-Control: no-cache',
                '-H', 'Connection: keep-alive',
                '-H', 'Upgrade-Insecure-Requests: 1',
                '-H', 'Sec-Fetch-Dest: document',
                '-H', 'Sec-Fetch-Mode: navigate',
                '-H', 'Sec-Fetch-Site: none',
                '-H', 'DNT: 1'
            ]
            
            cmd = ['curl', '-s', '-L', '--compressed', '--max-time', '30'] + headers + [pbp_url]
            
            logger.info(f"Fetching: {pbp_url}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
            
            if result.returncode == 0:
                html_content = result.stdout
                logger.info(f"✅ Downloaded {len(html_content):,} characters")
                return html_content
            else:
                logger.error(f"Curl failed with return code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Curl error: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error("Curl request timed out")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch HTML: {e}")
            return None
    
    def try_api_endpoints(self, game_id):
        """Try ESPN API endpoints for play-by-play data"""
        api_endpoints = [
            f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}",
            f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{game_id}/competitions/{game_id}",
            f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/playbyplay?event={game_id}"
        ]
        
        for api_url in api_endpoints:
            try:
                logger.info(f"Trying API: {api_url}")
                
                cmd = ['curl', '-s', '--max-time', '15', '-H', 'Accept: application/json', api_url]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
                
                if result.returncode == 0 and result.stdout:
                    try:
                        data = json.loads(result.stdout)
                        if self.extract_plays_from_api(data, game_id):
                            logger.info(f"✅ Success with API: {api_url}")
                            return True
                    except json.JSONDecodeError:
                        logger.debug(f"Invalid JSON from API: {api_url}")
                        continue
                        
            except Exception as e:
                logger.debug(f"API {api_url} failed: {e}")
                continue
        
        return False
    
    def extract_plays_from_api(self, data, game_id):
        """Extract plays from ESPN API JSON response"""
        try:
            plays = []
            
            # Look for plays in various JSON structures
            if 'drives' in data:
                drives = data['drives']
                logger.info(f"Found {len(drives)} drives in API response")
                
                for drive in drives:
                    if isinstance(drive, dict) and 'plays' in drive:
                        for play in drive['plays']:
                            play_data = self.parse_api_play(play, drive)
                            if play_data:
                                plays.append(play_data)
            
            # Try alternative structures
            elif 'playByPlay' in data and 'drives' in data['playByPlay']:
                drives = data['playByPlay']['drives']
                for drive in drives:
                    if 'plays' in drive:
                        for play in drive['plays']:
                            play_data = self.parse_api_play(play, drive)
                            if play_data:
                                plays.append(play_data)
            
            # Look for direct plays array
            elif 'plays' in data:
                for play in data['plays']:
                    play_data = self.parse_api_play(play, {})
                    if play_data:
                        plays.append(play_data)
            
            if plays:
                logger.info(f"✅ Extracted {len(plays)} plays from API")
                self.save_playbyplay_csv(plays, game_id)
                return True
            
            logger.info("No plays found in API response")
            return False
            
        except Exception as e:
            logger.error(f"Failed to extract plays from API: {e}")
            return False
    
    def parse_api_play(self, play_data, drive_data):
        """Parse individual play from API JSON"""
        try:
            # Handle nested data structures
            if isinstance(play_data, dict):
                return {
                    'game_id': play_data.get('gameId', ''),
                    'drive_number': drive_data.get('displayOrder', drive_data.get('sequenceNumber', '')),
                    'play_number': play_data.get('sequenceNumber', ''),
                    'quarter': play_data.get('period', {}).get('number', '') if isinstance(play_data.get('period'), dict) else play_data.get('period', ''),
                    'time_remaining': play_data.get('clock', {}).get('displayValue', '') if isinstance(play_data.get('clock'), dict) else play_data.get('clock', ''),
                    'down': play_data.get('start', {}).get('down', '') if isinstance(play_data.get('start'), dict) else '',
                    'distance': play_data.get('start', {}).get('distance', '') if isinstance(play_data.get('start'), dict) else '',
                    'yard_line': play_data.get('start', {}).get('yardLine', '') if isinstance(play_data.get('start'), dict) else '',
                    'possession_team': play_data.get('start', {}).get('team', {}).get('abbreviation', '') if isinstance(play_data.get('start'), dict) else '',
                    'play_type': play_data.get('type', {}).get('text', '') if isinstance(play_data.get('type'), dict) else play_data.get('type', ''),
                    'description': play_data.get('text', play_data.get('description', '')),
                    'yards_gained': play_data.get('statYardage', play_data.get('yards', 0)),
                    'scoring_play': play_data.get('scoringPlay', False),
                    'touchdown': self.is_touchdown(play_data.get('text', '')),
                    'field_goal': self.is_field_goal(play_data.get('text', '')),
                    'interception': self.is_interception(play_data.get('text', '')),
                    'fumble': self.is_fumble(play_data.get('text', '')),
                    'sack': self.is_sack(play_data.get('text', '')),
                    'penalty': self.is_penalty(play_data.get('text', ''))
                }
            else:
                return None
                
        except Exception as e:
            logger.debug(f"Failed to parse API play: {e}")
            return None
    
    def parse_html_playbyplay(self, html_content, game_id):
        """Parse play-by-play data from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            plays = []
            
            # Look for embedded JSON first (most reliable)
            json_plays = self.extract_embedded_json_plays(html_content)
            if json_plays:
                plays.extend(json_plays)
                logger.info(f"Found {len(json_plays)} plays in embedded JSON")
            
            # If no JSON, try HTML parsing
            if not plays:
                html_plays = self.extract_html_plays(soup)
                plays.extend(html_plays)
                logger.info(f"Found {len(html_plays)} plays in HTML")
            
            if plays:
                logger.info(f"✅ Total plays extracted: {len(plays)}")
                self.save_playbyplay_csv(plays, game_id)
                return True
            else:
                logger.warning("No plays found in HTML content")
                self.save_debug_html(html_content, game_id)
                return False
                
        except Exception as e:
            logger.error(f"Failed to parse HTML: {e}")
            return False
    
    def extract_embedded_json_plays(self, html_content):
        """Extract plays from embedded JSON in HTML"""
        plays = []
        
        try:
            # Look for various ESPN JSON patterns
            json_patterns = [
                r'window\.espn\.gamepackage\s*=\s*({.+?});',
                r'window\.__espnfitt__\s*=\s*({.+?});',
                r'__INITIAL_STATE__\s*=\s*({.+?});',
                r'"drives":\s*(\[.+?\])',
                r'"playByPlay":\s*({.+?})'
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                
                for match in matches:
                    try:
                        # Clean up the JSON string
                        json_str = match.strip()
                        if not json_str.startswith('{') and not json_str.startswith('['):
                            continue
                            
                        data = json.loads(json_str)
                        extracted = self.extract_plays_from_json_data(data)
                        plays.extend(extracted)
                        
                        if plays:
                            break
                            
                    except json.JSONDecodeError as e:
                        logger.debug(f"JSON decode failed: {e}")
                        continue
                
                if plays:
                    break
                    
        except Exception as e:
            logger.debug(f"Failed to extract embedded JSON: {e}")
        
        return plays
    
    def extract_plays_from_json_data(self, data):
        """Recursively extract plays from JSON data"""
        plays = []
        
        def search_json(obj, path=""):
            if isinstance(obj, dict):
                # Look for drives
                if 'drives' in obj:
                    drives = obj['drives']
                    if isinstance(drives, list):
                        for drive in drives:
                            if isinstance(drive, dict) and 'plays' in drive:
                                for play in drive['plays']:
                                    play_data = self.parse_api_play(play, drive)
                                    if play_data:
                                        plays.append(play_data)
                
                # Look for plays directly
                elif 'plays' in obj:
                    plays_list = obj['plays']
                    if isinstance(plays_list, list):
                        for play in plays_list:
                            play_data = self.parse_api_play(play, {})
                            if play_data:
                                plays.append(play_data)
                
                # Continue searching
                for key, value in obj.items():
                    search_json(value, f"{path}.{key}")
                    
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    search_json(item, f"{path}[{i}]")
        
        search_json(data)
        return plays
    
    def extract_html_plays(self, soup):
        """Extract plays from HTML structure"""
        plays = []
        
        try:
            # Try various ESPN HTML selectors
            selectors = [
                '.PlayByPlay__Drive .PlayByPlay__Play',
                '.Playbyplay__Drive .Playbyplay__Play',
                '.play-by-play .drive .play',
                '.pbp-drive .pbp-play',
                '[data-testid="play"]',
                '.accordion-item .play-item'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                
                if elements:
                    logger.info(f"Found {len(elements)} play elements with selector: {selector}")
                    
                    for elem in elements:
                        play_text = elem.get_text().strip()
                        
                        if play_text and len(play_text) > 20:  # Filter out empty/short elements
                            play_data = self.parse_html_play_text(play_text)
                            if play_data:
                                plays.append(play_data)
                    
                    if plays:
                        break  # Use first successful selector
                        
        except Exception as e:
            logger.debug(f"HTML parsing failed: {e}")
        
        return plays
    
    def parse_html_play_text(self, play_text):
        """Parse play data from text content"""
        try:
            return {
                'description': play_text[:500],  # Limit length
                'quarter': self.extract_quarter_from_text(play_text),
                'time_remaining': self.extract_time_from_text(play_text),
                'down': self.extract_down_from_text(play_text),
                'distance': self.extract_distance_from_text(play_text),
                'yard_line': self.extract_yard_line_from_text(play_text),
                'play_type': self.classify_play_type_from_text(play_text),
                'yards_gained': self.extract_yards_from_text(play_text),
                'scoring_play': self.is_scoring_play(play_text),
                'touchdown': self.is_touchdown(play_text),
                'field_goal': self.is_field_goal(play_text),
                'interception': self.is_interception(play_text),
                'fumble': self.is_fumble(play_text),
                'sack': self.is_sack(play_text),
                'penalty': self.is_penalty(play_text)
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse play text: {e}")
            return None
    
    # Text parsing utility methods
    def extract_quarter_from_text(self, text):
        match = re.search(r'(\d)(?:st|nd|rd|th)?\s*(?:quarter|qtr)', text.lower())
        return match.group(1) if match else ''
    
    def extract_time_from_text(self, text):
        match = re.search(r'(\d{1,2}:\d{2})', text)
        return match.group(1) if match else ''
    
    def extract_down_from_text(self, text):
        match = re.search(r'(\d)(?:st|nd|rd|th)\s+(?:down|&)', text.lower())
        return match.group(1) if match else ''
    
    def extract_distance_from_text(self, text):
        match = re.search(r'&\s*(\d+)', text)
        return match.group(1) if match else ''
    
    def extract_yard_line_from_text(self, text):
        match = re.search(r'([A-Z]{2,3})\s*(\d+)', text.upper())
        return f"{match.group(1)} {match.group(2)}" if match else ''
    
    def classify_play_type_from_text(self, text):
        text_lower = text.lower()
        if 'pass' in text_lower:
            return 'pass'
        elif any(word in text_lower for word in ['rush', 'run', 'carry']):
            return 'run'
        elif 'punt' in text_lower:
            return 'punt'
        elif 'field goal' in text_lower:
            return 'field_goal'
        elif 'kickoff' in text_lower:
            return 'kickoff'
        else:
            return 'other'
    
    def extract_yards_from_text(self, text):
        # Look for yardage patterns
        patterns = [
            r'for\s+(\d+)\s+yard',
            r'(\d+)\s*yard\s*gain',
            r'gain\s+of\s+(\d+)',
            r'loss\s+of\s+(\d+)',
            r'(\d+)\s*yard\s*pass',
            r'(\d+)\s*yard\s*run'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                yards = int(match.group(1))
                return -yards if 'loss' in text.lower() else yards
        
        return 0
    
    # Classification methods
    def is_scoring_play(self, text):
        return any(term in text.lower() for term in ['touchdown', 'field goal', 'safety'])
    
    def is_touchdown(self, text):
        return 'touchdown' in text.lower()
    
    def is_field_goal(self, text):
        return 'field goal' in text.lower()
    
    def is_interception(self, text):
        return 'interception' in text.lower()
    
    def is_fumble(self, text):
        return 'fumble' in text.lower()
    
    def is_sack(self, text):
        return 'sack' in text.lower()
    
    def is_penalty(self, text):
        return 'penalty' in text.lower()
    
    def save_playbyplay_csv(self, plays, game_id):
        """Save play-by-play data to CSV file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d')
            filename = f"nfl_playbyplay_{game_id}_{timestamp}.csv"
            filepath = self.csv_dir / filename
            
            if not plays:
                logger.warning(f"No plays to save for game {game_id}")
                return False
            
            # Get all fieldnames
            fieldnames = set()
            for play in plays:
                if isinstance(play, dict):
                    fieldnames.update(play.keys())
            
            fieldnames = sorted(list(fieldnames))
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for play in plays:
                    if isinstance(play, dict):
                        writer.writerow(play)
            
            logger.info(f"✅ Saved {len(plays)} plays to: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")
            return False
    
    def save_debug_html(self, html_content, game_id):
        """Save HTML content for debugging"""
        try:
            debug_file = self.csv_dir / f"debug_pbp_{game_id}.html"
            with open(debug_file, 'w') as f:
                # Save first 200k characters to avoid huge files
                f.write(html_content[:200000])
            logger.info(f"💾 Saved debug HTML to: {debug_file}")
        except Exception as e:
            logger.debug(f"Failed to save debug HTML: {e}")
    
    def scrape_game_playbyplay(self, game_url):
        """Main method to scrape play-by-play for a single game"""
        try:
            game_id = game_url.split('gameId/')[-1].split('/')[0]
            logger.info(f"🏈 Scraping play-by-play for game {game_id}")
            
            # Check if already exists
            if self.check_for_existing_pbp(game_id):
                return True
            
            # Try API endpoints first (fastest)
            if self.try_api_endpoints(game_id):
                return True
            
            # Try HTML scraping as fallback
            html_content = self.fetch_playbyplay_html(game_url)
            if html_content:
                return self.parse_html_playbyplay(html_content, game_id)
            
            logger.error(f"❌ All approaches failed for game {game_id}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to scrape play-by-play: {e}")
            return False

def main():
    """Test the curl-based play-by-play scraper"""
    scraper = CurlPlayByPlayScraper()
    
    # Test with the WAS vs ARI game
    test_url = "https://www.espn.com/nfl/game/_/gameId/401547406"
    
    logger.info("🏈 CURL PLAY-BY-PLAY SCRAPER TEST")
    logger.info("=" * 60)
    
    success = scraper.scrape_game_playbyplay(test_url)
    
    if success:
        logger.info("✅ Play-by-play scraping completed successfully")
        logger.info("Check the CSV_BACKUPS directory for the play-by-play file")
    else:
        logger.error("❌ Play-by-play scraping failed")
        logger.info("ESPN's play-by-play pages may require JavaScript execution")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())