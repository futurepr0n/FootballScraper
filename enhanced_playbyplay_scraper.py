#!/usr/bin/env python3
"""
Enhanced Play-by-Play Scraper for ESPN NFL Games
Handles JavaScript-heavy pages and extracts complete play-by-play data
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random
import os
import re
from datetime import datetime
import logging
from pathlib import Path
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedPlayByPlayScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
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
    
    def try_espn_api_approach(self, game_id):
        """Try to access ESPN's internal API for play-by-play data"""
        try:
            # ESPN sometimes has API endpoints for play-by-play data
            api_urls = [
                f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={game_id}",
                f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{game_id}/competitions/{game_id}/plays",
                f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/playbyplay?event={game_id}"
            ]
            
            for api_url in api_urls:
                logger.info(f"Trying API: {api_url}")
                
                try:
                    response = self.session.get(api_url, timeout=15)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    # Check if this contains play-by-play data
                    if self.extract_plays_from_api_response(data, game_id):
                        logger.info(f"✅ Successfully extracted plays from API")
                        return True
                        
                except requests.exceptions.RequestException as e:
                    logger.debug(f"API request failed: {e}")
                    continue
                except json.JSONDecodeError as e:
                    logger.debug(f"JSON decode failed: {e}")
                    continue
            
            logger.warning("All API approaches failed")
            return False
            
        except Exception as e:
            logger.error(f"API approach failed: {e}")
            return False
    
    def extract_plays_from_api_response(self, data, game_id):
        """Extract play-by-play data from ESPN API JSON response"""
        try:
            plays = []
            
            # Try different JSON structures ESPN might use
            play_sources = [
                data.get('drives', []),
                data.get('playByPlay', {}).get('drives', []),
                data.get('gamepackageJSON', {}).get('drives', []),
                data.get('content', {}).get('plays', [])
            ]
            
            for drives in play_sources:
                if not drives:
                    continue
                    
                logger.info(f"Found {len(drives)} drives/plays in API response")
                
                for drive in drives:
                    if isinstance(drive, dict) and 'plays' in drive:
                        # Drive-based structure
                        drive_plays = drive.get('plays', [])
                        for play in drive_plays:
                            play_data = self.parse_api_play(play, drive)
                            if play_data:
                                plays.append(play_data)
                    elif isinstance(drive, dict):
                        # Direct play structure
                        play_data = self.parse_api_play(drive, {})
                        if play_data:
                            plays.append(play_data)
            
            if plays:
                logger.info(f"✅ Extracted {len(plays)} plays from API")
                self.save_playbyplay_csv(plays, game_id)
                return True
            
            logger.warning("No plays found in API response")
            return False
            
        except Exception as e:
            logger.error(f"Failed to extract plays from API response: {e}")
            return False
    
    def parse_api_play(self, play_data, drive_data):
        """Parse individual play from API JSON"""
        try:
            return {
                'game_id': play_data.get('gameId', ''),
                'drive_number': drive_data.get('displayOrder', ''),
                'play_number': play_data.get('sequenceNumber', ''),
                'quarter': play_data.get('period', {}).get('number', ''),
                'time_remaining': play_data.get('clock', {}).get('displayValue', ''),
                'down': play_data.get('start', {}).get('down', ''),
                'distance': play_data.get('start', {}).get('distance', ''),
                'yard_line': play_data.get('start', {}).get('yardLine', ''),
                'possession_team': play_data.get('start', {}).get('team', {}).get('abbreviation', ''),
                'play_type': play_data.get('type', {}).get('text', ''),
                'description': play_data.get('text', ''),
                'yards_gained': play_data.get('statYardage', 0),
                'end_down': play_data.get('end', {}).get('down', ''),
                'end_distance': play_data.get('end', {}).get('distance', ''),
                'end_yard_line': play_data.get('end', {}).get('yardLine', ''),
                'scoring_play': play_data.get('scoringPlay', False),
                'touchdown': 'touchdown' in play_data.get('text', '').lower(),
                'field_goal': 'field goal' in play_data.get('text', '').lower(),
                'interception': 'interception' in play_data.get('text', '').lower(),
                'fumble': 'fumble' in play_data.get('text', '').lower(),
                'sack': 'sack' in play_data.get('text', '').lower(),
                'penalty': 'penalty' in play_data.get('text', '').lower()
            }
        except Exception as e:
            logger.debug(f"Failed to parse play: {e}")
            return None
    
    def try_curl_with_javascript(self, game_url):
        """Try using curl with additional JavaScript simulation"""
        try:
            pbp_url = game_url.replace('/game/_/', '/playbyplay/_/')
            
            # Enhanced headers to better simulate a browser
            headers = [
                '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                '-H', 'Accept-Language: en-US,en;q=0.5',
                '-H', 'Accept-Encoding: gzip, deflate, br',
                '-H', 'Connection: keep-alive',
                '-H', 'Upgrade-Insecure-Requests: 1',
                '-H', 'Sec-Fetch-Dest: document',
                '-H', 'Sec-Fetch-Mode: navigate',
                '-H', 'Sec-Fetch-Site: none'
            ]
            
            cmd = ['curl', '-s', '-L', '--compressed'] + headers + [pbp_url]
            
            logger.info(f"Fetching play-by-play with enhanced curl: {pbp_url}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                html_content = result.stdout
                
                # Check if we got meaningful content
                if len(html_content) > 50000:  # ESPN pages are typically large
                    logger.info(f"✅ Got {len(html_content):,} characters from ESPN")
                    return self.parse_playbyplay_html(html_content, game_url)
                else:
                    logger.warning(f"Got small response ({len(html_content)} chars), likely not full page")
            else:
                logger.error(f"Curl failed with code {result.returncode}")
                
            return False
            
        except Exception as e:
            logger.error(f"Enhanced curl approach failed: {e}")
            return False
    
    def parse_playbyplay_html(self, html_content, game_url):
        """Parse play-by-play data from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            game_id = game_url.split('gameId/')[-1].split('/')[0]
            plays = []
            
            # Look for various ESPN play-by-play structures
            play_selectors = [
                '.Playbyplay__Drive',
                '.PlayByPlay__Drive', 
                '.drive-list .drive',
                '.play-by-play-drive',
                '[data-testid="drive"]',
                '.accordion-item'
            ]
            
            for selector in play_selectors:
                drives = soup.select(selector)
                if drives:
                    logger.info(f"Found {len(drives)} drives using selector: {selector}")
                    
                    for drive in drives:
                        drive_plays = self.extract_drive_plays(drive)
                        plays.extend(drive_plays)
                    
                    if plays:
                        break
            
            # Also look for embedded JSON data
            if not plays:
                plays = self.extract_plays_from_embedded_json(html_content)
            
            if plays:
                logger.info(f"✅ Extracted {len(plays)} plays from HTML")
                self.save_playbyplay_csv(plays, game_id)
                return True
            else:
                logger.warning("No plays found in HTML content")
                
                # Debug: save HTML to file for manual inspection
                debug_file = self.csv_dir / f"debug_pbp_{game_id}.html"
                with open(debug_file, 'w') as f:
                    f.write(html_content[:100000])  # Save first 100k chars
                logger.info(f"Saved HTML debug to: {debug_file}")
                
                return False
                
        except Exception as e:
            logger.error(f"Failed to parse HTML: {e}")
            return False
    
    def extract_drive_plays(self, drive_element):
        """Extract plays from a drive element"""
        plays = []
        
        try:
            # Look for individual plays within the drive
            play_selectors = [
                '.play-item',
                '.play',
                '.Playbyplay__Play',
                '[data-testid="play"]',
                '.pbp-play'
            ]
            
            for selector in play_selectors:
                play_elements = drive_element.select(selector)
                
                for play_elem in play_elements:
                    play_data = self.parse_html_play(play_elem)
                    if play_data:
                        plays.append(play_data)
                
                if play_elements:
                    break  # Use first selector that finds elements
        
        except Exception as e:
            logger.debug(f"Error extracting drive plays: {e}")
        
        return plays
    
    def parse_html_play(self, play_element):
        """Parse individual play from HTML element"""
        try:
            # Extract text content
            play_text = play_element.get_text().strip()
            
            if not play_text or len(play_text) < 10:
                return None
            
            return {
                'quarter': self.extract_quarter(play_text),
                'time_remaining': self.extract_time(play_text),
                'down': self.extract_down(play_text),
                'distance': self.extract_distance(play_text),
                'yard_line': self.extract_yard_line(play_text),
                'play_type': self.classify_play_type(play_text),
                'description': play_text[:500],  # Limit description length
                'yards_gained': self.extract_yards_gained(play_text),
                'scoring_play': self.is_scoring_play(play_text),
                'touchdown': 'touchdown' in play_text.lower(),
                'field_goal': 'field goal' in play_text.lower(),
                'interception': 'interception' in play_text.lower(),
                'fumble': 'fumble' in play_text.lower(),
                'sack': 'sack' in play_text.lower(),
                'penalty': 'penalty' in play_text.lower()
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse HTML play: {e}")
            return None
    
    def extract_plays_from_embedded_json(self, html_content):
        """Extract plays from embedded JSON in the HTML"""
        plays = []
        
        try:
            # Look for JSON data embedded in script tags
            json_patterns = [
                r'window\.espn\.gamepackage\s*=\s*({.+?});',
                r'window\.__espnfitt__\s*=\s*({.+?});',
                r'__INITIAL_STATE__\s*=\s*({.+?});'
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                
                for match in matches:
                    try:
                        data = json.loads(match)
                        extracted_plays = self.extract_plays_from_json_data(data)
                        plays.extend(extracted_plays)
                    except json.JSONDecodeError:
                        continue
                
                if plays:
                    break
        
        except Exception as e:
            logger.debug(f"Failed to extract embedded JSON: {e}")
        
        return plays
    
    def extract_plays_from_json_data(self, json_data):
        """Extract plays from parsed JSON data"""
        plays = []
        
        # Recursive search for play-by-play data in JSON
        def find_plays(obj, path=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in ['drives', 'plays', 'playByPlay']:
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict):
                                    play_data = self.parse_json_play(item)
                                    if play_data:
                                        plays.append(play_data)
                    else:
                        find_plays(value, f"{path}.{key}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    find_plays(item, f"{path}[{i}]")
        
        find_plays(json_data)
        return plays
    
    def parse_json_play(self, play_obj):
        """Parse play from JSON object"""
        try:
            # Flexible JSON parsing
            text = play_obj.get('text', play_obj.get('description', ''))
            
            if not text:
                return None
            
            return {
                'description': text,
                'quarter': play_obj.get('period', play_obj.get('quarter', '')),
                'time_remaining': play_obj.get('clock', play_obj.get('time', '')),
                'play_type': play_obj.get('type', ''),
                'yards_gained': play_obj.get('yards', play_obj.get('yardage', 0)),
                'scoring_play': play_obj.get('scoring', False),
                'touchdown': 'touchdown' in text.lower(),
                'field_goal': 'field goal' in text.lower()
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse JSON play: {e}")
            return None
    
    # Utility methods for parsing play text
    def extract_quarter(self, text):
        match = re.search(r'(\d)(?:st|nd|rd|th)\s+quarter', text.lower())
        return match.group(1) if match else ''
    
    def extract_time(self, text):
        match = re.search(r'(\d{1,2}:\d{2})', text)
        return match.group(1) if match else ''
    
    def extract_down(self, text):
        match = re.search(r'(\d)(?:st|nd|rd|th)\s+down', text.lower())
        return match.group(1) if match else ''
    
    def extract_distance(self, text):
        match = re.search(r'(\d+)\s+yard', text.lower())
        return match.group(1) if match else ''
    
    def extract_yard_line(self, text):
        match = re.search(r'([A-Z]{2,3})\s+(\d+)', text)
        return f"{match.group(1)} {match.group(2)}" if match else ''
    
    def classify_play_type(self, text):
        text_lower = text.lower()
        if 'pass' in text_lower:
            return 'pass'
        elif 'rush' in text_lower or 'run' in text_lower:
            return 'run'
        elif 'punt' in text_lower:
            return 'punt'
        elif 'field goal' in text_lower:
            return 'field_goal'
        elif 'kickoff' in text_lower:
            return 'kickoff'
        else:
            return 'other'
    
    def extract_yards_gained(self, text):
        # Look for yardage patterns
        patterns = [
            r'for\s+(\d+)\s+yard',
            r'(\d+)\s+yard\s+gain',
            r'gain\s+of\s+(\d+)',
            r'loss\s+of\s+(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                yards = int(match.group(1))
                return -yards if 'loss' in text.lower() else yards
        
        return 0
    
    def is_scoring_play(self, text):
        scoring_terms = ['touchdown', 'field goal', 'safety']
        return any(term in text.lower() for term in scoring_terms)
    
    def save_playbyplay_csv(self, plays, game_id):
        """Save play-by-play data to CSV file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d')
            filename = f"nfl_playbyplay_{game_id}_{timestamp}.csv"
            filepath = self.csv_dir / filename
            
            if not plays:
                logger.warning(f"No plays to save for game {game_id}")
                return False
            
            # Get all unique fieldnames from all plays
            fieldnames = set()
            for play in plays:
                fieldnames.update(play.keys())
            
            fieldnames = sorted(list(fieldnames))
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(plays)
            
            logger.info(f"✅ Saved {len(plays)} plays to: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save play-by-play CSV: {e}")
            return False
    
    def scrape_game_playbyplay(self, game_url):
        """Main method to scrape play-by-play for a single game"""
        try:
            game_id = game_url.split('gameId/')[-1].split('/')[0]
            logger.info(f"🏈 Scraping play-by-play for game {game_id}")
            
            # Check if already exists
            if self.check_for_existing_pbp(game_id):
                return True
            
            # Try multiple approaches
            approaches = [
                ("ESPN API", lambda: self.try_espn_api_approach(game_id)),
                ("Enhanced curl", lambda: self.try_curl_with_javascript(game_url))
            ]
            
            for approach_name, approach_func in approaches:
                logger.info(f"Trying approach: {approach_name}")
                
                try:
                    if approach_func():
                        logger.info(f"✅ Success with {approach_name}")
                        return True
                except Exception as e:
                    logger.error(f"❌ {approach_name} failed: {e}")
                
                # Brief delay between approaches
                time.sleep(1)
            
            logger.error(f"❌ All approaches failed for game {game_id}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to scrape play-by-play: {e}")
            return False

def main():
    """Test the enhanced play-by-play scraper"""
    scraper = EnhancedPlayByPlayScraper()
    
    # Test with the WAS vs ARI game that was missing play-by-play
    test_url = "https://www.espn.com/nfl/game/_/gameId/401547406"
    
    logger.info("🏈 ENHANCED PLAY-BY-PLAY SCRAPER TEST")
    logger.info("=" * 60)
    
    success = scraper.scrape_game_playbyplay(test_url)
    
    if success:
        logger.info("✅ Play-by-play scraping completed successfully")
    else:
        logger.error("❌ Play-by-play scraping failed")
        logger.info("This may be due to ESPN's JavaScript requirements")
        logger.info("Consider using a browser automation tool like Selenium")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())