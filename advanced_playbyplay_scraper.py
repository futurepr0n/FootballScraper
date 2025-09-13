#!/usr/bin/env python3
"""
Advanced Play-by-Play Scraper with Dynamic Content Expansion
Handles ESPN's accordion-style play-by-play sections with smart expansion
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import json
import time
import random
import os
import re
from datetime import datetime
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import csv

class AdvancedPlayByPlayScraper:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.csv_dir.mkdir(exist_ok=True)
        
        # Setup Chrome options for headless browsing with stealth
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        self.chrome_options.add_argument('--disable-extensions')
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)
        self.chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        self.successful_games = 0
        self.failed_games = 0
        self.skipped_games = 0
        
        print("🏈 Advanced Play-by-Play Scraper initialized")
        print("🎯 Focus: Dynamic content expansion and intelligent parsing")
    
    def create_driver(self):
        """Create a new Chrome driver instance"""
        try:
            # Use webdriver-manager to automatically handle ChromeDriver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            driver.set_page_load_timeout(60)
            driver.implicitly_wait(10)
            
            # Execute script to hide automation detection
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            print("✅ Chrome WebDriver initialized successfully")
            return driver
        except Exception as e:
            print(f"❌ Failed to create Chrome driver: {e}")
            return None
    
    def get_all_games(self):
        """Get all games from database"""
        try:
            self.cursor.execute("""
                SELECT 
                    game_id,
                    season,
                    week,
                    season_type,
                    date,
                    ht.abbreviation as home_team,
                    at.abbreviation as away_team,
                    home_score,
                    away_score
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.id
                JOIN teams at ON g.away_team_id = at.id
                ORDER BY date DESC, game_id
                LIMIT 10  -- Start with small batch for testing
            """)
            
            games = self.cursor.fetchall()
            print(f"📊 Found {len(games)} games for processing")
            return games
            
        except Exception as e:
            print(f"❌ Failed to get games: {e}")
            return []
    
    def check_existing_plays(self, game_id):
        """Check if play-by-play data already exists in database"""
        try:
            self.cursor.execute("SELECT COUNT(*) as play_count FROM plays WHERE game_id = %s", (game_id,))
            count = self.cursor.fetchone()['play_count']
            return count > 0
        except:
            return False
    
    def expand_all_accordions(self, driver):
        """Find and expand all accordion sections on the page"""
        try:
            print("🔍 Looking for accordion buttons to expand...")
            
            # Wait for page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Find all accordion buttons - these are the collapsible sections
            accordion_selectors = [
                "button[aria-expanded='false']",  # Standard collapsed accordions
                "button[aria-controls*='details_']",  # ESPN-specific accordion pattern
                ".XiXVm.FDRJm.rGbDS",  # ESPN accordion button classes
                "[data-testid='prism-Accordion'] button"  # Prism accordion buttons
            ]
            
            expanded_count = 0
            for selector in accordion_selectors:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"   Found {len(buttons)} buttons with selector: {selector}")
                    
                    for button in buttons:
                        try:
                            # Check if button is actually collapsed
                            aria_expanded = button.get_attribute('aria-expanded')
                            if aria_expanded == 'false':
                                # Scroll button into view and click
                                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                                time.sleep(0.2)  # Brief pause
                                button.click()
                                expanded_count += 1
                                time.sleep(0.3)  # Brief pause after click
                                print(f"   ✅ Expanded accordion #{expanded_count}")
                        except Exception as e:
                            continue  # Skip if button can't be clicked
                            
                except Exception as e:
                    continue  # Try next selector
            
            print(f"🎯 Total accordions expanded: {expanded_count}")
            
            # Wait for content to load after expansion
            time.sleep(2)
            return expanded_count > 0
            
        except Exception as e:
            print(f"❌ Failed to expand accordions: {e}")
            return False
    
    def extract_plays_from_html(self, html_content):
        """Extract play-by-play data from expanded HTML"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            plays = []
            
            # Look for play containers - multiple possible selectors
            play_selectors = [
                "[data-testid='prism-LayoutCard']",
                ".liAe.wWJeS.qVkLt.pYgZk",
                "section.liAe",
                ".play-item",
                ".play-row"
            ]
            
            play_elements = []
            for selector in play_selectors:
                elements = soup.select(selector)
                if elements:
                    play_elements = elements
                    print(f"   Found {len(elements)} play elements with selector: {selector}")
                    break
            
            if not play_elements:
                print("   ❌ No play elements found")
                return plays
            
            for i, play_element in enumerate(play_elements):
                try:
                    play_data = self.parse_play_element(play_element, i)
                    if play_data:
                        plays.append(play_data)
                except Exception as e:
                    continue  # Skip problematic plays
            
            print(f"   ✅ Extracted {len(plays)} plays from HTML")
            return plays
            
        except Exception as e:
            print(f"❌ Failed to extract plays from HTML: {e}")
            return []
    
    def parse_play_element(self, play_element, play_index):
        """Parse individual play element into structured data"""
        try:
            play_data = {
                'play_index': play_index,
                'play_type': 'Unknown',
                'description': '',
                'quarter': 1,
                'time_remaining': '',
                'down': None,
                'distance': None,
                'yard_line': '',
                'yards_gained': 0,
                'possession_team': '',
                'first_down': False,
                'touchdown': False,
                'penalty': False
            }
            
            # Extract play type and description
            play_type_elem = play_element.select_one('.Bneh.tWudT, .play-type')
            if play_type_elem:
                play_data['play_type'] = play_type_elem.get_text(strip=True)
            
            # Extract play description
            desc_selectors = [
                '.FWLyZ.LiUVm',
                '.play-description', 
                '.play-text'
            ]
            
            for selector in desc_selectors:
                desc_elem = play_element.select_one(selector)
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if len(desc_text) > 10:  # Avoid time stamps
                        play_data['description'] = desc_text
                        break
            
            # Extract time
            time_elem = play_element.select_one('.FWLyZ.LiUVm')
            if time_elem:
                time_text = time_elem.get_text(strip=True)
                if re.match(r'\d+:\d+.*\d+(st|nd|rd|th)', time_text):
                    play_data['time_remaining'] = time_text
            
            # Parse play details from description
            if play_data['description']:
                self.parse_play_details(play_data)
            
            return play_data if play_data['description'] else None
            
        except Exception as e:
            return None
    
    def parse_play_details(self, play_data):
        """Extract detailed information from play description"""
        try:
            desc = play_data['description'].lower()
            
            # Detect play type
            if 'pass' in desc:
                play_data['play_type'] = 'Pass'
            elif 'rush' in desc or 'run' in desc:
                play_data['play_type'] = 'Run'
            elif 'punt' in desc:
                play_data['play_type'] = 'Punt'
            elif 'field goal' in desc:
                play_data['play_type'] = 'Field Goal'
            elif 'kickoff' in desc:
                play_data['play_type'] = 'Kickoff'
            elif 'sack' in desc:
                play_data['play_type'] = 'Sack'
            
            # Extract yards gained
            yard_patterns = [
                r'for\s+(-?\d+)\s+yard',
                r'(-?\d+)\s+yard\s+gain',
                r'gain\s+of\s+(-?\d+)',
                r'loss\s+of\s+(\d+)'
            ]
            
            for pattern in yard_patterns:
                match = re.search(pattern, desc)
                if match:
                    yards = int(match.group(1))
                    if 'loss' in desc and yards > 0:
                        yards = -yards
                    play_data['yards_gained'] = yards
                    break
            
            # Detect scoring plays
            if 'touchdown' in desc:
                play_data['touchdown'] = True
            
            # Detect penalties
            if 'penalty' in desc:
                play_data['penalty'] = True
            
            # Detect first downs
            if 'first down' in desc or '1st down' in desc:
                play_data['first_down'] = True
                
        except Exception as e:
            pass  # Continue with partial data
    
    def save_plays_to_database(self, game_id, plays):
        """Save plays to the database"""
        try:
            if not plays:
                return False
            
            # Insert plays into database
            for play in plays:
                self.cursor.execute("""
                    INSERT INTO plays (
                        game_id, play_index, play_type, description, quarter,
                        time_remaining, down, distance, yard_line, yards_gained,
                        possession_team, first_down, touchdown, penalty
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, play_index) DO NOTHING
                """, (
                    game_id, play['play_index'], play['play_type'], play['description'],
                    play['quarter'], play['time_remaining'], play['down'], play['distance'],
                    play['yard_line'], play['yards_gained'], play['possession_team'],
                    play['first_down'], play['touchdown'], play['penalty']
                ))
            
            self.conn.commit()
            print(f"   ✅ Saved {len(plays)} plays to database")
            return True
            
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Failed to save plays to database: {e}")
            return False
    
    def save_plays_to_csv(self, game_id, plays):
        """Save plays to CSV file as backup"""
        try:
            if not plays:
                return False
            
            timestamp = datetime.now().strftime('%Y%m%d')
            filename = f"nfl_playbyplay_advanced_{game_id}_{timestamp}.csv"
            filepath = self.csv_dir / filename
            
            fieldnames = [
                'game_id', 'play_index', 'play_type', 'description', 'quarter',
                'time_remaining', 'down', 'distance', 'yard_line', 'yards_gained',
                'possession_team', 'first_down', 'touchdown', 'penalty'
            ]
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for play in plays:
                    play['game_id'] = game_id
                    writer.writerow(play)
            
            print(f"   ✅ Saved {len(plays)} plays to CSV: {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to save plays to CSV: {e}")
            return False
    
    def scrape_game_playbyplay_advanced(self, game_record):
        """Advanced scraping with dynamic content expansion"""
        try:
            game_id = game_record['game_id']
            away_team = game_record['away_team']
            home_team = game_record['home_team']
            away_score = game_record['away_score']
            home_score = game_record['home_score']
            
            print(f"\n🎯 Processing: {away_team} {away_score} @ {home_team} {home_score}")
            print(f"   Game ID: {game_id}")
            
            # Check if already processed
            if self.check_existing_plays(game_id):
                print(f"⚠️ Play-by-play already exists for game {game_id}")
                self.skipped_games += 1
                return True
            
            # Create ESPN URL
            game_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_id}"
            print(f"   URL: {game_url}")
            
            # Create Chrome driver
            driver = self.create_driver()
            if not driver:
                self.failed_games += 1
                return False
            
            try:
                # Load the page
                print("   📡 Loading ESPN page...")
                driver.get(game_url)
                
                # Wait for initial page load
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Expand all accordion sections
                if not self.expand_all_accordions(driver):
                    print("   ⚠️ No accordions found or expanded")
                
                # Get the fully expanded HTML
                html_content = driver.page_source
                
                # Extract plays from the HTML
                plays = self.extract_plays_from_html(html_content)
                
                if plays:
                    # Save to database
                    db_success = self.save_plays_to_database(game_id, plays)
                    
                    # Save to CSV as backup
                    csv_success = self.save_plays_to_csv(game_id, plays)
                    
                    if db_success:
                        self.successful_games += 1
                        print(f"✅ Successfully processed game {game_id} - {len(plays)} plays")
                        return True
                    else:
                        self.failed_games += 1
                        return False
                else:
                    print(f"❌ No plays extracted for game {game_id}")
                    self.failed_games += 1
                    return False
                    
            finally:
                # Clean up driver
                driver.quit()
                
        except Exception as e:
            print(f"❌ Failed to process game {game_id}: {e}")
            self.failed_games += 1
            return False
    
    def run_advanced_scraping(self):
        """Run advanced play-by-play scraping"""
        print(f"\n{'='*80}")
        print("🏈 ADVANCED PLAY-BY-PLAY SCRAPER - DYNAMIC CONTENT")
        print(f"{'='*80}")
        
        # Get games to process
        games = self.get_all_games()
        
        if not games:
            print("❌ No games found for processing")
            return False
        
        # Process each game
        for i, game_record in enumerate(games):
            print(f"\n[{i+1}/{len(games)}] ===============================")
            
            success = self.scrape_game_playbyplay_advanced(game_record)
            
            # Rate limiting - be respectful
            time.sleep(random.uniform(3.0, 5.0))
            
            # Progress update
            if (i + 1) % 5 == 0:
                print(f"\n📊 Progress: {i+1}/{len(games)} - Success: {self.successful_games}, Failed: {self.failed_games}, Skipped: {self.skipped_games}")
        
        # Final summary
        print(f"\n{'='*80}")
        print("📊 ADVANCED SCRAPING COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successful scrapes: {self.successful_games}")
        print(f"❌ Failed scrapes: {self.failed_games}")
        print(f"⚠️ Skipped games: {self.skipped_games}")
        
        total_processed = self.successful_games + self.failed_games
        if total_processed > 0:
            success_rate = (self.successful_games / total_processed) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")
        
        return self.successful_games > 0

def main():
    """Run advanced play-by-play scraping"""
    scraper = AdvancedPlayByPlayScraper()
    
    # Run advanced scraping
    success = scraper.run_advanced_scraping()
    
    if success:
        print("\n🎉 ADVANCED SCRAPING SUCCESSFUL")
        print("Database contains detailed play-by-play data")
        print("Ready for comprehensive game analysis")
    else:
        print("\n💥 ADVANCED SCRAPING FAILED")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())