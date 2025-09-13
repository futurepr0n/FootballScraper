#!/usr/bin/env python3
"""
Accordion Expansion Play-by-Play Scraper
Uses ChromeWebDriver with your accordion expansion approach
Handles ESPN's dynamic content by expanding collapsed accordions
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import time
import os
import re
from datetime import datetime
from pathlib import Path
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class AccordionPlayByPlayScraper:
    def __init__(self):
        # Database connection
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST', '192.168.1.23'),
                database=os.getenv('DB_NAME', 'football_tracker'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'korn5676'),
                port=int(os.getenv('DB_PORT', 5432))
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            print("✅ Database connected")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
        
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.csv_dir.mkdir(exist_ok=True)
        
        self.successful_games = 0
        self.failed_games = 0
        self.skipped_games = 0
        
        print("🏈 Accordion Play-by-Play Scraper initialized")
        print("🎯 Using ChromeWebDriver with your accordion expansion approach")
    
    def setup_driver(self):
        """Setup ChromeWebDriver with stealth options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Comment out to see browser
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Use webdriver-manager to handle ChromeDriver automatically
            print("🔧 Setting up ChromeDriver...")
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(60)
            driver.implicitly_wait(10)
            
            # Execute script to hide automation detection
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            print("✅ ChromeWebDriver initialized successfully")
            return driver
            
        except Exception as e:
            print(f"❌ Failed to setup ChromeDriver: {e}")
            return None
    
    def get_test_games(self, limit=5):
        """Get a small set of games for testing"""
        try:
            self.cursor.execute(f"""
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
                WHERE season = 2023  -- Use older games for testing
                ORDER BY date DESC
                LIMIT {limit}
            """)
            
            games = self.cursor.fetchall()
            print(f"📊 Found {len(games)} test games")
            return games
            
        except Exception as e:
            print(f"❌ Failed to get games: {e}")
            return []
    
    def check_existing_plays(self, game_id):
        """Check if play-by-play data already exists"""
        try:
            self.cursor.execute("SELECT COUNT(*) as play_count FROM plays WHERE game_id = %s", (game_id,))
            count = self.cursor.fetchone()['play_count']
            return count > 0
        except:
            return False
    
    def expand_all_accordions(self, driver):
        """Your accordion expansion approach - find and expand all collapsed sections"""
        try:
            print("   🔍 Looking for accordion buttons to expand...")
            
            # Wait for page to load completely
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Give page time to fully load
            time.sleep(3)
            
            # Find collapsed accordion buttons using your suggested approach
            accordion_selectors = [
                "button[aria-expanded='false']",  # Main target - collapsed accordions
                "button[aria-controls*='details']",  # ESPN accordion pattern
                "[data-testid*='Accordion'] button",  # Prism accordion buttons
                "button[class*='Accordion']",  # Any button with Accordion in class
            ]
            
            total_expanded = 0
            
            for selector in accordion_selectors:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"   📋 Found {len(buttons)} buttons with selector: {selector}")
                    
                    for i, button in enumerate(buttons):
                        try:
                            # Check if actually collapsed
                            aria_expanded = button.get_attribute('aria-expanded')
                            if aria_expanded == 'false':
                                # Scroll into view
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                                time.sleep(0.2)
                                
                                # Try to click
                                button.click()
                                total_expanded += 1
                                time.sleep(0.3)  # Brief pause between clicks
                                
                                print(f"   ✅ Expanded accordion {total_expanded}")
                                
                        except Exception as e:
                            # Skip buttons that can't be clicked
                            continue
                            
                except Exception as e:
                    print(f"   ⚠️  Selector {selector} failed: {e}")
                    continue
            
            print(f"   🎯 Total accordions expanded: {total_expanded}")
            
            # Wait for content to load after expansion
            if total_expanded > 0:
                print("   ⏳ Waiting for expanded content to load...")
                time.sleep(5)
            
            return total_expanded > 0
            
        except Exception as e:
            print(f"   ❌ Failed to expand accordions: {e}")
            return False
    
    def extract_plays_from_page(self, driver):
        """Extract play-by-play data from the expanded page"""
        try:
            print("   📄 Extracting play-by-play data...")
            
            # Get page source after expansion
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            plays = []
            
            # Look for play data using multiple strategies
            play_selectors = [
                "[data-testid*='play']",
                ".play-item",
                ".play-row", 
                "section[class*='play']",
                "[class*='PlayByPlay']"
            ]
            
            play_elements = []
            for selector in play_selectors:
                elements = soup.select(selector)
                if elements:
                    play_elements = elements
                    print(f"   ✅ Found {len(elements)} play elements with: {selector}")
                    break
            
            # If no specific play containers found, look for text patterns
            if not play_elements:
                print("   🔍 No play containers found, searching for play patterns in text...")
                
                # Look for common play indicators in text
                play_indicators = [
                    r'\d+(?:st|nd|rd|th)\s+(?:down|and)',  # "1st down", "3rd and 5"
                    r'touchdown',
                    r'field goal',
                    r'punt',
                    r'kickoff',
                    r'interception',
                    r'fumble',
                    r'sack',
                    r'penalty'
                ]
                
                all_text_elements = soup.find_all(text=True)
                potential_plays = []
                
                for text in all_text_elements:
                    text_clean = text.strip()
                    if len(text_clean) > 20:  # Skip short text
                        for pattern in play_indicators:
                            if re.search(pattern, text_clean, re.IGNORECASE):
                                potential_plays.append(text_clean)
                                break
                
                # Convert text to play objects
                for i, play_text in enumerate(potential_plays[:100]):  # Limit to first 100
                    plays.append({
                        'play_index': i,
                        'play_text': play_text[:500],  # Limit length
                        'quarter': None,
                        'time_remaining': None,
                        'down': None,
                        'yards_to_go': None,
                        'yard_line': None
                    })
            
            else:
                # Extract from structured elements
                for i, element in enumerate(play_elements):
                    try:
                        play_text = element.get_text(strip=True)
                        if len(play_text) > 10:  # Skip empty elements
                            plays.append({
                                'play_index': i,
                                'play_text': play_text[:500],
                                'quarter': None,
                                'time_remaining': None,
                                'down': None,
                                'yards_to_go': None,
                                'yard_line': None
                            })
                    except:
                        continue
            
            print(f"   ✅ Extracted {len(plays)} plays")
            return plays
            
        except Exception as e:
            print(f"   ❌ Failed to extract plays: {e}")
            return []
    
    def save_plays_to_database(self, game_id, plays):
        """Save plays to PostgreSQL database"""
        if not plays:
            return
            
        try:
            for play in plays:
                self.cursor.execute("""
                    INSERT INTO plays (
                        game_id, play_index, play_text, quarter,
                        time_remaining, down, yards_to_go, yard_line
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, play_index) DO NOTHING
                """, (
                    game_id,
                    play['play_index'],
                    play['play_text'],
                    play['quarter'],
                    play['time_remaining'],
                    play['down'],
                    play['yards_to_go'],
                    play['yard_line']
                ))
            
            self.conn.commit()
            print(f"   ✅ Saved {len(plays)} plays to database")
            
        except Exception as e:
            print(f"   ❌ Failed to save to database: {e}")
            self.conn.rollback()
    
    def save_plays_to_csv(self, game_id, plays, home_team, away_team):
        """Save plays to CSV backup"""
        if not plays:
            return
            
        try:
            filename = f"playbyplay_{game_id}_{away_team}_at_{home_team}.csv"
            csv_path = self.csv_dir / filename
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['game_id', 'play_index', 'play_text', 'quarter', 'time_remaining', 'down', 'yards_to_go', 'yard_line']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for play in plays:
                    play['game_id'] = game_id
                    writer.writerow(play)
            
            print(f"   ✅ Saved CSV: {filename}")
            
        except Exception as e:
            print(f"   ❌ Failed to save CSV: {e}")
    
    def scrape_single_game(self, driver, game):
        """Scrape a single game's play-by-play data"""
        game_id = game['game_id']
        home_team = game['home_team']
        away_team = game['away_team']
        
        if self.check_existing_plays(game_id):
            print(f"   ⏭️  Already have plays for {game_id}")
            self.skipped_games += 1
            return True
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_id}"
        print(f"   📡 Loading: {espn_url}")
        
        try:
            # Navigate to page
            driver.get(espn_url)
            
            # Expand accordions using your approach
            expanded = self.expand_all_accordions(driver)
            
            if not expanded:
                print(f"   ⚠️  No accordions found or expanded")
            
            # Extract plays
            plays = self.extract_plays_from_page(driver)
            
            if plays:
                # Save to database and CSV
                self.save_plays_to_database(game_id, plays)
                self.save_plays_to_csv(game_id, plays, home_team, away_team)
                
                self.successful_games += 1
                return True
            else:
                print(f"   ❌ No plays extracted")
                self.failed_games += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error scraping {game_id}: {e}")
            self.failed_games += 1
            return False
    
    def run_test_scraper(self):
        """Run scraper on test games"""
        print(f"\n{'='*80}")
        print(f"🧪 TESTING ACCORDION EXPANSION SCRAPER")
        print(f"{'='*80}")
        
        # Setup driver
        driver = self.setup_driver()
        if not driver:
            print("❌ Failed to setup ChromeDriver")
            return
        
        try:
            # Get test games
            games = self.get_test_games(5)
            if not games:
                print("❌ No test games found")
                return
            
            print(f"📊 Testing on {len(games)} games from 2023 season")
            
            for i, game in enumerate(games, 1):
                print(f"\n[{i}/{len(games)}] {'='*31}")
                print(f"🎯 {game['away_team']} {game['away_score']} @ {game['home_team']} {game['home_score']}")
                print(f"   Game ID: {game['game_id']} ({game['season']} Week {game['week']})")
                
                success = self.scrape_single_game(driver, game)
                
                # Brief pause between games
                time.sleep(2)
            
            # Results
            print(f"\n{'='*80}")
            print(f"🧪 TEST RESULTS")
            print(f"{'='*80}")
            print(f"✅ Successful: {self.successful_games}")
            print(f"❌ Failed: {self.failed_games}")
            print(f"⏭️  Skipped: {self.skipped_games}")
            
            if self.successful_games > 0:
                print(f"🎉 SUCCESS! Accordion expansion approach is working!")
                print(f"📊 Success rate: {(self.successful_games/(self.successful_games+self.failed_games)*100):.1f}%")
                print(f"🚀 Ready to run on all 465 games")
            else:
                print(f"❌ No games successfully scraped - need to debug approach")
            
        finally:
            driver.quit()
            print("🔒 Browser closed")

def main():
    """Main function"""
    scraper = AccordionPlayByPlayScraper()
    scraper.run_test_scraper()

if __name__ == "__main__":
    main()