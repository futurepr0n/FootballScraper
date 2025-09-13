#!/usr/bin/env python3
"""
Production Play-by-Play Scraper - All 465 Games
Uses proven Gemini accordion expansion approach on all games in database
Processes games in batches with progress tracking and error recovery
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

class ProductionPlayByPlayScraper:
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
        self.processed_games = 0
        
        print("🏈 Production Play-by-Play Scraper initialized")
        print("🎯 Target: Process all games with accordion expansion approach")
    
    def setup_driver(self):
        """Setup ChromeWebDriver with stealth options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(60)
            driver.implicitly_wait(10)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            return driver
            
        except Exception as e:
            print(f"❌ Failed to setup ChromeDriver: {e}")
            return None
    
    def get_all_games_for_processing(self):
        """Get all games from database that need play-by-play data"""
        try:
            self.cursor.execute("""
                SELECT 
                    g.game_id,
                    g.season,
                    g.week,
                    g.season_type,
                    g.date,
                    ht.abbreviation as home_team,
                    at.abbreviation as away_team,
                    g.home_score,
                    g.away_score,
                    COALESCE(play_counts.play_count, 0) as existing_plays
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.id
                JOIN teams at ON g.away_team_id = at.id
                LEFT JOIN (
                    SELECT game_id::VARCHAR, COUNT(*) as play_count 
                    FROM plays 
                    GROUP BY game_id
                ) play_counts ON g.game_id = play_counts.game_id
                ORDER BY g.date DESC, g.game_id
            """)
            
            all_games = self.cursor.fetchall()
            
            # Separate games that need processing
            games_to_process = [game for game in all_games if game['existing_plays'] == 0]
            games_with_plays = [game for game in all_games if game['existing_plays'] > 0]
            
            print(f"📊 Database Summary:")
            print(f"   Total games in database: {len(all_games)}")
            print(f"   Games with play-by-play data: {len(games_with_plays)}")
            print(f"   Games needing processing: {len(games_to_process)}")
            
            if games_with_plays:
                print(f"   Example games with plays: {games_with_plays[0]['game_id']} ({games_with_plays[0]['existing_plays']} plays)")
            
            return games_to_process
            
        except Exception as e:
            print(f"❌ Failed to get games: {e}")
            return []
    
    def check_existing_plays(self, game_id):
        """Check if play-by-play data already exists for this game_id"""
        try:
            self.cursor.execute("SELECT COUNT(*) as play_count FROM plays WHERE game_id = %s", (str(game_id),))
            count = self.cursor.fetchone()['play_count']
            return count > 0
        except:
            return False
    
    def expand_all_accordions(self, driver):
        """Expand all ESPN accordion sections using proven approach"""
        try:
            print("   🔍 Looking for accordion buttons...")
            
            # Wait for page to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            # Use your proven accordion selectors
            accordion_selectors = [
                "button[aria-expanded='false']",
                "button[aria-controls*='details']", 
                "[data-testid*='Accordion'] button",
                "button[class*='Accordion']",
            ]
            
            total_expanded = 0
            
            for selector in accordion_selectors:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                    if buttons:
                        print(f"   📋 Found {len(buttons)} buttons with: {selector}")
                    
                    for button in buttons:
                        try:
                            aria_expanded = button.get_attribute('aria-expanded')
                            if aria_expanded == 'false':
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                                time.sleep(0.2)
                                driver.execute_script("arguments[0].click();", button)
                                total_expanded += 1
                                time.sleep(0.3)
                        except:
                            continue
                            
                except Exception as e:
                    continue
            
            print(f"   🎯 Expanded {total_expanded} accordions")
            
            if total_expanded > 0:
                print("   ⏳ Waiting for content to load...")
                time.sleep(5)
            
            return total_expanded > 0
            
        except Exception as e:
            print(f"   ❌ Accordion expansion failed: {e}")
            return False
    
    def extract_plays_from_page(self, driver, game_id):
        """Extract play-by-play data using proven text pattern approach"""
        try:
            print("   📄 Extracting plays...")
            
            plays_data = []
            play_index = 0
            
            # Strategy 1: Look for structured play containers
            play_containers = []
            container_selectors = [
                ".PlayByPlay__Row", "[data-testid*='play']", 
                ".play-item", ".play-row", "section[class*='play']"
            ]
            
            for selector in container_selectors:
                containers = driver.find_elements(By.CSS_SELECTOR, selector)
                if containers:
                    play_containers = containers
                    print(f"   ✅ Found {len(containers)} containers with: {selector}")
                    break
            
            if play_containers:
                for container in play_containers:
                    try:
                        play_text = container.text.strip()
                        if len(play_text) > 10:
                            plays_data.append({
                                'game_id': str(game_id),
                                'play_number': play_index,
                                'play_description': play_text[:500],
                                'quarter': None,
                                'time_remaining': None,
                                'down': None,
                                'distance': None,
                                'yard_line': None
                            })
                            play_index += 1
                    except:
                        continue
            
            # Strategy 2: Text pattern extraction (proven fallback)
            if not plays_data:
                print("   🔍 Using text pattern extraction...")
                
                body_text = driver.find_element(By.TAG_NAME, "body").text
                lines = body_text.split('\n')
                
                play_patterns = [
                    r'\d+(?:st|nd|rd|th)\s+(?:down|and)',
                    r'touchdown', r'field goal', r'punt', r'kickoff',
                    r'interception', r'fumble', r'sack', r'penalty'
                ]
                
                for line in lines:
                    line_clean = line.strip()
                    if len(line_clean) > 20:
                        for pattern in play_patterns:
                            if re.search(pattern, line_clean, re.IGNORECASE):
                                plays_data.append({
                                    'game_id': str(game_id),
                                    'play_number': play_index,
                                    'play_description': line_clean[:500],
                                    'quarter': None,
                                    'time_remaining': None,
                                    'down': None,
                                    'distance': None,
                                    'yard_line': None
                                })
                                play_index += 1
                                break
            
            print(f"   ✅ Extracted {len(plays_data)} plays")
            return plays_data
            
        except Exception as e:
            print(f"   ❌ Play extraction failed: {e}")
            return []
    
    def save_plays_to_database(self, plays_data):
        """Save plays to PostgreSQL using game_id"""
        if not plays_data:
            return 0
            
        try:
            insert_count = 0
            for play in plays_data:
                try:
                    # Use game_id directly as specified by user
                    self.cursor.execute("""
                        INSERT INTO plays (game_id, play_number, play_description, quarter, time_remaining, down, distance, yard_line)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, play_number) DO NOTHING;
                    """, (
                        play['game_id'],
                        play['play_number'], 
                        play['play_description'],
                        play['quarter'],
                        play['time_remaining'],
                        play['down'],
                        play['distance'],
                        play['yard_line']
                    ))
                    if self.cursor.rowcount > 0:
                        insert_count += 1
                except Exception as e:
                    print(f"   ⚠️  Insert error for play {play['play_number']}: {e}")
                    continue
            
            self.conn.commit()
            return insert_count
            
        except Exception as e:
            print(f"   ❌ Database save failed: {e}")
            self.conn.rollback()
            return 0
    
    def save_plays_to_csv(self, plays_data, game_id, home_team, away_team):
        """Save plays to CSV backup"""
        if not plays_data:
            return
            
        try:
            filename = f"playbyplay_{game_id}_{away_team}_at_{home_team}.csv"
            csv_path = self.csv_dir / filename
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['game_id', 'play_number', 'play_description', 'quarter', 'time_remaining', 'down', 'distance', 'yard_line']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(plays_data)
                
        except Exception as e:
            print(f"   ⚠️  CSV save failed: {e}")
    
    def scrape_single_game(self, driver, game):
        """Scrape play-by-play data for a single game"""
        game_id = game['game_id']
        home_team = game['home_team']  
        away_team = game['away_team']
        
        print(f"\n🎯 {away_team} {game['away_score']} @ {home_team} {game['home_score']}")
        print(f"   Game: {game_id} ({game['season']} Week {game['week']}, {game['season_type']})")
        print(f"   Date: {game['date']}")
        
        if self.check_existing_plays(game_id):
            print(f"   ⏭️  Already processed")
            self.skipped_games += 1
            return True
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_id}"
        print(f"   📡 Loading: {espn_url}")
        
        try:
            # Navigate and expand accordions
            driver.get(espn_url)
            expanded = self.expand_all_accordions(driver)
            
            # Extract plays
            plays_data = self.extract_plays_from_page(driver, game_id)
            
            if plays_data:
                # Save to database and CSV
                inserted_count = self.save_plays_to_database(plays_data)
                self.save_plays_to_csv(plays_data, game_id, home_team, away_team)
                
                print(f"   ✅ SUCCESS: {len(plays_data)} plays extracted, {inserted_count} saved to DB")
                self.successful_games += 1
                return True
            else:
                print(f"   ❌ FAILED: No plays extracted")
                self.failed_games += 1
                return False
                
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            self.failed_games += 1
            return False
    
    def run_production_scraper(self):
        """Run the full production scraper on all games"""
        print(f"\n{'='*80}")
        print(f"🏭 PRODUCTION PLAY-BY-PLAY SCRAPER")
        print(f"{'='*80}")
        
        # Get all games that need processing
        games_to_process = self.get_all_games_for_processing()
        
        if not games_to_process:
            print("🎉 All games already have play-by-play data!")
            return
        
        print(f"\n🚀 Starting processing of {len(games_to_process)} games...")
        
        # Setup driver
        driver = self.setup_driver()
        if not driver:
            print("❌ Failed to setup ChromeDriver")
            return
        
        try:
            start_time = datetime.now()
            
            # Process all games
            for i, game in enumerate(games_to_process, 1):
                print(f"\n[{i}/{len(games_to_process)}] {'='*40}")
                
                success = self.scrape_single_game(driver, game)
                self.processed_games += 1
                
                # Progress update every 10 games
                if i % 10 == 0:
                    elapsed = datetime.now() - start_time
                    rate = i / elapsed.total_seconds() * 60  # games per minute
                    remaining = len(games_to_process) - i
                    eta_minutes = remaining / rate if rate > 0 else 0
                    
                    print(f"\n📊 PROGRESS UPDATE:")
                    print(f"   Processed: {i}/{len(games_to_process)} ({i/len(games_to_process)*100:.1f}%)")
                    print(f"   Success: {self.successful_games}, Failed: {self.failed_games}, Skipped: {self.skipped_games}")
                    print(f"   Rate: {rate:.1f} games/minute, ETA: {eta_minutes:.0f} minutes")
                
                # Brief pause between games to avoid overwhelming ESPN
                time.sleep(2)
            
            # Final results
            total_time = datetime.now() - start_time
            print(f"\n{'='*80}")
            print(f"🏁 PRODUCTION SCRAPER COMPLETE")
            print(f"{'='*80}")
            print(f"⏱️  Total time: {total_time}")
            print(f"✅ Successful games: {self.successful_games}")
            print(f"❌ Failed games: {self.failed_games}")
            print(f"⏭️  Skipped games: {self.skipped_games}")
            print(f"📊 Success rate: {self.successful_games/(self.successful_games+self.failed_games)*100:.1f}%")
            
            if self.successful_games > 0:
                print(f"🎉 Successfully extracted play-by-play data for {self.successful_games} games!")
                print(f"💾 CSV backups saved to: {self.csv_dir}")
                print(f"🗃️  Database updated with play-by-play data")
            
        finally:
            driver.quit()
            print("🔒 Browser closed")

def main():
    """Main function to run production scraper"""
    scraper = ProductionPlayByPlayScraper()
    scraper.run_production_scraper()

if __name__ == "__main__":
    main()