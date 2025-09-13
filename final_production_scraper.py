#!/usr/bin/env python3
"""
Final Production Scraper - Uses working Selenium approach with correct 4-column structure
- Uses ESPN game_id relationship (not internal ID)
- Extracts 4-line play card structure as specified
- Processes all 465 games
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import time
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

class FinalProductionScraper:
    def __init__(self):
        # Database connection
        try:
            self.conn = psycopg2.connect(
                host='192.168.1.23',
                database='football_tracker',
                user='postgres',
                password='korn5676',
                port=5432
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            print("✅ Database connected")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
        
        self.successful_games = 0
        self.failed_games = 0
        self.processed_games = 0
        
        print("🏈 Final Production Scraper initialized")

    def get_all_games(self):
        """Get all games from database using ESPN game_id"""
        self.cursor.execute("""
            SELECT g.game_id as espn_game_id, g.date, g.season, g.week,
                   t1.abbreviation as home_team, t2.abbreviation as away_team
            FROM games g
            JOIN teams t1 ON g.home_team_id = t1.id
            JOIN teams t2 ON g.away_team_id = t2.id
            WHERE g.game_id IS NOT NULL
            ORDER BY g.date DESC
        """)
        return self.cursor.fetchall()

    def parse_4_line_play_structure(self, text_content):
        """Parse ESPN 4-line play structure from text content"""
        plays_data = []
        lines = text_content.split('\n')
        
        i = 0
        while i < len(lines) - 2:  # Need at least 3 lines
            line1 = lines[i].strip()
            line2 = lines[i + 1].strip() if i + 1 < len(lines) else ""
            line3 = lines[i + 2].strip() if i + 2 < len(lines) else ""
            line4 = lines[i + 3].strip() if i + 3 < len(lines) else ""
            
            # Look for time-quarter pattern in line2
            time_quarter_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', line2)
            
            if time_quarter_match and line1 and line3:
                # This looks like a valid 4-line play structure
                play_data = {
                    'play_summary': line1,      # "2-yd Run", "13-yd Pass"
                    'time_quarter': line2,      # "8:53 - 3rd"
                    'play_description': line3,  # "(Shotgun) T.Spears left guard..."
                    'situation': None           # "3rd & 2 at JAX 18" (optional)
                }
                
                # Check if line4 is situation (down & distance)
                if line4 and re.search(r'\d+(st|nd|rd|th)\s*&\s*\d+', line4):
                    play_data['situation'] = line4
                
                plays_data.append(play_data)
                i += 4  # Skip to next potential play
            else:
                i += 1
        
        return plays_data

    def extract_structured_fields(self, play_data):
        """Extract structured fields from 4-line play data"""
        structured = {
            'quarter': None,
            'time_remaining': None,
            'down': None,
            'distance': None,
            'yards_gained': None,
            'play_type': None
        }
        
        # Parse time_quarter: "8:53 - 3rd"
        if play_data['time_quarter']:
            time_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', play_data['time_quarter'])
            if time_match:
                structured['time_remaining'] = time_match.group(1) + ':00'
                
                quarter_text = time_match.group(2).lower()
                quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                structured['quarter'] = quarter_map.get(quarter_text)
        
        # Parse play_summary: "2-yd Run", "13-yd Pass"
        if play_data['play_summary']:
            # Extract yards
            yard_match = re.search(r'(\d+)-?yd', play_data['play_summary'])
            if yard_match:
                structured['yards_gained'] = int(yard_match.group(1))
            
            # Extract play type
            summary_lower = play_data['play_summary'].lower()
            if 'run' in summary_lower:
                structured['play_type'] = 'rush'
            elif 'pass' in summary_lower:
                structured['play_type'] = 'pass'
            elif 'kick' in summary_lower:
                structured['play_type'] = 'kickoff'
            elif 'punt' in summary_lower:
                structured['play_type'] = 'punt'
            elif 'field goal' in summary_lower:
                structured['play_type'] = 'field_goal'
        
        # Parse situation: "3rd & 2 at JAX 18"
        if play_data.get('situation'):
            down_dist_match = re.match(r'(\d+)(st|nd|rd|th)\s*&\s*(\d+)', play_data['situation'])
            if down_dist_match:
                structured['down'] = int(down_dist_match.group(1))
                structured['distance'] = int(down_dist_match.group(3))
        
        return structured

    def save_plays_to_database(self, plays_data, game_id):
        """Save plays with correct 4-column structure"""
        saved_count = 0
        
        for i, play_data in enumerate(plays_data, 1):
            structured = self.extract_structured_fields(play_data)
            
            try:
                self.cursor.execute("""
                    INSERT INTO plays (
                        game_id, play_sequence,
                        play_summary, time_quarter, play_description, situation,
                        quarter, time_remaining, down, distance, yards_gained, play_type
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    game_id,  # ESPN game_id
                    i,
                    play_data['play_summary'],
                    play_data['time_quarter'],
                    play_data['play_description'],
                    play_data.get('situation'),
                    structured['quarter'],
                    structured['time_remaining'],
                    structured['down'],
                    structured['distance'],
                    structured['yards_gained'],
                    structured['play_type']
                ))
                saved_count += 1
            except Exception as e:
                print(f"   ⚠️ Error saving play {i}: {e}")
        
        self.conn.commit()
        return saved_count

    def scrape_game(self, game_data):
        """Scrape single game using working Selenium approach"""
        
        print(f"\n🎯 {game_data['away_team']} @ {game_data['home_team']}")
        print(f"   DB ID: {game_data['espn_game_id']} ({game_data['season']} Week {game_data['week']})")
        print(f"   Date: {game_data['date']}")
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_data['espn_game_id']}"
        print(f"   📡 Loading: {espn_url}")
        
        # Setup Chrome driver
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        
        driver = None
        try:
            # Use ChromeDriverManager to handle version compatibility
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(30)
            
            driver.get(espn_url)
            
            # Expand accordions (proven working approach)
            print("   🔍 Looking for accordion buttons...")
            
            accordion_selectors = [
                "button[aria-expanded='false']",
                "button[aria-controls*='details']", 
                "[data-testid*='Accordion'] button"
            ]
            
            total_expanded = 0
            for selector in accordion_selectors:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"   📋 Found {len(buttons)} buttons with: {selector}")
                
                for button in buttons:
                    try:
                        driver.execute_script("arguments[0].click();", button)
                        total_expanded += 1
                    except:
                        pass
            
            print(f"   🎯 Expanded {total_expanded} accordions")
            
            # Wait for content
            print("   ⏳ Waiting for content to load...")
            time.sleep(3)
            
            # Extract text content
            print("   📄 Extracting plays...")
            print("   🔍 Using text pattern extraction...")
            
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Parse 4-line play structure from text
            plays_data = self.parse_4_line_play_structure(body_text)
            
            print(f"   ✅ Extracted {len(plays_data)} plays")
            
            if plays_data:
                saved_count = self.save_plays_to_database(plays_data, game_data['espn_game_id'])
                print(f"   ✅ SUCCESS: {len(plays_data)} plays extracted, {saved_count} saved to DB")
                return True
            else:
                print("   ❌ No plays extracted")
                return False
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
        finally:
            if driver:
                driver.quit()

    def process_all_games(self, batch_size=10):
        """Process all games in batches"""
        
        print("🚀 FINAL PRODUCTION SCRAPER")
        print("=" * 50)
        
        games = self.get_all_games()
        total_games = len(games)
        
        print(f"🎯 Target: Process all {total_games} games with correct 4-column structure")
        print(f"📊 Database Summary:")
        
        # Check current play count
        self.cursor.execute("SELECT COUNT(*) FROM plays")
        result = self.cursor.fetchone()
        current_plays = result['count'] if result else 0
        print(f"   Current plays in database: {current_plays}")
        
        print(f"\n🚀 Starting processing of {total_games} games in batches of {batch_size}...")
        
        start_time = datetime.now()
        
        for i in range(0, total_games, batch_size):
            batch_games = games[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            print(f"\n[{batch_num}/{(total_games + batch_size - 1) // batch_size}] " + "=" * 40)
            
            for j, game in enumerate(batch_games, 1):
                print(f"\n🎯 Processing game {j}/{len(batch_games)} in this batch...")
                
                success = self.scrape_game(game)
                self.processed_games += 1
                
                if success:
                    self.successful_games += 1
                else:
                    self.failed_games += 1
                
                # Rate limiting
                if j < len(batch_games):
                    time.sleep(2)
            
            # Batch summary
            elapsed = datetime.now() - start_time
            print(f"\n{'🏁 BATCH COMPLETE'} ({len(batch_games)} games)")
            print(f"=" * 50)
            print(f"⏱️  Total time: {elapsed}")
            print(f"✅ Successful games: {self.successful_games}")
            print(f"❌ Failed games: {self.failed_games}")
            print(f"🎉 Successfully extracted play-by-play data for {self.successful_games} games!")
            
            # Check remaining
            remaining = total_games - self.processed_games
            if remaining > 0:
                print(f"📊 Remaining games to process: {remaining}")
                print(f"🔄 Run again to continue with next batch")
        
        print(f"\n🏁 FINAL SUMMARY")
        print(f"=" * 40)
        print(f"Total games processed: {self.processed_games}")
        print(f"Successful: {self.successful_games}")
        print(f"Failed: {self.failed_games}")
        success_rate = (self.successful_games / self.processed_games * 100) if self.processed_games > 0 else 0
        print(f"Success rate: {success_rate:.1f}%")

def main():
    """Main execution"""
    scraper = FinalProductionScraper()
    
    print("🎯 FINAL PRODUCTION SCRAPER - ALL 465 GAMES")
    print("✅ Using working Selenium approach")
    print("✅ Correct 4-column play structure")
    print("✅ ESPN game_id relationship")
    print("✅ Proper database schema")
    
    scraper.process_all_games(batch_size=10)

if __name__ == "__main__":
    main()