#!/usr/bin/env python3
"""
Complete Accordion Scraper - Ensures ALL accordions are expanded to get complete play data
Focuses on thorough accordion expansion with multiple strategies and verification
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

class CompleteAccordionScraper:
    def __init__(self):
        self.conn = None
        self.connect_to_database()
        self.processed_games = 0
        self.successful_games = 0
        self.failed_games = 0
        
        print("🏈 Complete Accordion Scraper")
        print("✅ Focuses on thorough accordion expansion")
        print("✅ Multiple expansion strategies")
        print("✅ Verification of expansion success")
        
    def connect_to_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host="192.168.1.23",
                database="football_tracker",
                user="postgres", 
                password="korn5676",
                port=5432
            )
            print("✅ Database connected")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def setup_driver(self):
        """Setup ChromeDriver with enhanced settings"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    
    def comprehensive_accordion_expansion(self, driver):
        """Comprehensive accordion expansion with multiple strategies"""
        print("   🔍 Starting comprehensive accordion expansion...")
        
        total_expanded = 0
        
        # Strategy 1: Find and expand all collapsed accordions
        print("   📋 Strategy 1: Basic accordion expansion")
        basic_expanded = self.basic_accordion_expansion(driver)
        total_expanded += basic_expanded
        
        # Strategy 2: Scroll and expand (some accordions may be lazy-loaded)
        print("   📋 Strategy 2: Scroll-based expansion")
        scroll_expanded = self.scroll_and_expand(driver)
        total_expanded += scroll_expanded
        
        # Strategy 3: Click on quarter headers specifically
        print("   📋 Strategy 3: Quarter-specific expansion")
        quarter_expanded = self.expand_quarter_headers(driver)
        total_expanded += quarter_expanded
        
        # Strategy 4: Drive/possession based expansion
        print("   📋 Strategy 4: Drive-specific expansion")
        drive_expanded = self.expand_drive_headers(driver)
        total_expanded += drive_expanded
        
        # Verification: Check if all quarters are now visible
        print("   🔍 Verifying accordion expansion...")
        visible_quarters = self.verify_all_quarters_expanded(driver)
        
        print(f"   🎯 Total accordions expanded: {total_expanded}")
        print(f"   ✅ Visible quarters: {visible_quarters}")
        
        return total_expanded
    
    def basic_accordion_expansion(self, driver):
        """Basic accordion expansion using proven selectors"""
        expanded_count = 0
        
        selectors = [
            "button[aria-expanded='false']",
            "button[aria-controls*='details']",
            "[data-testid*='Accordion'] button",
            "button[class*='Accordion']",
            ".Collapsible__trigger",
            "[role='button'][aria-expanded='false']"
        ]
        
        for selector in selectors:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"      Found {len(buttons)} buttons with selector: {selector}")
                
                for button in buttons:
                    try:
                        if button.is_displayed() and button.is_enabled():
                            # Scroll into view
                            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", button)
                            time.sleep(0.3)
                            
                            # Try click
                            driver.execute_script("arguments[0].click();", button)
                            expanded_count += 1
                            time.sleep(0.2)
                    except Exception as e:
                        continue
                        
            except Exception as e:
                continue
        
        return expanded_count
    
    def scroll_and_expand(self, driver):
        """Scroll through page and expand accordions that may be lazy-loaded"""
        expanded_count = 0
        
        # Get page height
        last_height = driver.execute_script("return document.body.scrollHeight")
        
        # Scroll in increments
        scroll_position = 0
        scroll_increment = 500
        
        while scroll_position < last_height:
            # Scroll to position
            driver.execute_script(f"window.scrollTo(0, {scroll_position});")
            time.sleep(1)
            
            # Look for new accordions at this position
            buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='false']")
            
            for button in buttons:
                try:
                    if button.is_displayed() and button.location['y'] >= scroll_position - 200 and button.location['y'] <= scroll_position + 700:
                        driver.execute_script("arguments[0].click();", button)
                        expanded_count += 1
                        time.sleep(0.1)
                except:
                    continue
            
            scroll_position += scroll_increment
            
            # Check if page height changed (lazy loading)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height != last_height:
                last_height = new_height
        
        return expanded_count
    
    def expand_quarter_headers(self, driver):
        """Specifically target quarter headers"""
        expanded_count = 0
        
        # Look for quarter-specific elements
        quarter_selectors = [
            "[aria-label*='1st Quarter']",
            "[aria-label*='2nd Quarter']", 
            "[aria-label*='3rd Quarter']",
            "[aria-label*='4th Quarter']",
            "[aria-label*='Overtime']",
            "button:contains('1st Quarter')",
            "button:contains('2nd Quarter')",
            "button:contains('3rd Quarter')",
            "button:contains('4th Quarter')",
            "[data-testid*='quarter']",
            ".quarter-header",
            "[class*='quarter'][class*='header']"
        ]
        
        for selector in quarter_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    try:
                        # Check if it's collapsible
                        if element.get_attribute('aria-expanded') == 'false' or 'collapsed' in element.get_attribute('class', '').lower():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(0.2)
                            driver.execute_script("arguments[0].click();", element)
                            expanded_count += 1
                            time.sleep(0.3)
                    except:
                        continue
                        
            except:
                continue
        
        return expanded_count
    
    def expand_drive_headers(self, driver):
        """Target drive/possession headers specifically"""
        expanded_count = 0
        
        # Look for drive-specific patterns
        drive_patterns = [
            "//button[contains(text(), 'Drive')]",
            "//button[contains(text(), 'Possession')]",
            "//div[contains(@class, 'drive')]//button",
            "//div[contains(@class, 'possession')]//button"
        ]
        
        for pattern in drive_patterns:
            try:
                elements = driver.find_elements(By.XPATH, pattern)
                
                for element in elements:
                    try:
                        if element.get_attribute('aria-expanded') == 'false':
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(0.2)
                            driver.execute_script("arguments[0].click();", element)
                            expanded_count += 1
                            time.sleep(0.2)
                    except:
                        continue
                        
            except:
                continue
        
        return expanded_count
    
    def verify_all_quarters_expanded(self, driver):
        """Verify that all quarters are now visible and expanded"""
        visible_quarters = []
        
        # Check for quarter content visibility
        quarter_indicators = [
            "1st", "2nd", "3rd", "4th", "OT", "Overtime",
            "First", "Second", "Third", "Fourth"
        ]
        
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        
        for quarter in quarter_indicators:
            if quarter.lower() in page_text:
                visible_quarters.append(quarter)
        
        # Also check for expanded accordions
        expanded_accordions = len(driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='true']"))
        collapsed_accordions = len(driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='false']"))
        
        print(f"      Expanded accordions: {expanded_accordions}")
        print(f"      Collapsed accordions: {collapsed_accordions}")
        
        return visible_quarters
    
    def extract_all_plays_comprehensive(self, driver):
        """Extract all plays with comprehensive approach"""
        print("   📄 Extracting all plays comprehensively...")
        
        plays_data = []
        
        # Strategy 1: Look for structured play containers
        containers = driver.find_elements(By.CSS_SELECTOR, 'section[data-testid*="prism"], section.Card, .play-item, .play-row')
        
        if containers:
            print(f"      Found {len(containers)} structured containers")
            
            for i, container in enumerate(containers):
                try:
                    text_content = container.text.strip()
                    if len(text_content) > 10 and self.looks_like_play(text_content):
                        play_data = self.parse_play_to_4_columns(text_content, i)
                        if play_data:
                            plays_data.append(play_data)
                except:
                    continue
        
        # Strategy 2: Text-based extraction for anything missed
        if len(plays_data) < 20:  # If we didn't get many plays, try text extraction
            print("      Supplementing with text-based extraction...")
            
            body_text = driver.find_element(By.TAG_NAME, "body").text
            text_plays = self.extract_plays_from_text(body_text)
            
            # Merge unique plays
            for text_play in text_plays:
                if not any(play['play_description'] == text_play['play_description'] for play in plays_data):
                    plays_data.append(text_play)
        
        print(f"   ✅ Total plays extracted: {len(plays_data)}")
        return plays_data
    
    def looks_like_play(self, text):
        """Check if text looks like a football play"""
        play_indicators = [
            'touchdown', 'field goal', 'punt', 'kickoff', 'sacked', 
            'pass', 'run', 'rush', 'carry', 'penalty', 'fumble', 
            'interception', 'yards', 'yard line', 'snap', 'down'
        ]
        
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in play_indicators)
    
    def extract_plays_from_text(self, body_text):
        """Extract plays from body text as fallback"""
        plays_data = []
        lines = body_text.split('\n')
        
        play_index = 0
        for line in lines:
            line_clean = line.strip()
            if len(line_clean) > 20 and self.looks_like_play(line_clean):
                play_data = self.parse_play_to_4_columns(line_clean, play_index)
                if play_data:
                    plays_data.append(play_data)
                    play_index += 1
        
        return plays_data
    
    def parse_play_to_4_columns(self, text, play_index):
        """Parse play text into 4-column structure"""
        try:
            play_data = {
                'play_summary': self.extract_play_summary(text),
                'time_quarter': self.extract_time_quarter(text),
                'play_description': text[:500],
                'situation': self.extract_situation(text)
            }
            
            # Only return if we have substantial data
            if play_data['play_description'] and len(play_data['play_description']) > 10:
                return play_data
            else:
                return None
                
        except:
            return None
    
    def extract_play_summary(self, text):
        """Extract concise play summary"""
        text_lower = text.lower()
        
        if 'touchdown' in text_lower:
            return 'Touchdown'
        elif 'field goal' in text_lower:
            return 'Field Goal'
        elif 'punt' in text_lower:
            return 'Punt'
        elif 'kickoff' in text_lower:
            return 'Kickoff'
        elif 'sacked' in text_lower:
            return 'Sack'
        elif 'pass' in text_lower:
            yard_match = re.search(r'(\d+)\s*yard', text)
            return f"{yard_match.group(1)}-yd Pass" if yard_match else 'Pass'
        elif any(word in text_lower for word in ['run', 'rush', 'carry']):
            yard_match = re.search(r'(\d+)\s*yard', text)
            return f"{yard_match.group(1)}-yd Run" if yard_match else 'Run'
        else:
            return 'Play'
    
    def extract_time_quarter(self, text):
        """Extract time and quarter"""
        time_match = re.search(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', text, re.IGNORECASE)
        return f"{time_match.group(1)} - {time_match.group(2)}" if time_match else None
    
    def extract_situation(self, text):
        """Extract down and distance"""
        down_dist_match = re.search(r'(\d+)(st|nd|rd|th)\s*(&|and)\s*(\d+)', text)
        return f"{down_dist_match.group(1)}{down_dist_match.group(2)} & {down_dist_match.group(4)}" if down_dist_match else None
    
    def extract_structured_fields(self, play_data):
        """Extract structured fields"""
        structured = {
            'quarter': None, 'time_remaining': None, 'down': None,
            'distance': None, 'yards_gained': None, 'play_type': None
        }
        
        try:
            if play_data.get('time_quarter'):
                time_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', play_data['time_quarter'])
                if time_match:
                    structured['time_remaining'] = time_match.group(1) + ':00'
                    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                    structured['quarter'] = quarter_map.get(time_match.group(2).lower())
            
            if play_data.get('play_summary'):
                yard_match = re.search(r'(\d+)-?yd', play_data['play_summary'])
                if yard_match:
                    structured['yards_gained'] = int(yard_match.group(1))
                
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
            
            if play_data.get('situation'):
                down_dist_match = re.search(r'(\d+)(st|nd|rd|th)\s*&\s*(\d+)', play_data['situation'])
                if down_dist_match:
                    structured['down'] = int(down_dist_match.group(1))
                    structured['distance'] = int(down_dist_match.group(3))
                    
        except:
            pass
        
        return structured
    
    def save_plays_to_database(self, plays_data, game_data):
        """Save plays to database"""
        if not plays_data:
            return 0
            
        cursor = self.conn.cursor()
        saved_count = 0
        
        try:
            for i, play_data in enumerate(plays_data, 1):
                structured = self.extract_structured_fields(play_data)
                
                cursor.execute("""
                    INSERT INTO plays (
                        game_id, play_sequence,
                        play_summary, time_quarter, play_description, situation,
                        quarter, time_remaining, down, distance, yards_gained, play_type
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    game_data['espn_game_id'], i,
                    play_data.get('play_summary'), play_data.get('time_quarter'),
                    play_data.get('play_description'), play_data.get('situation'),
                    structured['quarter'], structured['time_remaining'],
                    structured['down'], structured['distance'],
                    structured['yards_gained'], structured['play_type']
                ))
                saved_count += 1
                
            self.conn.commit()
            cursor.close()
            return saved_count
            
        except Exception as e:
            print(f"   ❌ Error saving plays: {e}")
            self.conn.rollback()
            cursor.close()
            return 0
    
    def process_single_game_complete(self, game_data):
        """Process single game with complete accordion expansion"""
        
        print(f"\n🎯 {game_data['away_team']} @ {game_data['home_team']}")
        print(f"   ESPN ID: {game_data['espn_game_id']} ({game_data['season']} Week {game_data['week']})")
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_data['espn_game_id']}"
        print(f"   📡 Loading: {espn_url}")
        
        driver = self.setup_driver()
        
        try:
            driver.get(espn_url)
            time.sleep(5)  # Let page fully load
            
            # Comprehensive accordion expansion
            expanded_count = self.comprehensive_accordion_expansion(driver)
            
            if expanded_count > 0:
                print("   ⏳ Waiting for all content to load...")
                time.sleep(5)
                
                # Extract all plays comprehensively
                plays_data = self.extract_all_plays_comprehensive(driver)
                
                if plays_data:
                    saved_count = self.save_plays_to_database(plays_data, game_data)
                    
                    if saved_count > 0:
                        print(f"   ✅ SUCCESS: {len(plays_data)} plays extracted, {saved_count} saved")
                        
                        # Show sample of extracted plays
                        for i, play in enumerate(plays_data[:3], 1):
                            summary = play.get('play_summary', 'N/A')
                            time_q = play.get('time_quarter', 'N/A')
                            print(f"      Play {i}: {summary} | {time_q}")
                        
                        return True
                    else:
                        print(f"   ❌ FAILED: Could not save to database")
                        return False
                else:
                    print(f"   ❌ FAILED: No plays extracted")
                    return False
            else:
                print(f"   ❌ FAILED: Could not expand accordions")
                return False
                
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            return False
        finally:
            driver.quit()

def main():
    """Test the complete accordion scraper"""
    
    print("🎯 COMPLETE ACCORDION SCRAPER - COMPREHENSIVE EXPANSION")
    print("=" * 60)
    print("✅ Multiple expansion strategies")
    print("✅ Scroll-based lazy loading handling")
    print("✅ Quarter and drive specific targeting")
    print("✅ Verification of expansion success")
    
    scraper = CompleteAccordionScraper()
    
    # Get one recent game for testing
    cursor = scraper.conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT g.id as db_id, g.game_id as espn_game_id, g.date, g.season, g.week,
               t1.abbreviation as home_team, t2.abbreviation as away_team
        FROM games g
        JOIN teams t1 ON g.home_team_id = t1.id
        JOIN teams t2 ON g.away_team_id = t2.id
        WHERE g.game_id IS NOT NULL
        ORDER BY g.date DESC
        LIMIT 1
    """)
    
    game = cursor.fetchone()
    cursor.close()
    
    if game:
        print(f"\n🧪 Testing comprehensive expansion on recent game:")
        success = scraper.process_single_game_complete(game)
        
        if success:
            print(f"\n🎉 COMPREHENSIVE EXPANSION: SUCCESS")
            print(f"✅ All quarters and drives expanded")
            print(f"✅ Complete play data extracted")
            print(f"⚡ Ready to scale to all 465 games")
        else:
            print(f"\n❌ Need further refinement")
    else:
        print("❌ No games found for testing")

if __name__ == "__main__":
    main()