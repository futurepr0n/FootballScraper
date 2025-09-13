#!/usr/bin/env python3
"""
Comprehensive Play-by-Play Scraper
Extracts complete structured data from ESPN play-by-play pages
Parses quarter, time, down, distance, yards, player info, scoring, penalties
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import time
import os
import re
import random
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
from enhanced_text_parser import enhanced_parse_play_text

class ComprehensivePlayByPlayScraper:
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
        
        # CSV backup directory
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.csv_dir.mkdir(exist_ok=True)
        
        self.successful_games = 0
        self.failed_games = 0
        self.skipped_games = 0
        
        print("🏈 Comprehensive Play-by-Play Scraper initialized")
        print("🎯 Focus: Extract complete structured play-by-play data for all games")
    
    def get_games_needing_processing(self):
        """Get games that need complete play-by-play data extraction"""
        try:
            # Get games with their database ID (for foreign keys) and ESPN game_id (for URLs)
            self.cursor.execute("""
                SELECT 
                    g.id as db_id,
                    g.game_id as espn_game_id,
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
                    SELECT game_id, COUNT(*) as play_count 
                    FROM plays 
                    WHERE quarter IS NOT NULL AND time_remaining IS NOT NULL
                    GROUP BY game_id
                ) play_counts ON g.id = play_counts.game_id
                WHERE COALESCE(play_counts.play_count, 0) = 0
                ORDER BY g.date DESC, g.game_id
            """)
            
            games = self.cursor.fetchall()
            print(f"📊 Found {len(games)} games needing complete structured data")
            return games
            
        except Exception as e:
            print(f"❌ Failed to get games: {e}")
            return []
    
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
    
    def expand_all_accordions(self, driver):
        """Expand all ESPN accordion sections"""
        try:
            print("   🔍 Looking for accordion buttons...")
            
            # Wait for page to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            # Use proven accordion selectors
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
    
    def parse_play_text(self, play_text, play_index):
        """Parse structured data from ESPN play text"""
        try:
            # Initialize play data structure
            play_data = {
                'play_number': play_index,
                'play_description': play_text.strip()[:500],
                'quarter': None,
                'time_remaining': None, 
                'down': None,
                'distance': None,
                'yard_line': None,
                'play_type': None,
                'yards_gained': None,
                'touchdown': False,
                'penalty': False,
                'turnover': False,
                'player_names': []
            }
            
            text = play_text.strip()
            
            # Parse quarter and time: "14:16 - 1st" or "2:30 - 4th"
            time_pattern = r'(\d{1,2}:\d{2})\s*-\s*(\d+(?:st|nd|rd|th))'
            time_match = re.search(time_pattern, text)
            if time_match:
                play_data['time_remaining'] = time_match.group(1)
                quarter_text = time_match.group(2)
                play_data['quarter'] = int(re.findall(r'\d+', quarter_text)[0])
            
            # Parse down and distance: "2nd & 7 at SF 42" or "1st & 10 at DEN 25"
            down_pattern = r'(\d+)(?:st|nd|rd|th)\s*&\s*(\d+|Goal)\s*at\s*([A-Z]{2,3})\s*(\d+)'
            down_match = re.search(down_pattern, text)
            if down_match:
                play_data['down'] = int(down_match.group(1))
                distance_text = down_match.group(2)
                play_data['distance'] = 0 if distance_text == 'Goal' else int(distance_text)
                play_data['yard_line'] = int(down_match.group(4))
            
            # Parse yards gained: "5 yard gain", "12 yard pass", "3 yard loss"
            yards_patterns = [
                r'(\d+)\s*yard\s*(?:gain|pass|rush|run)',
                r'(\d+)\s*yard\s*loss',
                r'(?:gained|for)\s*(\d+)\s*yard'
            ]
            for pattern in yards_patterns:
                yards_match = re.search(pattern, text, re.IGNORECASE)
                if yards_match:
                    yards = int(yards_match.group(1))
                    play_data['yards_gained'] = -yards if 'loss' in text.lower() else yards
                    break
            
            # Detect play type
            play_types = {
                'pass': r'(?:pass|threw|completion|incomplete)',
                'rush': r'(?:rush|ran|carry|handoff)',
                'punt': r'punt',
                'field_goal': r'field\s*goal',
                'kickoff': r'kickoff',
                'extra_point': r'extra\s*point',
                'safety': r'safety',
                'kneel': r'kneel'
            }
            
            for play_type, pattern in play_types.items():
                if re.search(pattern, text, re.IGNORECASE):
                    play_data['play_type'] = play_type
                    break
            
            # Detect special events
            if re.search(r'touchdown', text, re.IGNORECASE):
                play_data['touchdown'] = True
            if re.search(r'penalty|holding|false\s*start|offsides', text, re.IGNORECASE):
                play_data['penalty'] = True
            if re.search(r'interception|fumble|turnover', text, re.IGNORECASE):
                play_data['turnover'] = True
            
            # Extract player names (capitalize words that look like names)
            name_pattern = r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b'
            names = re.findall(name_pattern, text)
            play_data['player_names'] = names[:3]  # Limit to 3 names
            
            return play_data
            
        except Exception as e:
            print(f"   ⚠️  Play parsing error: {e}")
            return {
                'play_number': play_index,
                'play_description': play_text.strip()[:500],
                'quarter': None,
                'time_remaining': None,
                'down': None,
                'distance': None,
                'yard_line': None,
                'play_type': None,
                'yards_gained': None,
                'touchdown': False,
                'penalty': False,
                'turnover': False,
                'player_names': []
            }
    
    def parse_time_text(self, time_text):
        """Parse time and quarter from ESPN time element"""
        try:
            result = {}
            # ESPN time format: "14:16 - 1st" or "2:30 - 4th"
            time_pattern = r'(\d{1,2}:\d{2})\s*-\s*(\d+)(?:st|nd|rd|th)'
            match = re.search(time_pattern, time_text)
            if match:
                result['time_remaining'] = match.group(1)
                result['quarter'] = int(match.group(2))
            return result
        except:
            return {}
    
    def parse_down_distance_text(self, down_text):
        """Parse down and distance from ESPN down element"""
        try:
            result = {}
            # ESPN down format: "2nd & 7 at SF 42" or "1st & 10 at DEN 25"
            down_pattern = r'(\d+)(?:st|nd|rd|th)\s*&\s*(\d+|Goal)\s*at\s*([A-Z]{2,3})\s*(\d+)'
            match = re.search(down_pattern, down_text)
            if match:
                result['down'] = int(match.group(1))
                distance_text = match.group(2)
                result['distance'] = 0 if distance_text == 'Goal' else int(distance_text)
                result['yard_line'] = int(match.group(4))
            return result
        except:
            return {}

    def parse_play_card_structure(self, container, play_index):
        """Parse ESPN play card using exact DOM structure from user's HTML example
        Structure:
        <section data-testid="prism-LayoutCard">
          <div><div><div>Kickoff</div><div>15:00 - 1st</div></div></div>
          <div><div><div>J.Bates kicks 63 yards...</div><div></div></div></div>
        </section>
        """
        try:
            # Initialize play data
            play_data = {
                'play_number': play_index,
                'play_description': None,
                'quarter': None,
                'time_remaining': None,
                'down': None,
                'distance': None,
                'yard_line': None,
                'play_type': None,
                'yards_gained': None,
                'touchdown': False,
                'penalty': False,
                'turnover': False,
                'player_names': []
            }
            
            # Get all div elements within the card, filtering for meaningful content
            all_divs = container.find_elements(By.TAG_NAME, "div")
            meaningful_texts = []
            
            for div in all_divs:
                text = div.text.strip()
                # Only keep divs with meaningful, short text (avoid huge containers)
                if text and 5 < len(text) < 200:
                    # Avoid duplicates (nested divs can repeat text)
                    if text not in meaningful_texts:
                        meaningful_texts.append(text)
            
            # Based on user's DOM structure, expected pattern:
            # meaningful_texts[0] = "Kickoff" (play type)
            # meaningful_texts[1] = "15:00 - 1st" (time/quarter)  
            # meaningful_texts[2] = "J.Bates kicks 63 yards..." (description)
            
            if len(meaningful_texts) >= 2:
                # Parse play type (first meaningful div)
                play_type_text = meaningful_texts[0]
                play_data['play_type'] = self.parse_play_type_header(play_type_text)
                
                # Extract yards from play type header if present
                yards_match = re.search(r'(\d+)-yd', play_type_text)
                if yards_match:
                    play_data['yards_gained'] = int(yards_match.group(1))
                
                # Parse time/quarter (second meaningful div)
                time_quarter_text = meaningful_texts[1] 
                time_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', time_quarter_text, re.IGNORECASE)
                if time_match:
                    play_data['time_remaining'] = time_match.group(1)
                    quarter_text = time_match.group(2).lower()
                    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                    play_data['quarter'] = quarter_map.get(quarter_text)
                
                # Parse description (third meaningful div if available)
                if len(meaningful_texts) >= 3:
                    description_text = meaningful_texts[2]
                    play_data['play_description'] = description_text[:500]
                    
                    # Extract player names from description
                    player_names = re.findall(r'([A-Z]\.[A-Z][a-z]+)', description_text)
                    play_data['player_names'] = list(dict.fromkeys(player_names))[:3]
                    
                    # Check for special events
                    desc_upper = description_text.upper()
                    if 'TOUCHDOWN' in desc_upper or ' TD ' in desc_upper:
                        play_data['touchdown'] = True
                    if 'PENALTY' in desc_upper:
                        play_data['penalty'] = True
                    if 'FUMBLE' in desc_upper or 'INTERCEPTION' in desc_upper:
                        play_data['turnover'] = True
                        
                    # Extract yards from description if not found in header
                    if not play_data['yards_gained']:
                        yards_patterns = [
                            r'for (-?\d+) yards?',
                            r'kicks (-?\d+) yards?', 
                            r'(-?\d+) yard (?:gain|loss)',
                        ]
                        for pattern in yards_patterns:
                            yards_match = re.search(pattern, description_text, re.IGNORECASE)
                            if yards_match:
                                play_data['yards_gained'] = int(yards_match.group(1))
                                break
                
                # Look for down/distance in remaining texts
                for text in meaningful_texts[3:]:
                    down_match = re.search(r'(1st|2nd|3rd|4th)\s*&\s*(\d+)\s*at\s*([A-Z]{2,3})\s*(\d+)', text)
                    if down_match:
                        down_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4}
                        play_data['down'] = down_map.get(down_match.group(1).lower())
                        play_data['distance'] = int(down_match.group(2))
                        play_data['yard_line'] = int(down_match.group(4))
                        break
                
                # Only return if we got essential structured data
                if play_data['play_type'] and play_data['quarter'] and play_data['time_remaining']:
                    return play_data
            
            return None
            
        except Exception as e:
            print(f"   ⚠️  Card structure parse error: {e}")
            return None

    def parse_play_type_header(self, header_text):
        """Parse play type from header text like 'Kickoff', '13-yd Pass', etc."""
        text = header_text.lower().strip()
        
        if 'kickoff' in text:
            return 'kickoff'
        elif 'field goal' in text:
            return 'field_goal'
        elif 'extra point' in text:
            return 'extra_point'
        elif 'punt' in text:
            return 'punt'
        elif 'safety' in text:
            return 'safety'
        elif 'sack' in text:
            return 'sack'
        elif 'run' in text or 'rush' in text:
            return 'rush'
        elif 'pass' in text:
            return 'pass'
        elif 'incomplete' in text:
            return 'pass'
        else:
            # Return cleaned version for debugging
            return text[:20]

    def extract_structured_plays(self, driver, db_id, espn_game_id):
        """Extract complete structured play-by-play data using ESPN-specific selectors"""
        try:
            print("   📄 Extracting structured plays...")
            
            plays_data = []
            play_index = 0
            
            # Strategy 1: Target ESPN play cards with exact DOM structure parsing
            play_selectors = [
                "section[data-testid='prism-LayoutCard']",  # Exact match from user's HTML
                "section[data-testid*='prism']",           # Partial match fallback
                "section",                                 # Broad fallback
            ]
            
            for selector in play_selectors:
                containers = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"   📋 Found {len(containers)} containers with selector: {selector}")
                
                # Filter containers that look like play cards
                valid_play_cards = []
                for container in containers:
                    container_text = container.text.strip()
                    # Must contain time pattern and play keywords to be a valid play card
                    if (len(container_text) > 20 and 
                        re.search(r'\d{1,2}:\d{2}\s*-\s*(1st|2nd|3rd|4th)', container_text) and
                        any(keyword in container_text.lower() for keyword in ['kick', 'pass', 'run', 'punt', 'yards', 'touchdown'])):
                        valid_play_cards.append(container)
                
                if valid_play_cards:
                    print(f"   ✅ Found {len(valid_play_cards)} valid play cards with {selector}")
                    
                    for container in valid_play_cards:
                        try:
                            # Extract structured data using exact DOM pattern from user's example
                            parsed_play = self.parse_play_card_structure(container, play_index)
                            
                            if parsed_play:
                                parsed_play['game_id'] = db_id
                                plays_data.append(parsed_play)
                                play_index += 1
                        except Exception as e:
                            print(f"   ⚠️  Error parsing play card {play_index}: {e}")
                            continue
                    
                    if plays_data:
                        break  # Found plays, no need to try other selectors
            
            # Strategy 2: Fallback to general text extraction
            if not plays_data:
                print("   🔍 Using general text extraction as fallback...")
                body_text = driver.find_element(By.TAG_NAME, "body").text
                lines = body_text.split('\n')
                
                for line in lines:
                    line = line.strip()
                    if len(line) > 20 and any(keyword in line.lower() for keyword in 
                        ['down', 'yard', 'pass', 'rush', 'punt', 'kick', 'touchdown', 'penalty']):
                        
                        parsed_play = enhanced_parse_play_text(line, play_index)
                        parsed_play['game_id'] = db_id
                        plays_data.append(parsed_play)
                        play_index += 1
            
            print(f"   ✅ Extracted {len(plays_data)} structured plays")
            return plays_data
            
        except Exception as e:
            print(f"   ❌ Play extraction failed: {e}")
            return []
    
    def save_plays_to_database(self, plays_data):
        """Save complete structured plays to PostgreSQL"""
        if not plays_data:
            return 0
            
        try:
            insert_count = 0
            for play in plays_data:
                try:
                    # Convert player names list to comma-separated string
                    player_names_str = ', '.join(play['player_names']) if play['player_names'] else None
                    
                    self.cursor.execute("""
                        INSERT INTO plays (
                            game_id, play_number, play_description, quarter, time_remaining, 
                            down, distance, yard_line, play_type, yards_gained, 
                            touchdown, penalty, turnover, player_names
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id, play_number) DO UPDATE SET
                            play_description = EXCLUDED.play_description,
                            quarter = EXCLUDED.quarter,
                            time_remaining = EXCLUDED.time_remaining,
                            down = EXCLUDED.down,
                            distance = EXCLUDED.distance,
                            yard_line = EXCLUDED.yard_line,
                            play_type = EXCLUDED.play_type,
                            yards_gained = EXCLUDED.yards_gained,
                            touchdown = EXCLUDED.touchdown,
                            penalty = EXCLUDED.penalty,
                            turnover = EXCLUDED.turnover,
                            player_names = EXCLUDED.player_names;
                    """, (
                        play['game_id'], play['play_number'], play['play_description'],
                        play['quarter'], play['time_remaining'], play['down'], play['distance'],
                        play['yard_line'], play['play_type'], play['yards_gained'],
                        play['touchdown'], play['penalty'], play['turnover'], player_names_str
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
    
    def save_plays_to_csv(self, plays_data, espn_game_id, home_team, away_team):
        """Save complete structured plays to CSV backup"""
        if not plays_data:
            return
            
        try:
            filename = f"comprehensive_playbyplay_{espn_game_id}_{away_team}_at_{home_team}.csv"
            csv_path = self.csv_dir / filename
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'game_id', 'play_number', 'play_description', 'quarter', 'time_remaining',
                    'down', 'distance', 'yard_line', 'play_type', 'yards_gained', 
                    'touchdown', 'penalty', 'turnover', 'player_names'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for play in plays_data:
                    # Convert list to string for CSV
                    csv_play = play.copy()
                    csv_play['player_names'] = ', '.join(play['player_names']) if play['player_names'] else ''
                    writer.writerow(csv_play)
                    
        except Exception as e:
            print(f"   ⚠️  CSV save failed: {e}")
    
    def scrape_single_game(self, driver, game):
        """Scrape complete structured play-by-play data for a single game"""
        db_id = game['db_id']
        espn_game_id = game['espn_game_id'] 
        home_team = game['home_team']  
        away_team = game['away_team']
        
        print(f"\n🎯 {away_team} {game['away_score']} @ {home_team} {game['home_score']}")
        print(f"   DB ID: {db_id}, ESPN ID: {espn_game_id} ({game['season']} Week {game['week']}, {game['season_type']})")
        print(f"   Date: {game['date']}")
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{espn_game_id}"
        print(f"   📡 Loading: {espn_url}")
        
        try:
            # Navigate and expand accordions
            driver.get(espn_url)
            expanded = self.expand_all_accordions(driver)
            
            # Extract structured plays
            plays_data = self.extract_structured_plays(driver, db_id, espn_game_id)
            
            if plays_data:
                # Save to database and CSV
                inserted_count = self.save_plays_to_database(plays_data)
                self.save_plays_to_csv(plays_data, espn_game_id, home_team, away_team)
                
                # Count structured vs basic plays (any structured field populated)
                structured_count = sum(1 for p in plays_data if p['play_type'] or p['yards_gained'] or p['player_names'])
                
                print(f"   ✅ SUCCESS: {len(plays_data)} plays extracted, {structured_count} with structured data, {inserted_count} saved to DB")
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
    
    def run_comprehensive_scraper(self, batch_size=10):
        """Run the comprehensive scraper with complete structured data extraction"""
        print(f"\n{'='*80}")
        print(f"🏭 COMPREHENSIVE STRUCTURED PLAY-BY-PLAY SCRAPER")
        print(f"{'='*80}")
        
        # Get all games that need complete processing
        games_to_process = self.get_games_needing_processing()
        
        if not games_to_process:
            print("🎉 All games already have complete structured play-by-play data!")
            return
        
        print(f"\n🚀 Starting processing of {len(games_to_process)} games in batches of {batch_size}...")
        
        # Setup driver
        driver = self.setup_driver()
        if not driver:
            print("❌ Failed to setup ChromeDriver")
            return
        
        try:
            start_time = datetime.now()
            
            # Process games in batches
            for i, game in enumerate(games_to_process[:batch_size], 1):
                print(f"\n[{i}/{min(batch_size, len(games_to_process))}] {'='*40}")
                
                success = self.scrape_single_game(driver, game)
                
                # Brief pause between games
                time.sleep(2)
            
            # Results
            total_time = datetime.now() - start_time
            print(f"\n{'='*80}")
            print(f"🏁 BATCH COMPLETE ({batch_size} games)")
            print(f"{'='*80}")
            print(f"⏱️  Total time: {total_time}")
            print(f"✅ Successful games: {self.successful_games}")
            print(f"❌ Failed games: {self.failed_games}")
            print(f"⏭️  Skipped games: {self.skipped_games}")
            
            if self.successful_games > 0:
                print(f"🎉 Successfully extracted complete structured play-by-play data for {self.successful_games} games!")
                print(f"💾 CSV backups saved to: {self.csv_dir}")
                print(f"🗃️  Database updated with complete structured data")
                
                # Show next batch info
                remaining = len(games_to_process) - batch_size
                if remaining > 0:
                    print(f"\n📊 Remaining games to process: {remaining}")
                    print(f"🔄 Run again to continue with next batch")
            
        finally:
            driver.quit()
            print("🔒 Browser closed")

def main():
    """Main function to run comprehensive structured scraper"""
    scraper = ComprehensivePlayByPlayScraper()
    scraper.run_comprehensive_scraper(batch_size=10)  # Process 10 games per batch

if __name__ == "__main__":
    main()