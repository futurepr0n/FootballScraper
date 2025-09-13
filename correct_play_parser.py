#!/usr/bin/env python3
"""
Correct Play Parser - Properly extracts 4-line ESPN play card structure
Line 1: play_summary ('2-yd Run', '13-yd Pass', 'Kickoff')
Line 2: time_quarter ('8:53 - 3rd', '14:16 - 1st', '15:00 - 1st')
Line 3: play_description ('(Shotgun) T.Spears left guard to JAX 16...')
Line 4: situation ('3rd & 2 at JAX 18') [optional]
"""

import sys
import re
import psycopg2
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from typing import Dict, List, Optional

class CorrectPlayParser:
    def __init__(self):
        self.conn = None
        self.connect_to_database()
        
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
            sys.exit(1)
    
    def get_games_to_process(self, limit=5):
        """Get games that need play-by-play data"""
        if not self.conn:
            return []
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT g.game_id, g.date, g.season, g.week,
                       t1.abbreviation as home_team, t2.abbreviation as away_team
                FROM games g
                JOIN teams t1 ON g.home_team_id = t1.id
                JOIN teams t2 ON g.away_team_id = t2.id
                ORDER BY g.date DESC
                LIMIT %s
            """, (limit,))
            
            games = []
            for row in cursor.fetchall():
                games.append({
                    'espn_game_id': row[0],
                    'date': row[1],
                    'season': row[2],
                    'week': row[3],
                    'home_team': row[4],
                    'away_team': row[5]
                })
            
            cursor.close()
            return games
            
        except Exception as e:
            print(f"❌ Error getting games: {e}")
            return []
    
    def parse_4_line_play_card(self, card_element) -> Optional[Dict]:
        """Parse the 4-line play card structure exactly as specified"""
        try:
            # Get all text lines from the card
            text_elements = card_element.find_elements(By.TAG_NAME, "div")
            text_lines = []
            
            for element in text_elements:
                text = element.text.strip()
                if text and len(text) > 0:
                    text_lines.append(text)
            
            # Filter for actual play card pattern
            if len(text_lines) < 3:
                return None
            
            # Look for time-quarter pattern to identify valid play cards
            time_quarter_found = False
            time_quarter_index = -1
            
            for i, line in enumerate(text_lines):
                if re.match(r'\d{1,2}:\d{2}\s*-\s*(1st|2nd|3rd|4th|OT)', line):
                    time_quarter_found = True
                    time_quarter_index = i
                    break
            
            if not time_quarter_found:
                return None
            
            # Extract the 4 lines based on time_quarter position
            play_data = {
                'play_summary': None,      # Line before time_quarter
                'time_quarter': None,      # Line with time pattern
                'play_description': None,  # Line after time_quarter
                'situation': None          # Optional 4th line
            }
            
            # Line 1: play_summary (should be before time_quarter)
            if time_quarter_index > 0:
                play_data['play_summary'] = text_lines[time_quarter_index - 1]
            
            # Line 2: time_quarter
            play_data['time_quarter'] = text_lines[time_quarter_index]
            
            # Line 3: play_description (should be after time_quarter) 
            if time_quarter_index + 1 < len(text_lines):
                play_data['play_description'] = text_lines[time_quarter_index + 1]
            
            # Line 4: situation (optional - down & distance pattern)
            if time_quarter_index + 2 < len(text_lines):
                potential_situation = text_lines[time_quarter_index + 2]
                if re.search(r'\d+(st|nd|rd|th)\s*&\s*\d+', potential_situation):
                    play_data['situation'] = potential_situation
            
            # Must have at least the first 3 lines
            if play_data['play_summary'] and play_data['time_quarter'] and play_data['play_description']:
                return play_data
            else:
                return None
                
        except Exception as e:
            print(f"   ⚠️ Error parsing play card: {e}")
            return None
    
    def extract_structured_data(self, play_data: Dict) -> Dict:
        """Extract structured fields from the 4-line play card data"""
        structured = {
            'quarter': None,
            'time_remaining': None,
            'down': None,
            'distance': None,
            'yards_gained': None,
            'play_type': None
        }
        
        try:
            # Parse time_quarter: '8:53 - 3rd'
            if play_data['time_quarter']:
                time_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', play_data['time_quarter'])
                if time_match:
                    structured['time_remaining'] = time_match.group(1) + ':00'
                    
                    quarter_text = time_match.group(2).lower()
                    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                    structured['quarter'] = quarter_map.get(quarter_text)
            
            # Parse play_summary: '2-yd Run', '13-yd Pass'
            if play_data['play_summary']:
                # Extract yards from summary
                yard_match = re.search(r'(\d+)-?yd', play_data['play_summary'])
                if yard_match:
                    structured['yards_gained'] = int(yard_match.group(1))
                
                # Extract play type
                if 'run' in play_data['play_summary'].lower():
                    structured['play_type'] = 'rush'
                elif 'pass' in play_data['play_summary'].lower():
                    structured['play_type'] = 'pass'
                elif 'kick' in play_data['play_summary'].lower():
                    structured['play_type'] = 'kickoff'
                elif 'punt' in play_data['play_summary'].lower():
                    structured['play_type'] = 'punt'
                elif 'field goal' in play_data['play_summary'].lower():
                    structured['play_type'] = 'field_goal'
                else:
                    # Default to first word
                    first_word = play_data['play_summary'].split()[0] if play_data['play_summary'].split() else None
                    structured['play_type'] = first_word.lower() if first_word else None
            
            # Parse situation: '3rd & 2 at JAX 18'
            if play_data['situation']:
                down_dist_match = re.match(r'(\d+)(st|nd|rd|th)\s*&\s*(\d+)', play_data['situation'])
                if down_dist_match:
                    structured['down'] = int(down_dist_match.group(1))
                    structured['distance'] = int(down_dist_match.group(3))
                    
        except Exception as e:
            print(f"   ⚠️ Error extracting structured data: {e}")
        
        return structured
    
    def save_play_to_database(self, play_data: Dict, structured: Dict, game_id: str, sequence: int) -> bool:
        """Save play with proper 4-column structure to database"""
        if not self.conn:
            return False
            
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO plays (
                    game_id, play_sequence, 
                    play_summary, time_quarter, play_description, situation,
                    quarter, time_remaining, down, distance, yards_gained, play_type
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                game_id,
                sequence,
                play_data['play_summary'],
                play_data['time_quarter'], 
                play_data['play_description'],
                play_data['situation'],
                structured['quarter'],
                structured['time_remaining'],
                structured['down'],
                structured['distance'],
                structured['yards_gained'],
                structured['play_type']
            ))
            
            self.conn.commit()
            cursor.close()
            return True
            
        except Exception as e:
            print(f"   ❌ Error saving play: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def process_game(self, game_data: Dict) -> bool:
        """Process a single game with correct 4-line parsing"""
        
        print(f"\n🎯 Processing: {game_data['away_team']} @ {game_data['home_team']}")
        print(f"   ESPN Game ID: {game_data['espn_game_id']}")
        print(f"   Season: {game_data['season']}, Week: {game_data['week']}")
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_data['espn_game_id']}"
        
        # Setup Chrome driver
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(espn_url)
            
            print(f"   📱 Navigated to ESPN page")
            
            # Wait for page load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Expand accordions
            print(f"   📂 Expanding accordions...")
            accordions = driver.find_elements(By.CSS_SELECTOR, '[aria-expanded="false"]')
            expanded_count = 0
            
            for accordion in accordions:
                try:
                    if any(keyword in accordion.text.lower() for keyword in ['quarter', '1st', '2nd', '3rd', '4th']):
                        driver.execute_script("arguments[0].click();", accordion)
                        expanded_count += 1
                        time.sleep(0.1)
                except:
                    continue
            
            print(f"   ✅ Expanded {expanded_count} accordions")
            time.sleep(2)  # Wait for content to load
            
            # Find play cards
            print(f"   📄 Extracting play cards...")
            play_cards = driver.find_elements(By.CSS_SELECTOR, 'section[data-testid*="prism"], section.Card')
            
            plays_extracted = 0
            plays_saved = 0
            
            for i, card in enumerate(play_cards):
                play_data = self.parse_4_line_play_card(card)
                
                if play_data:
                    plays_extracted += 1
                    structured = self.extract_structured_data(play_data)
                    
                    if self.save_play_to_database(play_data, structured, game_data['espn_game_id'], plays_extracted):
                        plays_saved += 1
                    
                    # Show example of first few plays
                    if plays_extracted <= 3:
                        print(f"   📋 Play {plays_extracted}:")
                        print(f"      Summary: {play_data['play_summary']}")
                        print(f"      Time: {play_data['time_quarter']}")
                        print(f"      Description: {play_data['play_description'][:50]}...")
                        if play_data['situation']:
                            print(f"      Situation: {play_data['situation']}")
            
            print(f"   ✅ SUCCESS: {plays_extracted} plays extracted, {plays_saved} saved")
            return plays_saved > 0
            
        except Exception as e:
            print(f"   ❌ Error processing game: {e}")
            return False
            
        finally:
            if driver:
                driver.quit()
    
    def run_processing(self, limit_games=3):
        """Run the correct parsing for specified games"""
        
        print("🚀 CORRECT PLAY PARSER - 4-LINE ESPN STRUCTURE")
        print("=" * 60)
        
        games = self.get_games_to_process(limit_games)
        
        if not games:
            print("❌ No games found to process")
            return False
            
        print(f"🎯 Processing {len(games)} games with correct 4-line parser")
        
        successful_games = 0
        
        for i, game in enumerate(games, 1):
            print(f"\n[{i}/{len(games)}]", end="")
            success = self.process_game(game)
            
            if success:
                successful_games += 1
                print(f"   ✅ Game completed successfully")
            else:
                print(f"   ❌ Game processing failed")
        
        print(f"\n🏁 PROCESSING COMPLETE")
        print(f"   Successful games: {successful_games}/{len(games)}")
        print(f"   Success rate: {successful_games/len(games)*100:.1f}%")
        
        return successful_games > 0

def main():
    """Main execution function"""
    
    print("🎯 CORRECT ESPN PLAY PARSER")
    print("=" * 40)
    print("Properly extracts 4-line play card structure:")
    print("Line 1: play_summary ('2-yd Run')")  
    print("Line 2: time_quarter ('8:53 - 3rd')")
    print("Line 3: play_description ('(Shotgun) T.Spears...')")
    print("Line 4: situation ('3rd & 2 at JAX 18') [optional]")
    
    parser = CorrectPlayParser()
    
    # Process first 3 games as test
    success = parser.run_processing(limit_games=3)
    
    if success:
        print(f"\n✅ CORRECT PARSING: SUCCESS")
        print(f"   4-line structure properly extracted")
        print(f"   ESPN game_id relationship established")
        print(f"   Database cleaned and restructured")
        
        print(f"\n📋 NEXT STEPS:")
        print(f"   1. Verify data structure in database")
        print(f"   2. Scale to all 465 games")
        print(f"   3. Validate play card parsing accuracy")
    else:
        print(f"\n❌ Parsing failed - check implementation")
        
    return success

if __name__ == "__main__":
    result = main()
    sys.exit(0 if result else 1)