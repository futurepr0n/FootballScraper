#!/usr/bin/env python3
"""
Targeted Card Scraper - Extract structured play data exactly like the user's image shows
Focus on ESPN's play card format with time/quarter, play type headers, and down/distance
"""

import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
import csv

def setup_driver():
    """Setup ChromeWebDriver optimized for ESPN card extraction"""
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox') 
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        return driver
    except Exception as e:
        print(f"❌ Driver setup failed: {e}")
        return None

def expand_accordions(driver):
    """Expand all play-by-play accordions"""
    print("🔍 Expanding play-by-play accordions...")
    
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(3)
    
    expanded_count = 0
    accordion_selectors = [
        "button[aria-expanded='false']",
        "button[aria-controls*='details']", 
        "[data-testid*='Accordion'] button"
    ]
    
    for selector in accordion_selectors:
        buttons = driver.find_elements(By.CSS_SELECTOR, selector)
        for button in buttons:
            try:
                if button.get_attribute('aria-expanded') == 'false':
                    driver.execute_script("arguments[0].click();", button)
                    expanded_count += 1
                    time.sleep(0.3)
            except:
                continue
    
    if expanded_count > 0:
        print(f"✅ Expanded {expanded_count} accordions")
        time.sleep(5)  # Wait for content to load
    
    return expanded_count

def extract_play_cards(driver):
    """Extract structured data from ESPN play cards matching the user's image format"""
    print("🎯 Extracting structured play card data...")
    
    plays = []
    
    # Target the play card containers - these contain the structured format
    card_selectors = [
        # ESPN uses these for individual play cards
        "[data-testid*='play']",
        ".PlayByPlay__Play",
        ".play-item",
        "article",
        "section[class*='Card']",
        # Generic containers that might hold play data
        "div[class*='play']",
        "li[class*='play']"
    ]
    
    all_cards = []
    for selector in card_selectors:
        cards = driver.find_elements(By.CSS_SELECTOR, selector)
        if cards:
            print(f"📋 Found {len(cards)} elements with selector: {selector}")
            all_cards.extend(cards)
    
    # Remove duplicates by getting unique elements
    unique_cards = []
    seen_texts = set()
    
    for card in all_cards:
        try:
            card_text = card.text.strip()
            if card_text and len(card_text) > 20 and card_text not in seen_texts:
                seen_texts.add(card_text)
                unique_cards.append(card)
        except:
            continue
    
    print(f"🎯 Processing {len(unique_cards)} unique play cards...")
    
    for card_idx, card in enumerate(unique_cards):
        try:
            card_text = card.text.strip()
            if not card_text or len(card_text) < 10:
                continue
            
            play_data = parse_play_card(card_text, card_idx)
            if play_data:
                plays.append(play_data)
                
        except Exception as e:
            print(f"⚠️  Error processing card {card_idx}: {e}")
            continue
    
    print(f"✅ Successfully extracted {len(plays)} structured plays")
    return plays

def parse_play_card(card_text, play_index):
    """Parse individual play card text to extract structured data like in the user's image"""
    try:
        lines = [line.strip() for line in card_text.split('\n') if line.strip()]
        if len(lines) < 2:
            return None
            
        play_data = {
            'play_number': play_index,
            'play_description': card_text[:500],
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
        
        # Look for play type header (like "Kickoff", "3-yd Run", "13-yd Pass")
        header_line = lines[0]
        
        # Extract play type and yards from header
        play_type_patterns = [
            (r'(\d+)-?yd\s+(Run|Pass|Punt|Return)', 'run_pass'),
            (r'Kickoff', 'kickoff'),
            (r'Field Goal', 'field_goal'),
            (r'Extra Point', 'extra_point'),
            (r'Safety', 'safety'),
            (r'Punt', 'punt'),
            (r'Sack', 'sack')
        ]
        
        for pattern, ptype in play_type_patterns:
            match = re.search(pattern, header_line, re.IGNORECASE)
            if match:
                if ptype == 'run_pass':
                    play_data['yards_gained'] = int(match.group(1))
                    play_type = match.group(2).lower()
                    if 'run' in play_type:
                        play_data['play_type'] = 'rush'
                    elif 'pass' in play_type:
                        play_data['play_type'] = 'pass'
                else:
                    play_data['play_type'] = ptype
                break
        
        # Look for time/quarter line (like "15:00 - 1st", "14:54 - 1st") 
        for line in lines[1:3]:  # Usually in first few lines
            time_match = re.search(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', line)
            if time_match:
                play_data['time_remaining'] = time_match.group(1)
                quarter_text = time_match.group(2)
                if quarter_text == '1st':
                    play_data['quarter'] = 1
                elif quarter_text == '2nd':
                    play_data['quarter'] = 2
                elif quarter_text == '3rd':
                    play_data['quarter'] = 3
                elif quarter_text == '4th':
                    play_data['quarter'] = 4
                elif quarter_text == 'OT':
                    play_data['quarter'] = 5
                break
        
        # Look for down/distance (like "1st & 10 at SF 39", "2nd & 7 at SF 42")
        for line in lines:
            down_match = re.search(r'(1st|2nd|3rd|4th)\s*&\s*(\d+)\s*at\s*([A-Z]{2,3})\s*(\d+)', line)
            if down_match:
                down_text = down_match.group(1)
                if down_text == '1st':
                    play_data['down'] = 1
                elif down_text == '2nd':
                    play_data['down'] = 2
                elif down_text == '3rd':
                    play_data['down'] = 3
                elif down_text == '4th':
                    play_data['down'] = 4
                    
                play_data['distance'] = int(down_match.group(2))
                play_data['yard_line'] = int(down_match.group(4))
                break
        
        # Extract player names
        player_names = []
        for line in lines:
            # Look for player name patterns
            names = re.findall(r'([A-Z]\.[A-Z][a-z]+)', line)
            player_names.extend(names)
        
        play_data['player_names'] = list(dict.fromkeys(player_names))[:3]  # Remove dupes, limit to 3
        
        # Check for special events
        full_text = ' '.join(lines).upper()
        if 'TOUCHDOWN' in full_text or ' TD ' in full_text:
            play_data['touchdown'] = True
        if 'PENALTY' in full_text:
            play_data['penalty'] = True
        if 'FUMBLE' in full_text or 'INTERCEPTION' in full_text:
            play_data['turnover'] = True
            
        return play_data
        
    except Exception as e:
        print(f"⚠️  Parse error: {e}")
        return None

def test_single_game():
    """Test the targeted card scraper on a single game"""
    print("🧪 TESTING TARGETED CARD SCRAPER")
    print("=" * 50)
    
    # Use the same game from user's context
    test_url = "https://www.espn.com/nfl/playbyplay/_/gameId/401671698"
    print(f"🎯 Testing: {test_url}")
    
    driver = setup_driver()
    if not driver:
        return
        
    try:
        driver.get(test_url)
        expanded = expand_accordions(driver)
        
        if expanded > 0:
            plays = extract_play_cards(driver)
            
            print(f"\n📊 RESULTS:")
            print(f"   Total plays extracted: {len(plays)}")
            
            # Show sample plays with structure
            structured_count = 0
            for play in plays[:10]:  # Show first 10
                if play['quarter'] or play['time_remaining'] or play['down']:
                    structured_count += 1
                    print(f"\n✅ STRUCTURED PLAY #{play['play_number']}:")
                    print(f"   Time: {play['time_remaining']} - Q{play['quarter']}")
                    print(f"   Down: {play['down']} & {play['distance']} at {play['yard_line']}")
                    print(f"   Type: {play['play_type']}, Yards: {play['yards_gained']}")
                    print(f"   Players: {play['player_names']}")
                    print(f"   Description: {play['play_description'][:100]}...")
            
            print(f"\n📈 SUCCESS RATE: {structured_count}/{len(plays)} plays have structured data")
            
        else:
            print("❌ No accordions expanded - page structure issue")
            
    finally:
        driver.quit()
        print("🔒 Browser closed")

if __name__ == "__main__":
    test_single_game()