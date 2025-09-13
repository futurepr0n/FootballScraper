#!/usr/bin/env python3
"""
Fixed Card Scraper - Extract ESPN play cards using exact DOM structure from user example
Target: section[data-testid="prism-LayoutCard"] and parse individual child divs
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

def setup_driver():
    """Setup ChromeWebDriver"""
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox') 
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        
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
    buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='false']")
    
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
        time.sleep(5)
    
    return expanded_count

def extract_play_cards_fixed(driver):
    """Extract play cards using the exact DOM structure from user's example"""
    print("🎯 Extracting play cards using fixed DOM structure...")
    
    # Try multiple selectors to find the play cards
    selectors_to_try = [
        "section[data-testid='prism-LayoutCard']",  # Exact match from user's example
        "section[data-testid*='prism']",           # Partial match
        "section[class*='LayoutCard']",            # Class-based fallback
        "section",                                 # Broad fallback
    ]
    
    play_cards = []
    
    for selector in selectors_to_try:
        cards = driver.find_elements(By.CSS_SELECTOR, selector)
        print(f"📋 Found {len(cards)} elements with selector: {selector}")
        
        # Filter cards that look like play cards (contain the structure we expect)
        valid_cards = []
        for card in cards:
            card_text = card.text.strip()
            # Look for cards that contain time patterns and play descriptions
            if (len(card_text) > 20 and 
                re.search(r'\d{1,2}:\d{2}\s*-\s*(1st|2nd|3rd|4th)', card_text) and
                any(keyword in card_text.lower() for keyword in ['kick', 'pass', 'run', 'punt', 'yards'])):
                valid_cards.append(card)
        
        if valid_cards:
            print(f"✅ Found {len(valid_cards)} valid play cards with {selector}")
            play_cards = valid_cards
            break
    
    plays = []
    
    for card_idx, card in enumerate(play_cards[:20]):  # Limit for testing
        try:
            play_data = parse_card_structure(card, card_idx)
            if play_data:
                plays.append(play_data)
        except Exception as e:
            print(f"⚠️  Error parsing card {card_idx}: {e}")
            continue
    
    print(f"✅ Successfully parsed {len(plays)} play cards")
    return plays

def parse_card_structure(card, play_index):
    """Parse the exact card structure from user's HTML example"""
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
            'player_names': [],
            'raw_structure': []  # Debug info
        }
        
        # Get all div elements within the card
        all_divs = card.find_elements(By.TAG_NAME, "div")
        
        # Extract text from each div for analysis
        div_texts = []
        for div in all_divs:
            text = div.text.strip()
            if text and len(text) < 200:  # Skip huge container divs
                div_texts.append(text)
                play_data['raw_structure'].append(text)
        
        # Based on user's structure, look for patterns:
        # div_texts[0] should be play type like "Kickoff"
        # div_texts[1] should be time like "15:00 - 1st" 
        # div_texts[2] should be description like "J.Bates kicks..."
        
        if len(div_texts) >= 3:
            # Parse play type (first meaningful div)
            play_type_text = div_texts[0]
            if play_type_text:
                play_data['play_type'] = parse_play_type(play_type_text)
                
                # Extract yards from play type if present
                yards_match = re.search(r'(\d+)-yd', play_type_text)
                if yards_match:
                    play_data['yards_gained'] = int(yards_match.group(1))
            
            # Parse time/quarter (second meaningful div)
            time_quarter_text = div_texts[1]
            if time_quarter_text:
                time_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', time_quarter_text)
                if time_match:
                    play_data['time_remaining'] = time_match.group(1)
                    quarter_text = time_match.group(2).lower()
                    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                    play_data['quarter'] = quarter_map.get(quarter_text)
            
            # Parse description (third meaningful div - this is what we were getting before)
            description_text = div_texts[2]
            if description_text:
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
                    
                # Extract yards from description if not already found
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
            
            # Look for down/distance in any remaining divs
            for text in div_texts[3:]:
                down_match = re.search(r'(1st|2nd|3rd|4th)\s*&\s*(\d+)\s*at\s*([A-Z]{2,3})\s*(\d+)', text)
                if down_match:
                    down_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4}
                    play_data['down'] = down_map.get(down_match.group(1).lower())
                    play_data['distance'] = int(down_match.group(2))
                    play_data['yard_line'] = int(down_match.group(4))
                    break
        
        # Only return play if we got the essential structured data
        if play_data['play_type'] and play_data['quarter'] and play_data['time_remaining']:
            return play_data
        else:
            return None
            
    except Exception as e:
        print(f"⚠️  Parse error: {e}")
        return None

def parse_play_type(play_type_text):
    """Parse play type from the header text"""
    text = play_type_text.lower()
    
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
    else:
        return play_type_text.lower()  # Return as-is for debugging

def test_fixed_scraper():
    """Test the fixed card scraper"""
    print("🧪 TESTING FIXED CARD SCRAPER")
    print("=" * 50)
    
    test_url = "https://www.espn.com/nfl/playbyplay/_/gameId/401671698"
    print(f"🎯 Testing: {test_url}")
    
    driver = setup_driver()
    if not driver:
        return
        
    try:
        driver.get(test_url)
        expanded = expand_accordions(driver)
        
        if expanded > 0:
            plays = extract_play_cards_fixed(driver)
            
            print(f"\n📊 RESULTS:")
            print(f"   Total play cards found: {len(plays)}")
            
            structured_count = 0
            for play in plays[:10]:  # Show first 10
                has_complete_structure = (play['quarter'] and 
                                        play['time_remaining'] and 
                                        play['play_type'] and 
                                        play['play_description'])
                
                if has_complete_structure:
                    structured_count += 1
                    
                print(f"\n{'✅' if has_complete_structure else '❌'} PLAY #{play['play_number']}:")
                print(f"   🕐 Time: {play['time_remaining']} - Q{play['quarter']}")
                print(f"   🏈 Type: {play['play_type']}, Yards: {play['yards_gained']}")
                if play['down']:
                    print(f"   📍 Down: {play['down']} & {play['distance']} at {play['yard_line']}")
                print(f"   👥 Players: {play['player_names']}")
                print(f"   📝 Description: {play['play_description'][:80]}...")
                print(f"   🔍 Raw structure: {play['raw_structure']}")
            
            print(f"\n🎯 COMPLETE STRUCTURE RATE: {structured_count}/{len(plays)} plays have all required fields")
            
            if structured_count > 0:
                print("✅ SUCCESS: Fixed scraper is extracting complete structured play cards!")
            else:
                print("❌ ISSUE: Still not getting complete structure")
                
        else:
            print("❌ No accordions expanded")
            
    finally:
        driver.quit()
        print("🔒 Browser closed")

if __name__ == "__main__":
    test_fixed_scraper()