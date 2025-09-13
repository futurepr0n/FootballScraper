#!/usr/bin/env python3
"""
Structured Card Parser - Extract ESPN play cards in exact 3-4 line format
Based on user's clear specification:
Line 1: Play type (Kickoff, 3-yd Run, 13-yd Pass)
Line 2: Time and quarter (15:00 - 1st, 14:54 - 1st) 
Line 3: Play details (players, yardage, tackles)
Line 4: Starting position (1st & 10 at SF 39) - optional
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
        time.sleep(5)  # Wait for content to load
    
    return expanded_count

def extract_play_cards_by_pattern(driver):
    """Extract play cards by looking for the exact 3-4 line pattern the user identified"""
    print("🎯 Extracting structured play cards by pattern...")
    
    # Get all text content and split into potential card blocks
    body_text = driver.find_element(By.TAG_NAME, "body").text
    
    # Split by multiple newlines to get potential card blocks
    blocks = re.split(r'\n\s*\n', body_text)
    
    plays = []
    play_number = 0
    
    for block in blocks:
        lines = [line.strip() for line in block.split('\n') if line.strip()]
        
        # Look for 3-4 line play card pattern
        if len(lines) >= 3:
            play_data = parse_card_pattern(lines, play_number)
            if play_data:
                plays.append(play_data)
                play_number += 1
    
    print(f"✅ Extracted {len(plays)} structured play cards")
    return plays

def parse_card_pattern(lines, play_number):
    """Parse lines looking for exact pattern:
    Line 1: Play type (Kickoff, 3-yd Run, 13-yd Pass)
    Line 2: Time and quarter (15:00 - 1st)
    Line 3: Play details
    Line 4: Starting position (optional)
    """
    
    if len(lines) < 3:
        return None
    
    line1 = lines[0].strip()
    line2 = lines[1].strip()
    line3 = lines[2].strip()
    line4 = lines[3].strip() if len(lines) > 3 else ""
    
    # Check if line 1 matches play type pattern
    play_type_patterns = [
        r'^Kickoff$',
        r'^(\d+)-yd\s+(Run|Pass|Punt|Return)$',
        r'^Field Goal$',
        r'^Extra Point$', 
        r'^Safety$',
        r'^Punt$',
        r'^Sack$',
        r'^Incomplete Pass$',
        r'^Fumble$'
    ]
    
    play_type_match = False
    for pattern in play_type_patterns:
        if re.match(pattern, line1, re.IGNORECASE):
            play_type_match = True
            break
    
    # Check if line 2 matches time/quarter pattern
    time_quarter_match = re.match(r'^(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)$', line2, re.IGNORECASE)
    
    if not play_type_match or not time_quarter_match:
        return None
    
    # We have a valid play card! Extract the data
    play_data = {
        'play_number': play_number,
        'play_description': line3[:500],  # Full play details
        'quarter': None,
        'time_remaining': time_quarter_match.group(1),
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
    
    # Parse quarter
    quarter_text = time_quarter_match.group(2).lower()
    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
    play_data['quarter'] = quarter_map.get(quarter_text)
    
    # Parse play type and yards from line 1
    if re.match(r'^Kickoff$', line1, re.IGNORECASE):
        play_data['play_type'] = 'kickoff'
    elif re.match(r'^(\d+)-yd\s+Run$', line1, re.IGNORECASE):
        match = re.match(r'^(\d+)-yd\s+Run$', line1, re.IGNORECASE)
        play_data['play_type'] = 'rush'
        play_data['yards_gained'] = int(match.group(1))
    elif re.match(r'^(\d+)-yd\s+Pass$', line1, re.IGNORECASE):
        match = re.match(r'^(\d+)-yd\s+Pass$', line1, re.IGNORECASE)
        play_data['play_type'] = 'pass'
        play_data['yards_gained'] = int(match.group(1))
    elif re.match(r'^Field Goal$', line1, re.IGNORECASE):
        play_data['play_type'] = 'field_goal'
    elif re.match(r'^Extra Point$', line1, re.IGNORECASE):
        play_data['play_type'] = 'extra_point'
    elif re.match(r'^Punt$', line1, re.IGNORECASE):
        play_data['play_type'] = 'punt'
    elif re.match(r'^Sack$', line1, re.IGNORECASE):
        play_data['play_type'] = 'sack'
    
    # Parse down/distance from line 4 if present
    if line4:
        down_match = re.match(r'^(1st|2nd|3rd|4th)\s*&\s*(\d+)\s*at\s*([A-Z]{2,3})\s*(\d+)', line4)
        if down_match:
            down_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4}
            play_data['down'] = down_map.get(down_match.group(1).lower())
            play_data['distance'] = int(down_match.group(2))
            play_data['yard_line'] = int(down_match.group(4))
    
    # Extract player names from line 3
    player_names = re.findall(r'([A-Z]\.[A-Z][a-z]+)', line3)
    play_data['player_names'] = list(dict.fromkeys(player_names))[:3]
    
    # Check for special events in line 3
    line3_upper = line3.upper()
    if 'TOUCHDOWN' in line3_upper or ' TD ' in line3_upper:
        play_data['touchdown'] = True
    if 'PENALTY' in line3_upper:
        play_data['penalty'] = True
    if 'FUMBLE' in line3_upper or 'INTERCEPTION' in line3_upper:
        play_data['turnover'] = True
    
    return play_data

def test_structured_parser():
    """Test the structured card parser on a single game"""
    print("🧪 TESTING STRUCTURED CARD PARSER")
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
            plays = extract_play_cards_by_pattern(driver)
            
            print(f"\n📊 RESULTS:")
            print(f"   Total structured plays: {len(plays)}")
            
            # Show first 5 plays with complete structure
            structured_count = 0
            for play in plays[:10]:
                if play['quarter'] and play['time_remaining']:
                    structured_count += 1
                    print(f"\n✅ PLAY #{play['play_number']}:")
                    print(f"   🕐 Time: {play['time_remaining']} - Q{play['quarter']}")
                    print(f"   🏈 Type: {play['play_type']}, Yards: {play['yards_gained']}")
                    if play['down']:
                        print(f"   📍 Down: {play['down']} & {play['distance']} at {play['yard_line']}")
                    print(f"   👥 Players: {play['player_names']}")
                    print(f"   📝 Details: {play['play_description'][:80]}...")
                    
                    if structured_count >= 5:  # Limit output
                        break
            
            complete_plays = sum(1 for p in plays if p['quarter'] and p['time_remaining'] and p['play_type'])
            print(f"\n🎯 SUCCESS RATE: {complete_plays}/{len(plays)} plays fully structured")
            
            if complete_plays > 0:
                print("✅ SUCCESS: Found structured play cards matching user's pattern!")
            else:
                print("❌ ISSUE: No plays match the expected 3-4 line card pattern")
                
        else:
            print("❌ No accordions expanded")
            
    finally:
        driver.quit()
        print("🔒 Browser closed")

if __name__ == "__main__":
    test_structured_parser()