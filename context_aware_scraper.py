#!/usr/bin/env python3
"""
Context-Aware Scraper - Since we can extract play_description, examine the DOM context
around those elements to find the time/quarter and play type headers in nearby elements
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

def find_play_elements_with_context(driver):
    """Find elements that contain play descriptions, then examine their DOM context"""
    print("🎯 Finding play elements and examining their context...")
    
    # Use the same selectors that successfully extract play descriptions
    play_selectors = [
        "li.css-1s6s92s",
        "p.css-115032d", 
        ".accordion-body > ul.css-15l3a1e li",
        "section[data-testid='prism-LayoutCard']",
        "section",
        "div[role='listitem']",
        "article",
        "[class*='PlayByPlay']",
        "[class*='play-by-play']",
        "li",
        "div"
    ]
    
    found_elements = []
    
    for selector in play_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                # Look for elements that contain detailed play text (like what we extract successfully)
                if (len(text) > 50 and 
                    ('yards' in text.lower() or 'pass' in text.lower() or 'run' in text.lower() or 'kick' in text.lower()) and
                    any(name in text for name in ['J.', 'D.', 'M.', 'K.', 'T.', 'C.', 'A.', 'B.', 'R.', 'S.'])):
                    found_elements.append(element)
        except:
            continue
    
    print(f"📋 Found {len(found_elements)} elements with play description text")
    return found_elements

def analyze_element_context(driver, element, play_index):
    """Analyze the DOM context around an element that contains play description"""
    try:
        play_data = {
            'play_number': play_index,
            'play_description': element.text.strip()[:500],
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
            'debug_context': []
        }
        
        # Get the element text
        element_text = element.text.strip()
        
        # Look in the element itself for patterns
        play_data['debug_context'].append(f"SELF: {element_text[:100]}...")
        
        # Check parent element
        try:
            parent = element.find_element(By.XPATH, "..")
            parent_text = parent.text.strip()
            if parent_text and len(parent_text) < 500:  # Don't capture huge parent containers
                play_data['debug_context'].append(f"PARENT: {parent_text[:150]}...")
                
                # Look for time/quarter in parent
                time_match = re.search(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', parent_text)
                if time_match and not play_data['time_remaining']:
                    play_data['time_remaining'] = time_match.group(1)
                    quarter_text = time_match.group(2).lower()
                    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                    play_data['quarter'] = quarter_map.get(quarter_text)
        except:
            pass
        
        # Check previous siblings
        try:
            prev_siblings = driver.execute_script("""
                var siblings = [];
                var elem = arguments[0].previousSibling;
                var count = 0;
                while (elem && count < 3) {
                    if (elem.nodeType === 1 && elem.textContent.trim()) {
                        siblings.push(elem.textContent.trim());
                    }
                    elem = elem.previousSibling;
                    count++;
                }
                return siblings;
            """, element)
            
            for i, sibling_text in enumerate(prev_siblings):
                if sibling_text and len(sibling_text) < 200:
                    play_data['debug_context'].append(f"PREV_SIB_{i}: {sibling_text[:100]}...")
                    
                    # Look for play type headers in siblings
                    if re.match(r'^(Kickoff|Field Goal|Extra Point|Punt|Safety|\d+-yd\s+(Run|Pass))$', sibling_text.strip(), re.IGNORECASE):
                        header_match = re.match(r'^(\d+)-yd\s+(Run|Pass)$', sibling_text.strip(), re.IGNORECASE)
                        if header_match:
                            play_data['yards_gained'] = int(header_match.group(1))
                            play_type = header_match.group(2).lower()
                            play_data['play_type'] = 'rush' if 'run' in play_type else 'pass'
                        elif 'kickoff' in sibling_text.lower():
                            play_data['play_type'] = 'kickoff'
                        elif 'field goal' in sibling_text.lower():
                            play_data['play_type'] = 'field_goal'
                        elif 'punt' in sibling_text.lower():
                            play_data['play_type'] = 'punt'
                    
                    # Look for time/quarter in siblings
                    time_match = re.search(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', sibling_text)
                    if time_match and not play_data['time_remaining']:
                        play_data['time_remaining'] = time_match.group(1)
                        quarter_text = time_match.group(2).lower()
                        quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                        play_data['quarter'] = quarter_map.get(quarter_text)
        except:
            pass
        
        # Check next siblings  
        try:
            next_siblings = driver.execute_script("""
                var siblings = [];
                var elem = arguments[0].nextSibling;
                var count = 0;
                while (elem && count < 2) {
                    if (elem.nodeType === 1 && elem.textContent.trim()) {
                        siblings.push(elem.textContent.trim());
                    }
                    elem = elem.nextSibling;
                    count++;
                }
                return siblings;
            """, element)
            
            for i, sibling_text in enumerate(next_siblings):
                if sibling_text and len(sibling_text) < 200:
                    play_data['debug_context'].append(f"NEXT_SIB_{i}: {sibling_text[:100]}...")
                    
                    # Look for down/distance in next siblings
                    down_match = re.search(r'(1st|2nd|3rd|4th)\s*&\s*(\d+)\s*at\s*([A-Z]{2,3})\s*(\d+)', sibling_text)
                    if down_match:
                        down_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4}
                        play_data['down'] = down_map.get(down_match.group(1).lower())
                        play_data['distance'] = int(down_match.group(2))
                        play_data['yard_line'] = int(down_match.group(4))
        except:
            pass
            
        return play_data
        
    except Exception as e:
        print(f"⚠️  Context analysis error: {e}")
        return None

def test_context_scraper():
    """Test the context-aware scraper"""
    print("🧪 TESTING CONTEXT-AWARE SCRAPER")
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
            play_elements = find_play_elements_with_context(driver)
            
            if play_elements:
                plays = []
                for i, element in enumerate(play_elements[:10]):  # Test first 10
                    play_data = analyze_element_context(driver, element, i)
                    if play_data:
                        plays.append(play_data)
                
                print(f"\n📊 RESULTS:")
                print(f"   Analyzed {len(plays)} play elements")
                
                structured_count = 0
                for play in plays[:5]:  # Show first 5
                    has_structure = bool(play['quarter'] or play['time_remaining'] or play['play_type'])
                    if has_structure:
                        structured_count += 1
                        
                    print(f"\n{'✅' if has_structure else '❌'} PLAY #{play['play_number']}:")
                    print(f"   🕐 Time: {play['time_remaining']} - Q{play['quarter']}")
                    print(f"   🏈 Type: {play['play_type']}, Yards: {play['yards_gained']}")
                    if play['down']:
                        print(f"   📍 Down: {play['down']} & {play['distance']} at {play['yard_line']}")
                    print(f"   📝 Description: {play['play_description'][:80]}...")
                    print(f"   🔍 Context found:")
                    for context in play['debug_context'][:3]:  # Show first 3 context items
                        print(f"      - {context}")
                
                print(f"\n🎯 SUCCESS RATE: {structured_count}/{len(plays)} plays found structured data in context")
                
            else:
                print("❌ No play elements found")
                
        else:
            print("❌ No accordions expanded")
            
    finally:
        driver.quit()
        print("🔒 Browser closed")

if __name__ == "__main__":
    test_context_scraper()