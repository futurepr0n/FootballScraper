#!/usr/bin/env python3
"""
Debug DOM Scraper
Inspect ESPN page structure after accordion expansion to identify correct selectors
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def setup_driver():
    """Setup ChromeWebDriver with debug options"""
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        return driver
        
    except Exception as e:
        print(f"❌ Failed to setup ChromeDriver: {e}")
        return None

def expand_accordions(driver):
    """Expand accordions and return count"""
    print("🔍 Expanding accordions...")
    
    # Wait for page to load
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)
    
    total_expanded = 0
    buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='false']")
    
    for button in buttons:
        try:
            if button.get_attribute('aria-expanded') == 'false':
                driver.execute_script("arguments[0].click();", button)
                total_expanded += 1
                time.sleep(0.3)
        except:
            continue
    
    if total_expanded > 0:
        print(f"✅ Expanded {total_expanded} accordions")
        time.sleep(5)  # Wait for content to load
    
    return total_expanded

def debug_dom_structure(driver):
    """Debug the DOM structure after accordion expansion"""
    print("\n🔍 DEBUGGING DOM STRUCTURE...")
    
    # Test all the selectors that should contain structured data
    selectors_to_test = [
        # From Gemini analysis
        "li.css-1s6s92s",
        "p.css-115032d", 
        ".accordion-body > ul.css-15l3a1e li",
        "h3.play-by-play-game-time",
        "div.down-distance",
        
        # Common ESPN patterns
        ".PlayByPlay__Row",
        "[data-testid*='play']",
        ".play-item",
        ".play-row",
        "section[class*='play']",
        
        # Generic play containers
        "[class*='PlayByPlay']",
        "[class*='play-by-play']",
        "[data-module*='play']",
        
        # Time and quarter elements
        "[class*='time']",
        "[class*='quarter']",
        "[class*='down']",
        "[class*='distance']",
    ]
    
    found_elements = {}
    
    for selector in selectors_to_test:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                found_elements[selector] = len(elements)
                print(f"✅ {selector}: {len(elements)} elements")
                
                # Sample first element text
                if len(elements) > 0:
                    sample_text = elements[0].text.strip()[:100]
                    if sample_text:
                        print(f"   📄 Sample: {sample_text}...")
            else:
                print(f"❌ {selector}: 0 elements")
        except Exception as e:
            print(f"⚠️  {selector}: Error - {e}")
    
    print(f"\n📊 Found {len(found_elements)} working selectors out of {len(selectors_to_test)} tested")
    
    # Try to find ANY elements that contain quarter/time info
    print("\n🔍 SEARCHING FOR TIME/QUARTER DATA...")
    body_text = driver.find_element(By.TAG_NAME, "body").text
    lines = body_text.split('\n')
    
    quarter_lines = []
    time_lines = []
    
    for line in lines:
        line = line.strip()
        if ('1st' in line or '2nd' in line or '3rd' in line or '4th' in line) and len(line) < 50:
            quarter_lines.append(line)
        if ':' in line and ('1st' in line or '2nd' in line or '3rd' in line or '4th' in line):
            time_lines.append(line)
    
    print(f"📍 Found {len(quarter_lines)} lines with quarter info:")
    for line in quarter_lines[:5]:  # Show first 5
        print(f"   {line}")
        
    print(f"⏰ Found {len(time_lines)} lines with time info:")  
    for line in time_lines[:5]:  # Show first 5
        print(f"   {line}")
    
    return found_elements

def main():
    """Debug ESPN DOM structure"""
    print("🐛 ESPN DOM STRUCTURE DEBUGGER")
    print("="*50)
    
    # Test on the same game we've been using
    test_url = "https://www.espn.com/nfl/playbyplay/_/gameId/401671698"
    print(f"🎯 Testing: {test_url}")
    
    driver = setup_driver()
    if not driver:
        return
        
    try:
        # Navigate and expand
        driver.get(test_url)
        expanded = expand_accordions(driver)
        
        if expanded > 0:
            # Debug DOM structure
            found_elements = debug_dom_structure(driver)
            
            if found_elements:
                print(f"\n✅ SUCCESS: Found {len(found_elements)} working selectors")
                print("💡 Use these selectors in the comprehensive scraper")
            else:
                print("\n❌ PROBLEM: No working selectors found")
                print("💡 ESPN page structure may have changed")
        else:
            print("❌ No accordions expanded - page structure issue")
            
    finally:
        driver.quit()
        print("🔒 Browser closed")

if __name__ == "__main__":
    main()