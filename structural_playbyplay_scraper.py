#!/usr/bin/env python3
"""
Structural Play-by-Play Scraper
Focuses on stable HTML structure patterns rather than dynamic CSS classes
Uses curl + BeautifulSoup approach to identify accordion patterns
"""

import subprocess
import json
import time
import os
from bs4 import BeautifulSoup
import re
import csv
from pathlib import Path

def test_structural_approach():
    """Test structural HTML parsing approach"""
    
    print("🏈 Structural Play-by-Play Scraper Test")
    print("🎯 Testing stable HTML structure detection")
    
    test_game_id = "401547444"  # Your example game
    test_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{test_game_id}"
    
    print(f"📡 Fetching ESPN page: {test_url}")
    
    # Fetch page with curl (no gzip to avoid encoding issues)
    headers = [
        '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        '-H', 'Accept-Language: en-US,en;q=0.5',
        '-H', 'Connection: keep-alive'
    ]
    
    cmd = ['curl', '-s', '--max-time', '15'] + headers + [test_url]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    
    if result.returncode != 0:
        print(f"❌ Failed to fetch page: curl returned {result.returncode}")
        return False
    
    html_content = result.stdout
    print(f"✅ Retrieved {len(html_content)} characters of HTML")
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Look for structural accordion patterns (stable HTML attributes)
    print("\n🔍 Analyzing HTML structure for accordion patterns...")
    
    # Find buttons with aria-expanded (standard accordion pattern)
    aria_buttons = soup.find_all('button', {'aria-expanded': True})
    print(f"   Found {len(aria_buttons)} buttons with aria-expanded attribute")
    
    # Find buttons that control other elements (accordion pattern)
    control_buttons = soup.find_all('button', {'aria-controls': True})
    print(f"   Found {len(control_buttons)} buttons with aria-controls attribute")
    
    # Look for collapsed accordions specifically
    collapsed_buttons = soup.find_all('button', {'aria-expanded': 'false'})
    print(f"   Found {len(collapsed_buttons)} collapsed accordion buttons")
    
    # Look for data-testid patterns (more stable than CSS classes)
    testid_elements = soup.find_all(attrs={'data-testid': True})
    accordion_testids = [elem for elem in testid_elements if 'accordion' in elem.get('data-testid', '').lower()]
    print(f"   Found {len(accordion_testids)} elements with accordion data-testid")
    
    # Look for role="button" elements
    role_buttons = soup.find_all(attrs={'role': 'button'})
    print(f"   Found {len(role_buttons)} elements with role=button")
    
    # Search for play-by-play content indicators
    print("\n🏈 Searching for play-by-play content...")
    
    # Look for common play patterns
    play_indicators = [
        "1st down", "2nd down", "3rd down", "4th down",
        "touchdown", "field goal", "punt", "kickoff",
        "interception", "fumble", "sack", "penalty"
    ]
    
    play_content_found = 0
    for indicator in play_indicators:
        if indicator.lower() in html_content.lower():
            play_content_found += 1
    
    print(f"   Found {play_content_found}/{len(play_indicators)} play indicators in HTML")
    
    # Look for JSON data in script tags (ESPN often embeds data)
    print("\n📊 Searching for embedded JSON data...")
    
    script_tags = soup.find_all('script')
    json_data_found = 0
    
    for script in script_tags:
        script_text = script.get_text()
        if 'play' in script_text.lower() and ('drive' in script_text.lower() or 'down' in script_text.lower()):
            json_data_found += 1
            # Try to find JSON objects
            if 'window.__espnfitt' in script_text or 'window.espn' in script_text:
                print(f"   ✅ Found potential play data in script tag")
                
                # Try to extract JSON
                try:
                    # Look for JSON patterns
                    json_pattern = r'\{.*"plays".*\}'
                    matches = re.findall(json_pattern, script_text, re.DOTALL)
                    if matches:
                        print(f"   Found {len(matches)} JSON objects with 'plays' data")
                except:
                    pass
    
    print(f"   Found {json_data_found} script tags with potential play data")
    
    # Summary
    print(f"\n📋 STRUCTURAL ANALYSIS SUMMARY:")
    print(f"   • Accordion buttons (aria-expanded): {len(aria_buttons)}")
    print(f"   • Collapsed accordions: {len(collapsed_buttons)}")  
    print(f"   • Control buttons: {len(control_buttons)}")
    print(f"   • Role buttons: {len(role_buttons)}")
    print(f"   • Play indicators found: {play_content_found}/{len(play_indicators)}")
    print(f"   • Scripts with play data: {json_data_found}")
    
    # Show samples of what we found
    if collapsed_buttons:
        print(f"\n🔍 Sample collapsed accordion button:")
        button = collapsed_buttons[0]
        print(f"   Tag: {button.name}")
        print(f"   Attributes: {dict(button.attrs)}")
        print(f"   Text: {button.get_text(strip=True)[:50]}...")
    
    success = (len(collapsed_buttons) > 0 or json_data_found > 0 or play_content_found > 5)
    
    return success

if __name__ == "__main__":
    success = test_structural_approach()
    
    if success:
        print("\n🎉 STRUCTURAL ANALYSIS SUCCESSFUL!")
        print("✅ Found accordion patterns and/or play data")
        print("✅ Ready to build proper scraper using stable HTML structure")
    else:
        print("\n❌ STRUCTURAL ANALYSIS INCONCLUSIVE") 
        print("❌ May need different approach or ESPN has changed structure")