#!/usr/bin/env python3
"""
Simple test script to verify ESPN NFL schedule access
"""

import requests
from bs4 import BeautifulSoup
import re

def test_espn_schedule():
    # Test current ESPN schedule page
    base_url = "https://www.espn.com/nfl/schedule"
    
    # Add proper headers to avoid blocking
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    print("🏈 Testing ESPN NFL Schedule Access")
    print("=" * 50)
    
    try:
        # Test base schedule page
        print(f"📡 Fetching: {base_url}")
        response = requests.get(base_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for any game-related content
        games_found = []
        
        # Method 1: Look for game links
        game_links = soup.find_all('a', href=re.compile(r'/nfl/game/'))
        print(f"🔗 Found {len(game_links)} game links")
        
        for i, link in enumerate(game_links[:5]):  # Show first 5
            href = link.get('href', '')
            text = link.get_text().strip()
            print(f"   {i+1}. {text} -> {href}")
        
        # Method 2: Look for team links
        team_links = soup.find_all('a', href=re.compile(r'/nfl/team/'))
        print(f"🏈 Found {len(team_links)} team links")
        
        # Method 3: Look for score containers
        score_elements = soup.find_all(['div', 'span'], class_=re.compile(r'score|game'))
        print(f"📊 Found {len(score_elements)} potential score elements")
        
        # Method 4: Look for table rows
        table_rows = soup.find_all('tr')
        print(f"📋 Found {len(table_rows)} table rows")
        
        # Test specific week URL
        week_url = "https://www.espn.com/nfl/schedule/_/week/3/year/2025/seasontype/1"
        print(f"\n📡 Testing specific week URL: {week_url}")
        
        week_response = requests.get(week_url, headers=headers, timeout=10)
        week_response.raise_for_status()
        
        week_soup = BeautifulSoup(week_response.text, 'html.parser')
        week_game_links = week_soup.find_all('a', href=re.compile(r'/nfl/game/'))
        print(f"🔗 Week 3 game links found: {len(week_game_links)}")
        
        for i, link in enumerate(week_game_links[:3]):  # Show first 3
            href = link.get('href', '')
            text = link.get_text().strip()
            print(f"   {i+1}. {text} -> {href}")
        
        # Check page title and main content
        title = week_soup.find('title')
        if title:
            print(f"📄 Page title: {title.get_text()}")
        
        # Look for any text mentioning games or scores
        page_text = week_soup.get_text()
        if 'vs' in page_text.lower() or '@' in page_text:
            print("✅ Page contains game matchup indicators (vs or @)")
        else:
            print("❌ No obvious game matchup indicators found")
            
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_espn_schedule()