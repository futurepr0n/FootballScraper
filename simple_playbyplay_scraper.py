#!/usr/bin/env python3
"""
Simple Play-by-Play Scraper
Based on proven accordion expansion approach
Processes games one at a time to avoid ChromeDriver issues
"""

import psycopg2
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re

def get_single_game_to_process():
    """Get one game that needs processing"""
    conn = psycopg2.connect(
        host='192.168.1.23',
        database='football_tracker', 
        user='postgres',
        password='korn5676'
    )
    cur = conn.cursor()
    
    # Get first game without plays
    cur.execute("""
        SELECT g.id as db_id, g.game_id as espn_game_id, 
               ht.abbreviation as home_team, at.abbreviation as away_team,
               g.season, g.week
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.id  
        JOIN teams at ON g.away_team_id = at.id
        WHERE NOT EXISTS (SELECT 1 FROM plays WHERE game_id = g.id)
        ORDER BY g.date DESC
        LIMIT 1
    """)
    
    game = cur.fetchone()
    cur.close()
    conn.close()
    return game

def setup_driver():
    """Setup basic ChromeDriver"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver

def expand_accordions(driver):
    """Expand accordions using proven selectors"""
    time.sleep(3)  # Wait for page load
    
    total_expanded = 0
    selectors = ["button[aria-expanded='false']"]
    
    for selector in selectors:
        buttons = driver.find_elements(By.CSS_SELECTOR, selector)
        print(f"   Found {len(buttons)} buttons with {selector}")
        
        for button in buttons:
            try:
                if button.get_attribute('aria-expanded') == 'false':
                    driver.execute_script("arguments[0].click();", button)
                    total_expanded += 1
                    time.sleep(0.1)
            except:
                continue
    
    print(f"   Expanded {total_expanded} accordions")
    time.sleep(2)  # Wait for content to load
    return total_expanded

def extract_plays(driver, db_id):
    """Extract plays using text pattern approach"""
    body_text = driver.find_element(By.TAG_NAME, "body").text
    lines = body_text.split('\n')
    
    plays = []
    play_patterns = [
        r'\d+(?:st|nd|rd|th)\s+(?:down|and)',
        r'touchdown', r'field goal', r'punt', r'kickoff', 
        r'interception', r'fumble', r'sack', r'penalty'
    ]
    
    play_index = 0
    for line in lines:
        line_clean = line.strip()
        if len(line_clean) > 20:
            for pattern in play_patterns:
                if re.search(pattern, line_clean, re.IGNORECASE):
                    plays.append((db_id, play_index, line_clean[:500]))
                    play_index += 1
                    break
    
    return plays

def save_plays(plays):
    """Save plays to database"""
    if not plays:
        return 0
        
    conn = psycopg2.connect(
        host='192.168.1.23',
        database='football_tracker',
        user='postgres', 
        password='korn5676'
    )
    cur = conn.cursor()
    
    insert_count = 0
    for play in plays:
        try:
            cur.execute("""
                INSERT INTO plays (game_id, play_number, play_description)
                VALUES (%s, %s, %s)
                ON CONFLICT (game_id, play_number) DO NOTHING
            """, play)
            if cur.rowcount > 0:
                insert_count += 1
        except Exception as e:
            print(f"      Error inserting play: {e}")
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    return insert_count

def main():
    """Process single game"""
    print("🏈 Simple Play-by-Play Scraper")
    
    # Get game to process
    game = get_single_game_to_process()
    if not game:
        print("✅ No games need processing!")
        return
    
    db_id, espn_game_id, home_team, away_team, season, week = game
    print(f"\n🎯 Processing: {away_team} @ {home_team} ({season} Week {week})")
    print(f"   DB ID: {db_id}, ESPN ID: {espn_game_id}")
    
    # Setup driver and process
    driver = setup_driver()
    try:
        url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{espn_game_id}"
        print(f"   📡 Loading: {url}")
        driver.get(url)
        
        # Expand accordions
        expanded = expand_accordions(driver)
        
        # Extract plays
        plays = extract_plays(driver, db_id)
        print(f"   📄 Extracted {len(plays)} plays")
        
        # Save to database
        if plays:
            saved = save_plays(plays)
            print(f"   ✅ Saved {saved} plays to database")
        else:
            print("   ❌ No plays found")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    finally:
        driver.quit()
        print("   🔒 Browser closed")

if __name__ == "__main__":
    main()