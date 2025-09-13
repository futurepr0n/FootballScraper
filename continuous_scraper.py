#!/usr/bin/env python3
"""
Continuous Production Play-by-Play Scraper
Processes ALL 465 games continuously until completion
No artificial batch limits - runs until done
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
from datetime import datetime

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host='192.168.1.23',
        database='football_tracker', 
        user='postgres',
        password='korn5676'
    )

def get_next_game_to_process():
    """Get the next game that needs processing"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT g.id as db_id, g.game_id as espn_game_id, 
               ht.abbreviation as home_team, at.abbreviation as away_team,
               g.season, g.week, g.date
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

def get_progress():
    """Get current progress stats"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM games")
    total_games = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT game_id) FROM plays")
    completed_games = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM plays")
    total_plays = cur.fetchone()[0]
    
    remaining = total_games - completed_games
    
    cur.close()
    conn.close()
    
    return {
        'total': total_games,
        'completed': completed_games,
        'remaining': remaining,
        'plays': total_plays,
        'percent': (completed_games / total_games * 100) if total_games > 0 else 0
    }

def setup_driver():
    """Setup ChromeDriver with minimal config"""
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
    """Expand accordions using proven approach"""
    time.sleep(3)
    total_expanded = 0
    
    buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='false']")
    
    for button in buttons:
        try:
            if button.get_attribute('aria-expanded') == 'false':
                driver.execute_script("arguments[0].click();", button)
                total_expanded += 1
                time.sleep(0.1)
        except:
            continue
    
    if total_expanded > 0:
        time.sleep(2)
    
    return total_expanded

def extract_plays(driver, db_id):
    """Extract plays using proven text pattern approach"""
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
        
    conn = get_db_connection()
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
            print(f"      ⚠️  Error inserting play: {e}")
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    return insert_count

def main():
    """Continuous processing until all games are complete"""
    print("🏈 CONTINUOUS Play-by-Play Scraper")
    print("🎯 Will process ALL games until completion")
    print("=" * 60)
    
    start_time = datetime.now()
    games_this_session = 0
    
    # Progress updates every N games
    PROGRESS_INTERVAL = 10
    
    while True:
        # Get next game
        game = get_next_game_to_process()
        if not game:
            print("\n🎉 ALL GAMES COMPLETED!")
            break
        
        db_id, espn_game_id, home_team, away_team, season, week, date = game
        games_this_session += 1
        
        print(f"\n[{games_this_session}] 🎯 {away_team} @ {home_team} ({season} Week {week})")
        print(f"    DB ID: {db_id}, ESPN ID: {espn_game_id}")
        
        # Setup driver and process
        driver = setup_driver()
        try:
            url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{espn_game_id}"
            driver.get(url)
            
            # Expand accordions
            expanded = expand_accordions(driver)
            
            # Extract plays
            plays = extract_plays(driver, db_id)
            
            # Save to database
            if plays:
                saved = save_plays(plays)
                print(f"    ✅ {expanded} accordions → {len(plays)} plays → {saved} saved")
            else:
                print(f"    ⚠️  {expanded} accordions → 0 plays found")
                
        except Exception as e:
            print(f"    ❌ Error: {e}")
        finally:
            driver.quit()
        
        # Progress report every N games
        if games_this_session % PROGRESS_INTERVAL == 0:
            progress = get_progress()
            elapsed = datetime.now() - start_time
            rate = games_this_session / elapsed.total_seconds() * 60  # games per minute
            
            print(f"\n📊 PROGRESS UPDATE:")
            print(f"    ✅ {progress['completed']}/{progress['total']} games ({progress['percent']:.1f}%)")
            print(f"    🎯 {progress['plays']} total plays in database")
            print(f"    ⏱️  Session: {games_this_session} games in {elapsed}")
            print(f"    🚀 Rate: {rate:.1f} games/minute")
            
            if progress['remaining'] > 0:
                eta_minutes = progress['remaining'] / rate if rate > 0 else 0
                print(f"    ⏰ ETA: {eta_minutes:.0f} minutes remaining")
        
        # Brief pause between games
        time.sleep(2)
    
    # Final summary
    final_time = datetime.now() - start_time
    final_progress = get_progress()
    
    print(f"\n🏁 SCRAPING COMPLETE!")
    print("=" * 60)
    print(f"⏱️  Total time: {final_time}")
    print(f"🎯 Games processed this session: {games_this_session}")
    print(f"✅ Total games completed: {final_progress['completed']}/{final_progress['total']}")
    print(f"💾 Total plays in database: {final_progress['plays']}")
    print(f"🎉 Database now contains complete play-by-play data!")

if __name__ == "__main__":
    main()