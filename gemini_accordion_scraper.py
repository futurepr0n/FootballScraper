#!/usr/bin/env python3
"""
Gemini-Generated ESPN Accordion Expansion Scraper
Uses Selenium WebDriver to expand ESPN's dynamic accordion content
Created by Gemini 2.5 Flash with high context window capability
"""

import time
import csv
import os
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pathlib import Path

def scrape_espn_nfl_playbyplay(game_id):
    """
    Scrapes ESPN NFL play-by-play data for a given game ID, expands accordions,
    extracts data, saves to PostgreSQL, and creates a CSV backup.
    """
    # Database configuration - Updated with correct credentials
    DB_HOST = '192.168.1.23'
    DB_NAME = 'football_tracker'
    DB_USER = 'postgres'  # Correct username
    DB_PASSWORD = 'korn5676'  # Correct password

    # CSV backup directory - Updated to correct path
    CSV_BACKUP_DIR = Path('/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS')
    CSV_BACKUP_DIR.mkdir(exist_ok=True)
    csv_filename = CSV_BACKUP_DIR / f'gemini_playbyplay_{game_id}.csv'

    # Configure Chrome options for stealth browsing
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("prefs", {"profile.default_content_setting_values.notifications": 2})

    driver = None
    conn = None
    cur = None

    try:
        print(f"🏈 Starting ESPN scraper for game {game_id}")
        
        # Initialize WebDriver
        print("🔧 Setting up ChromeDriver...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # Hide the WebDriver property
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })

        url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_id}"
        print(f"📡 Navigating to: {url}")
        driver.get(url)

        # Wait for the page to load - Updated selectors for ESPN's actual structure
        print("⏳ Waiting for page to load...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Give additional time for JavaScript to load
        time.sleep(3)
        print("✅ Page loaded. Looking for accordions...")

        # Enhanced accordion expansion approach - multiple rounds to catch dynamic loading
        expanded_count = 0
        accordion_selectors = [
            "button[aria-expanded='false']",  # Main target - collapsed accordions
            "button[aria-controls*='details']",  # ESPN accordion pattern
            "[data-testid*='Accordion'] button",  # Prism accordion buttons
            "button[class*='Accordion']",  # Any button with Accordion in class
            "button[class*='Button'][aria-expanded='false']",  # ESPN dynamic classes
            "button[data-testid*='prism']",  # Prism components
        ]
        
        # Multiple rounds of expansion - some accordions appear after others are expanded
        max_rounds = 3
        for round_num in range(max_rounds):
            print(f"   🔄 Round {round_num + 1} of accordion expansion...")
            round_expanded = 0
            
            for selector in accordion_selectors:
                try:
                    accordions = driver.find_elements(By.CSS_SELECTOR, selector)
                    if accordions:
                        print(f"   📋 Found {len(accordions)} elements with selector: {selector}")
                    
                    for accordion in accordions:
                        try:
                            # Check if actually collapsed and visible
                            aria_expanded = accordion.get_attribute('aria-expanded')
                            if aria_expanded == 'false' and accordion.is_displayed():
                                # Scroll into view and click
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", accordion)
                                time.sleep(0.2)
                                driver.execute_script("arguments[0].click();", accordion)
                                expanded_count += 1
                                round_expanded += 1
                                time.sleep(0.3)  # Brief pause between clicks
                                print(f"   ✅ Expanded accordion {expanded_count}")
                        except Exception as e:
                            continue  # Skip accordions that can't be clicked
                            
                except Exception as e:
                    print(f"   ⚠️  Selector {selector} failed: {e}")
                    continue
            
            print(f"   🎯 Round {round_num + 1} expanded: {round_expanded}")
            
            # If we expanded accordions, wait for content to load before next round
            if round_expanded > 0:
                time.sleep(2)
            else:
                print(f"   ✅ No more accordions found in round {round_num + 1}")
                break

        print(f"🎯 Total accordions expanded: {expanded_count}")
        
        # Wait for content to load after expansion
        if expanded_count > 0:
            print("⏳ Waiting for expanded content to load...")
            time.sleep(5)

        print("📄 Extracting play-by-play data...")

        # Extract play-by-play data with multiple strategies
        plays_data = []
        play_index_counter = 0

        # Strategy 1: Look for ESPN's specific play-by-play structure
        try:
            # Try to find play containers
            play_containers = driver.find_elements(By.CSS_SELECTOR, ".PlayByPlay__Row")
            if not play_containers:
                play_containers = driver.find_elements(By.CSS_SELECTOR, "[data-testid*='play']")
            if not play_containers:
                play_containers = driver.find_elements(By.CSS_SELECTOR, ".play-item, .play-row")
            
            print(f"   Found {len(play_containers)} play containers")
            
            for container in play_containers:
                try:
                    play_text = container.text.strip()
                    if len(play_text) > 10:  # Skip empty containers
                        plays_data.append({
                            'game_id': int(game_id),
                            'play_sequence': play_index_counter,
                            'play_description': play_text[:500],  # Limit length
                            'quarter': None,  # Will be parsed later if needed
                            'time_remaining': None,
                            'down': None,
                            'distance': None,
                            'yards_gained': None,
                            'play_type': None,
                            'play_summary': None,
                            'time_quarter': None,
                            'situation': None
                        })
                        play_index_counter += 1
                except:
                    continue
                    
        except Exception as e:
            print(f"   Strategy 1 failed: {e}")

        # Strategy 2: If no structured containers found, search for play patterns in text
        if not plays_data:
            print("   No structured containers found, searching text patterns...")
            
            # Get all text elements and search for play indicators
            body_text = driver.find_element(By.TAG_NAME, "body").text
            lines = body_text.split('\n')
            
            play_patterns = [
                r'\d+(?:st|nd|rd|th)\s+(?:down|and)',
                r'touchdown', r'field goal', r'punt', r'kickoff',
                r'interception', r'fumble', r'sack', r'penalty'
            ]
            
            import re
            for line in lines:
                line_clean = line.strip()
                if len(line_clean) > 20:  # Skip short lines
                    for pattern in play_patterns:
                        if re.search(pattern, line_clean, re.IGNORECASE):
                            plays_data.append({
                                'game_id': int(game_id),
                                'play_sequence': play_index_counter,
                                'play_description': line_clean[:500],
                                'quarter': None,
                                'time_remaining': None,
                                'down': None,
                                'distance': None,
                                'yards_gained': None,
                                'play_type': None,
                                'play_summary': None,
                                'time_quarter': None,
                                'situation': None
                            })
                            play_index_counter += 1
                            break

        print(f"✅ Extracted {len(plays_data)} plays")

        # Save to CSV
        if plays_data:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['game_id', 'play_sequence', 'play_description', 'quarter', 'time_remaining', 'down', 'distance', 'yards_gained', 'play_type', 'play_summary', 'time_quarter', 'situation']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(plays_data)
            print(f"💾 Data saved to CSV: {csv_filename}")
        else:
            print("❌ No plays extracted to save to CSV")

        # Connect to PostgreSQL and insert data
        if plays_data:
            print("🗃️  Connecting to PostgreSQL...")
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cur = conn.cursor()

            # Insert plays using existing database schema
            insert_count = 0
            for play in plays_data:
                try:
                    cur.execute("""
                        INSERT INTO plays (game_id, play_sequence, play_description, quarter, time_remaining, down, distance, yards_gained, play_type, play_summary, time_quarter, situation)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        play['game_id'],
                        play['play_sequence'],
                        play['play_description'],
                        play['quarter'],
                        play['time_remaining'],
                        play['down'],
                        play['distance'],
                        play['yards_gained'],
                        play['play_type'],
                        play['play_summary'],
                        play['time_quarter'],
                        play['situation']
                    ))
                    if cur.rowcount > 0:
                        insert_count += 1
                except Exception as e:
                    print(f"Error inserting play {play['game_id']}-{play['play_sequence']}: {e}")
                    conn.rollback()

            conn.commit()
            print(f"✅ Inserted {insert_count} plays into PostgreSQL")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("🔒 PostgreSQL connection closed")
        if driver:
            driver.quit()
            print("🔒 WebDriver closed")

def main():
    """Test the scraper on game 401547444"""
    print("🧪 Testing Gemini Accordion Expansion Scraper")
    print("=" * 60)
    scrape_espn_nfl_playbyplay(401547444)  # Broncos vs Bears Oct 1, 2023
    print("=" * 60)
    print("🏁 Test complete")

if __name__ == "__main__":
    main()