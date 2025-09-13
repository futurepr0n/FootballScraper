#!/usr/bin/env python3
"""
Test Final Scraper - Quick test of the fixed DOM structure parsing
"""

import time
import sys
from comprehensive_playbyplay_scraper import ComprehensivePlayByPlayScraper

def test_single_game():
    """Test the fixed scraper on one game"""
    print("🧪 TESTING FINAL DOM STRUCTURE SCRAPER")
    print("=" * 50)
    
    scraper = ComprehensivePlayByPlayScraper()
    
    # Test game from user's example
    test_game = {
        'db_id': 9999,
        'espn_game_id': '401671698',
        'away_team': 'DET',
        'home_team': 'SF',
        'season': 2024,
        'week': 17
    }
    
    try:
        # Setup driver and scrape single game
        driver = scraper.setup_driver()
        if driver:
            result = scraper.scrape_single_game(driver, test_game)
            scraper.close_driver(driver)
        else:
            result = False
        
        if result:
            print(f"✅ SUCCESS: Processed {test_game['away_team']} @ {test_game['home_team']}")
            
            # Check database for structured data quality
            conn = scraper.get_database_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) as total_plays,
                           COUNT(CASE WHEN quarter IS NOT NULL THEN 1 END) as with_quarter,
                           COUNT(CASE WHEN time_remaining IS NOT NULL THEN 1 END) as with_time,
                           COUNT(CASE WHEN play_type IS NOT NULL THEN 1 END) as with_play_type,
                           COUNT(CASE WHEN yards_gained IS NOT NULL THEN 1 END) as with_yards
                    FROM plays WHERE game_id = %s
                """, (test_game['db_id'],))
                
                stats = cursor.fetchone()
                if stats:
                    total, quarter, time_rem, play_type, yards = stats
                    print(f"\n📊 STRUCTURED DATA QUALITY:")
                    print(f"   Total plays: {total}")
                    print(f"   With quarter: {quarter}/{total} ({quarter/total*100:.1f}%)")
                    print(f"   With time: {time_rem}/{total} ({time_rem/total*100:.1f}%)")
                    print(f"   With play_type: {play_type}/{total} ({play_type/total*100:.1f}%)")
                    print(f"   With yards: {yards}/{total} ({yards/total*100:.1f}%)")
                    
                    if quarter > 0 and time_rem > 0:
                        print("🎉 SUCCESS: Quarter and time extraction is working!")
                        return True
                    else:
                        print("❌ ISSUE: Quarter/time extraction still not working")
                        return False
                        
                conn.close()
        else:
            print("❌ FAILED: Could not process game")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    success = test_single_game()
    sys.exit(0 if success else 1)