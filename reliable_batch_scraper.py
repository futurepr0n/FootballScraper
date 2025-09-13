#!/usr/bin/env python3
"""
Reliable Batch Scraper - Process games in small batches with better error handling
Prevents hanging by using shorter timeouts and smaller batch sizes
"""

import time
import sys
import traceback
from comprehensive_playbyplay_scraper import (
    get_database_connection, 
    get_games_needing_processing, 
    setup_driver,
    process_single_game_detailed,
    close_driver
)

def main():
    """Process games in very small batches to avoid hanging"""
    print("🔄 RELIABLE BATCH SCRAPER")
    print("=" * 50)
    
    # Use smaller batch size to prevent hanging
    BATCH_SIZE = 3  # Much smaller batches
    MAX_RETRIES = 2
    
    try:
        # Get database connection
        conn = get_database_connection()
        if not conn:
            print("❌ Could not connect to database")
            return
            
        # Get games needing processing
        games = get_games_needing_processing(conn)
        print(f"📊 Found {len(games)} games needing processing")
        
        if not games:
            print("✅ All games already processed!")
            return
            
        # Process in small batches
        batch_count = 0
        for i in range(0, len(games), BATCH_SIZE):
            batch_count += 1
            batch = games[i:i + BATCH_SIZE]
            
            print(f"\n[BATCH {batch_count}] Processing {len(batch)} games...")
            
            driver = None
            try:
                # Setup driver for this batch only
                driver = setup_driver()
                if not driver:
                    print("❌ Could not setup driver")
                    continue
                    
                # Process each game in the batch
                for game_idx, game in enumerate(batch, 1):
                    print(f"\n[{game_idx}/{len(batch)}] {game['away_team']} @ {game['home_team']}")
                    
                    retry_count = 0
                    while retry_count < MAX_RETRIES:
                        try:
                            success = process_single_game_detailed(driver, conn, game)
                            if success:
                                print(f"✅ SUCCESS: Processed game {game['espn_game_id']}")
                                break
                            else:
                                retry_count += 1
                                if retry_count < MAX_RETRIES:
                                    print(f"⚠️  Retry {retry_count}/{MAX_RETRIES}")
                                    time.sleep(5)
                                else:
                                    print(f"❌ FAILED: Max retries reached for {game['espn_game_id']}")
                        except Exception as e:
                            retry_count += 1
                            print(f"⚠️  Error on attempt {retry_count}: {e}")
                            if retry_count < MAX_RETRIES:
                                time.sleep(5)
                            else:
                                print(f"❌ FAILED: Game {game['espn_game_id']} after {MAX_RETRIES} attempts")
                            
            except Exception as e:
                print(f"❌ Batch error: {e}")
                traceback.print_exc()
            finally:
                # Always close driver after each batch
                if driver:
                    try:
                        close_driver(driver)
                        print("🔒 Driver closed")
                    except:
                        pass
                    
                # Wait between batches to avoid rate limiting
                print("⏳ Waiting 10 seconds between batches...")
                time.sleep(10)
                
        print(f"\n🏁 BATCH PROCESSING COMPLETE")
        print(f"📊 Processed {batch_count} batches")
        
        # Show final status
        remaining_games = get_games_needing_processing(conn)
        print(f"📊 Remaining games: {len(remaining_games)}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())