#!/usr/bin/env python3
"""
Playwright Play-by-Play Scraper
Uses Playwright to handle ESPN's dynamic accordion content expansion
Implements the user's webscraping approach with reliable browser automation
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
import time
import os
import re
from datetime import datetime
from pathlib import Path
import csv

class PlaywrightPlayByPlayScraper:
    def __init__(self):
        # Database connection
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.csv_dir.mkdir(exist_ok=True)
        
        self.successful_games = 0
        self.failed_games = 0
        self.skipped_games = 0
        
        print("🏈 Playwright Play-by-Play Scraper initialized")
        print("🎯 Focus: MCP Playwright browser automation with accordion expansion")
    
    def get_all_games(self, limit=None):
        """Get all games from database"""
        try:
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            self.cursor.execute(f"""
                SELECT 
                    game_id,
                    season,
                    week,
                    season_type,
                    date,
                    ht.abbreviation as home_team,
                    at.abbreviation as away_team,
                    home_score,
                    away_score
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.id
                JOIN teams at ON g.away_team_id = at.id
                ORDER BY date DESC, game_id
                {limit_clause}
            """)
            
            games = self.cursor.fetchall()
            print(f"📊 Found {len(games)} games for processing")
            return games
            
        except Exception as e:
            print(f"❌ Failed to get games: {e}")
            return []
    
    def check_existing_plays(self, game_id):
        """Check if play-by-play data already exists in database"""
        try:
            self.cursor.execute("SELECT COUNT(*) as play_count FROM plays WHERE game_id = %s", (game_id,))
            count = self.cursor.fetchone()['play_count']
            return count > 0
        except:
            return False
    
    def scrape_game_plays(self, game_id, home_team, away_team, game_date):
        """Scrape play-by-play data for a single game using MCP Playwright"""
        
        if self.check_existing_plays(game_id):
            print(f"   ✅ Play-by-play already exists for {game_id}")
            self.skipped_games += 1
            return True
        
        espn_url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_id}"
        print(f"   📡 Loading ESPN page: {espn_url}")
        
        try:
            print("   🌐 This scraper requires MCP Playwright browser automation")
            print("   📋 Run this scraper manually with MCP functions:")
            print(f"   1. Navigate: mcp__playwright__browser_navigate('{espn_url}')")
            print("   2. Expand accordions: Use the JavaScript provided below")
            print("   3. Extract plays: Use the play extraction script")
            
            # For now, return a placeholder - this needs to be run with MCP
            self.failed_games += 1
            return False
            if not nav_result.get('success', False):
                print(f"   ❌ Failed to navigate to {espn_url}")
                return False
            
            # Wait for page to load
            time.sleep(3)
            
            # Take a snapshot to see the page structure
            snapshot = mcp__playwright__browser_snapshot()
            
            # Look for accordion buttons with aria-expanded="false"
            accordion_buttons = []
            page_content = snapshot.get('content', '')
            
            # Find collapsed accordion buttons using your suggested approach
            if 'aria-expanded="false"' in page_content:
                print(f"   🎯 Found collapsed accordions, expanding them...")
                
                # Use JavaScript to find and click all collapsed accordions
                expand_script = """
                () => {
                    let expandedCount = 0;
                    
                    // Find all buttons with aria-expanded="false"
                    const collapsedButtons = document.querySelectorAll('button[aria-expanded="false"]');
                    
                    collapsedButtons.forEach((button, index) => {
                        try {
                            // Scroll into view and click
                            button.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            setTimeout(() => {
                                button.click();
                                expandedCount++;
                            }, 100 * index); // Stagger clicks
                        } catch (e) {
                            console.log('Failed to click button:', e);
                        }
                    });
                    
                    return {
                        found: collapsedButtons.length,
                        expanded: expandedCount
                    };
                }
                """
                
                expand_result = mcp__playwright__browser_evaluate(function=expand_script)
                
                if expand_result.get('result'):
                    result_data = expand_result['result']
                    print(f"   ✅ Found {result_data.get('found', 0)} accordions, expanded {result_data.get('expanded', 0)}")
                    
                    # Wait for content to load after expansion
                    time.sleep(5)
                
            # Extract play-by-play data after expansion
            extract_script = """
            () => {
                const plays = [];
                let playIndex = 0;
                
                // Look for various play container selectors
                const playSelectors = [
                    '[data-testid*="play"]',
                    '.play-item',
                    '.play-row',
                    'section.liAe',
                    '[class*="play"]'
                ];
                
                let playElements = [];
                
                for (const selector of playSelectors) {
                    const elements = document.querySelectorAll(selector);
                    if (elements.length > 0) {
                        playElements = Array.from(elements);
                        console.log(`Found ${elements.length} plays with selector: ${selector}`);
                        break;
                    }
                }
                
                if (playElements.length === 0) {
                    // Try to find any elements containing play indicators
                    const playText = ['1st down', '2nd down', '3rd down', '4th down', 'touchdown', 'field goal', 'punt', 'kickoff'];
                    const allElements = document.querySelectorAll('*');
                    
                    allElements.forEach(element => {
                        const text = element.textContent?.toLowerCase() || '';
                        if (playText.some(indicator => text.includes(indicator))) {
                            playElements.push(element);
                        }
                    });
                }
                
                // Extract play data
                playElements.forEach((element, index) => {
                    try {
                        const playText = element.textContent?.trim() || '';
                        
                        if (playText.length > 10) { // Filter out empty/short elements
                            plays.push({
                                play_index: playIndex++,
                                play_text: playText.substring(0, 500), // Limit text length
                                quarter: null, // Will parse later
                                time_remaining: null, // Will parse later
                                down: null, // Will parse later
                                yards_to_go: null, // Will parse later
                                yard_line: null // Will parse later
                            });
                        }
                    } catch (e) {
                        console.log('Error extracting play:', e);
                    }
                });
                
                return {
                    total_plays: plays.length,
                    plays: plays
                };
            }
            """
            
            plays_result = mcp__playwright__browser_evaluate(function=extract_script)
            
            if plays_result.get('result'):
                result_data = plays_result['result']
                plays = result_data.get('plays', [])
                
                if plays:
                    print(f"   🎉 Extracted {len(plays)} plays!")
                    
                    # Save plays to database
                    self.save_plays_to_database(game_id, plays)
                    
                    # Save plays to CSV backup
                    self.save_plays_to_csv(game_id, plays, home_team, away_team, game_date)
                    
                    self.successful_games += 1
                    return True
                else:
                    print(f"   ❌ No plays extracted")
                    self.failed_games += 1
                    return False
            else:
                print(f"   ❌ Failed to execute play extraction script")
                self.failed_games += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error scraping game {game_id}: {e}")
            self.failed_games += 1
            return False
    
    def save_plays_to_database(self, game_id, plays):
        """Save extracted plays to PostgreSQL database"""
        try:
            for play in plays:
                self.cursor.execute("""
                    INSERT INTO plays (
                        game_id, play_index, play_text, quarter, 
                        time_remaining, down, yards_to_go, yard_line
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, play_index) DO NOTHING
                """, (
                    game_id,
                    play['play_index'],
                    play['play_text'],
                    play['quarter'],
                    play['time_remaining'],
                    play['down'],
                    play['yards_to_go'],
                    play['yard_line']
                ))
            
            self.conn.commit()
            print(f"   ✅ Saved {len(plays)} plays to database")
            
        except Exception as e:
            print(f"   ❌ Failed to save plays to database: {e}")
            self.conn.rollback()
    
    def save_plays_to_csv(self, game_id, plays, home_team, away_team, game_date):
        """Save plays to CSV backup file"""
        try:
            csv_filename = f"playbyplay_{game_id}_{home_team}_vs_{away_team}_{game_date}.csv"
            csv_path = self.csv_dir / csv_filename
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['game_id', 'play_index', 'play_text', 'quarter', 'time_remaining', 'down', 'yards_to_go', 'yard_line']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for play in plays:
                    play['game_id'] = game_id
                    writer.writerow(play)
            
            print(f"   ✅ Saved plays to CSV: {csv_filename}")
            
        except Exception as e:
            print(f"   ❌ Failed to save CSV: {e}")
    
    def run_scraper(self, test_mode=True):
        """Run the scraper on all games"""
        
        print(f"\n{'='*80}")
        print(f"🏈 PLAYWRIGHT PLAY-BY-PLAY SCRAPER")
        print(f"{'='*80}")
        
        # Get games (limit to 5 for testing)
        limit = 5 if test_mode else None
        games = self.get_all_games(limit=limit)
        
        if not games:
            print("❌ No games found")
            return
        
        print(f"📊 Processing {'first 5 games (TEST MODE)' if test_mode else 'all games'}")
        print(f"📅 Date range: {games[-1]['date']} to {games[0]['date']}")
        
        for i, game in enumerate(games, 1):
            print(f"\n[{i}/{len(games)}] {'='*31}")
            print(f"🎯 Processing: {game['away_team']} {game['away_score']} @ {game['home_team']} {game['home_score']}")
            print(f"   Game ID: {game['game_id']} ({game['season']} Week {game['week']}, {game['season_type']})")
            
            success = self.scrape_game_plays(
                game['game_id'],
                game['home_team'],
                game['away_team'], 
                game['date']
            )
            
            if i % 10 == 0:
                print(f"\n📊 Progress: {i}/{len(games)} - Success: {self.successful_games}, Failed: {self.failed_games}, Skipped: {self.skipped_games}")
        
        # Final summary
        print(f"\n{'='*80}")
        print(f"🏁 SCRAPING COMPLETE")
        print(f"{'='*80}")
        print(f"✅ Successful: {self.successful_games}")
        print(f"❌ Failed: {self.failed_games}")
        print(f"⏭️  Skipped: {self.skipped_games}")
        print(f"📊 Success Rate: {(self.successful_games / (self.successful_games + self.failed_games) * 100):.1f}%" if (self.successful_games + self.failed_games) > 0 else "N/A")

def main():
    """Main function to run the scraper"""
    scraper = PlaywrightPlayByPlayScraper()
    
    # Run in test mode first (5 games)
    print("🧪 Starting TEST MODE (5 games)")
    scraper.run_scraper(test_mode=True)

if __name__ == "__main__":
    main()