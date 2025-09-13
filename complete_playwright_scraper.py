#!/usr/bin/env python3
"""
Complete Playwright Scraper - MCP Playwright integration with proven parsing logic
Extract structured NFL play-by-play data using browser automation
"""

import time
import re
import psycopg2
from typing import Dict, List, Optional

class CompletePlaywrightScraper:
    def __init__(self):
        self.conn = None
        self.connect_to_database()
    
    def connect_to_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host="192.168.1.23",
                database="football_tracker", 
                user="postgres",
                password="korn5676",
                port=5432
            )
            print("✅ Database connected")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            self.conn = None

    def get_games_needing_processing(self):
        """Get games that need structured play-by-play data"""
        if not self.conn:
            return []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT DISTINCT g.id, g.game_id, at.abbreviation as away_team, ht.abbreviation as home_team, g.season, g.week
                FROM games g
                LEFT JOIN teams at ON g.away_team_id = at.id
                LEFT JOIN teams ht ON g.home_team_id = ht.id
                WHERE g.id NOT IN (
                    SELECT DISTINCT game_id 
                    FROM plays 
                    WHERE quarter IS NOT NULL 
                    AND time_remaining IS NOT NULL
                )
                ORDER BY g.season DESC, g.week DESC
                LIMIT 10
            """)
            
            games = []
            for row in cursor.fetchall():
                games.append({
                    'db_id': row[0],
                    'espn_game_id': row[1], 
                    'away_team': row[2],
                    'home_team': row[3],
                    'season': row[4],
                    'week': row[5]
                })
            
            cursor.close()
            return games
            
        except Exception as e:
            print(f"❌ Error getting games: {e}")
            return []

    def extract_play_cards_js(self):
        """JavaScript function to extract play card data from ESPN page"""
        return '''
        () => {
            // Find all sections that might be play cards
            const sections = document.querySelectorAll('section');
            const playCards = [];
            
            sections.forEach((section, index) => {
                const text = section.textContent || '';
                
                // Check if this looks like a play card (has time pattern and play keywords)
                if (text.length > 20 && 
                    /\\d{1,2}:\\d{2}\\s*-\\s*(1st|2nd|3rd|4th)/.test(text) &&
                    /(kick|pass|run|punt|yards|touchdown)/i.test(text)) {
                    
                    // Get all div texts within this section
                    const divs = section.querySelectorAll('div');
                    const meaningfulTexts = [];
                    
                    divs.forEach(div => {
                        const divText = div.textContent.trim();
                        // Only keep divs with meaningful, short text
                        if (divText && divText.length > 5 && divText.length < 200) {
                            // Avoid duplicates (nested divs repeat text)
                            if (!meaningfulTexts.includes(divText)) {
                                meaningfulTexts.push(divText);
                            }
                        }
                    });
                    
                    if (meaningfulTexts.length >= 2) {
                        playCards.push({
                            index: index,
                            texts: meaningfulTexts.slice(0, 5)
                        });
                    }
                }
            });
            
            return playCards.slice(0, 50); // Return first 50 play cards
        }
        '''

    def expand_accordions_js(self):
        """JavaScript function to expand ESPN play-by-play accordions"""
        return '''
        () => {
            const buttons = document.querySelectorAll('button[aria-expanded="false"]');
            let expanded = 0;
            
            buttons.forEach(button => {
                if (button.getAttribute('aria-expanded') === 'false') {
                    button.click();
                    expanded++;
                }
            });
            
            return expanded;
        }
        '''

    def parse_card_data(self, card_info: Dict, play_index: int) -> Optional[Dict]:
        """Parse extracted card data into structured play information"""
        try:
            texts = card_info.get('texts', [])
            
            if len(texts) < 2:
                return None
            
            # Initialize play data
            play_data = {
                'play_number': play_index,
                'play_description': None,
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
                'player_names': []
            }
            
            # Parse play type (first meaningful text)
            if len(texts) >= 1:
                play_type_text = texts[0]
                play_data['play_type'] = self.parse_play_type_header(play_type_text)
                
                # Extract yards from play type header if present
                yards_match = re.search(r'(\d+)-yd', play_type_text)
                if yards_match:
                    play_data['yards_gained'] = int(yards_match.group(1))
            
            # Parse time/quarter (second meaningful text)
            if len(texts) >= 2:
                time_quarter_text = texts[1]
                time_match = re.match(r'(\d{1,2}:\d{2})\s*-\s*(1st|2nd|3rd|4th|OT)', time_quarter_text, re.IGNORECASE)
                if time_match:
                    play_data['time_remaining'] = time_match.group(1)
                    quarter_text = time_match.group(2).lower()
                    quarter_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4, 'ot': 5}
                    play_data['quarter'] = quarter_map.get(quarter_text)
            
            # Parse description (third meaningful text)
            if len(texts) >= 3:
                description_text = texts[2]
                play_data['play_description'] = description_text[:500]
                
                # Extract player names from description
                player_names = re.findall(r'([A-Z]\.[A-Z][a-z]+)', description_text)
                play_data['player_names'] = list(dict.fromkeys(player_names))[:3]
                
                # Check for special events
                desc_upper = description_text.upper()
                if 'TOUCHDOWN' in desc_upper or ' TD ' in desc_upper:
                    play_data['touchdown'] = True
                if 'PENALTY' in desc_upper:
                    play_data['penalty'] = True
                if 'FUMBLE' in desc_upper or 'INTERCEPTION' in desc_upper:
                    play_data['turnover'] = True
                    
                # Extract yards from description if not found in header
                if not play_data['yards_gained']:
                    yards_patterns = [
                        r'for (-?\d+) yards?',
                        r'kicks (-?\d+) yards?', 
                        r'(-?\d+) yard (?:gain|loss)',
                    ]
                    for pattern in yards_patterns:
                        yards_match = re.search(pattern, description_text, re.IGNORECASE)
                        if yards_match:
                            play_data['yards_gained'] = int(yards_match.group(1))
                            break
            
            # Look for down/distance in remaining texts
            for text in texts[3:]:
                down_match = re.search(r'(1st|2nd|3rd|4th)\s*&\s*(\d+)\s*at\s*([A-Z]{2,3})\s*(\d+)', text)
                if down_match:
                    down_map = {'1st': 1, '2nd': 2, '3rd': 3, '4th': 4}
                    play_data['down'] = down_map.get(down_match.group(1).lower())
                    play_data['distance'] = int(down_match.group(2))
                    play_data['yard_line'] = int(down_match.group(4))
                    break
            
            # Only return if we got essential structured data
            if play_data['play_type'] and play_data['quarter'] and play_data['time_remaining']:
                return play_data
            
            return None
            
        except Exception as e:
            print(f"⚠️  Card data parse error: {e}")
            return None

    def parse_play_type_header(self, header_text: str) -> str:
        """Parse play type from header text like 'Kickoff', '13-yd Pass', etc."""
        text = header_text.lower().strip()
        
        if 'kickoff' in text:
            return 'kickoff'
        elif 'field goal' in text:
            return 'field_goal'
        elif 'extra point' in text:
            return 'extra_point'
        elif 'punt' in text:
            return 'punt'
        elif 'safety' in text:
            return 'safety'
        elif 'sack' in text:
            return 'sack'
        elif 'run' in text or 'rush' in text:
            return 'rush'
        elif 'pass' in text:
            return 'pass'
        elif 'incomplete' in text:
            return 'pass'
        else:
            return text[:20]  # Return cleaned version for debugging

    def save_plays_to_database(self, plays_data: List[Dict], game_id: int) -> int:
        """Save structured plays to database"""
        if not self.conn or not plays_data:
            return 0
        
        try:
            cursor = self.conn.cursor()
            saved_count = 0
            
            for play in plays_data:
                play['game_id'] = game_id
                
                cursor.execute("""
                    INSERT INTO plays (
                        game_id, play_number, play_description, quarter, time_remaining,
                        down, distance, yard_line, play_type, yards_gained,
                        touchdown, penalty, turnover, player_names
                    ) VALUES (
                        %(game_id)s, %(play_number)s, %(play_description)s, %(quarter)s, %(time_remaining)s,
                        %(down)s, %(distance)s, %(yard_line)s, %(play_type)s, %(yards_gained)s,
                        %(touchdown)s, %(penalty)s, %(turnover)s, %(player_names)s
                    )
                    ON CONFLICT (game_id, play_number) DO UPDATE SET
                        play_description = EXCLUDED.play_description,
                        quarter = EXCLUDED.quarter,
                        time_remaining = EXCLUDED.time_remaining,
                        down = EXCLUDED.down,
                        distance = EXCLUDED.distance,
                        yard_line = EXCLUDED.yard_line,
                        play_type = EXCLUDED.play_type,
                        yards_gained = EXCLUDED.yards_gained,
                        touchdown = EXCLUDED.touchdown,
                        penalty = EXCLUDED.penalty,
                        turnover = EXCLUDED.turnover,
                        player_names = EXCLUDED.player_names
                """, play)
                saved_count += 1
            
            self.conn.commit()
            cursor.close()
            print(f"✅ Saved {saved_count} plays to database")
            return saved_count
            
        except Exception as e:
            print(f"❌ Database save error: {e}")
            if self.conn:
                self.conn.rollback()
            return 0

def scrape_single_game_with_playwright(game_data):
    """
    Function to be used with MCP Playwright browser tools
    This function expects to be called after browser navigation and setup
    """
    scraper = CompletePlaywrightScraper()
    
    print(f"🎯 Processing: {game_data['away_team']} @ {game_data['home_team']}")
    print(f"   ESPN Game ID: {game_data['espn_game_id']}")
    
    # Instructions for MCP Playwright integration:
    print("\n📋 MCP PLAYWRIGHT INTEGRATION STEPS:")
    print("1. Navigate to ESPN play-by-play page")
    print("2. Wait for page load (3-5 seconds)")
    print("3. Expand accordions using JavaScript")
    print("4. Extract play card data using JavaScript")
    print("5. Parse and save structured data")
    
    # Return the scraper instance and JavaScript functions for MCP use
    return {
        'scraper': scraper,
        'expand_js': scraper.expand_accordions_js(),
        'extract_js': scraper.extract_play_cards_js(),
        'game_data': game_data
    }

if __name__ == "__main__":
    scraper = CompletePlaywrightScraper()
    
    # Get games needing processing
    games = scraper.get_games_needing_processing()
    
    if games:
        print(f"🎯 Found {len(games)} games needing structured play data")
        test_game = games[0]
        
        print(f"\n🧪 READY FOR PLAYWRIGHT INTEGRATION")
        print(f"   Test Game: {test_game['away_team']} @ {test_game['home_team']}")
        print(f"   ESPN URL: https://www.espn.com/nfl/playbyplay/_/gameId/{test_game['espn_game_id']}")
        print(f"\n📝 Next step: Use MCP Playwright tools to navigate and extract data")
        
        # Return integration helper
        integration = scrape_single_game_with_playwright(test_game)
        
    else:
        print("✅ All games already have structured play data")