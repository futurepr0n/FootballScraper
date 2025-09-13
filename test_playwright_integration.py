#!/usr/bin/env python3
"""
Test Playwright Integration - Complete the MCP Playwright integration
Combine the JavaScript extraction with the parsing logic from playwright_card_scraper.py
"""

import time
import re
import psycopg2

class PlaywrightIntegrationTest:
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

    def parse_card_data(self, card_info, play_index):
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
            
            # Based on user's DOM structure:
            # texts[0] = "Kickoff" (play type)
            # texts[1] = "15:00 - 1st" (time/quarter)  
            # texts[2] = "J.Bates kicks 63 yards..." (description)
            
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

    def parse_play_type_header(self, header_text):
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

    def test_sample_data(self):
        """Test the parsing logic with sample card data"""
        print("🧪 TESTING PLAYWRIGHT INTEGRATION WITH SAMPLE DATA")
        print("=" * 60)
        
        # Sample card data based on user's HTML structure
        sample_cards = [
            {
                'texts': [
                    'Kickoff',
                    '15:00 - 1st', 
                    'J.Bates kicks 63 yards from SF 35 to DET 2. K.Raymond to DET 22 for 20 yards (D.Greenlaw).',
                    '1st & 10 at DET 22'
                ]
            },
            {
                'texts': [
                    '3-yd Run',
                    '14:17 - 1st',
                    'D.Montgomery right tackle to DET 25 for 3 yards (N.Bosa; F.Warner).',
                    '2nd & 7 at DET 25'
                ]
            },
            {
                'texts': [
                    '18-yd Pass',
                    '13:41 - 1st',
                    'J.Goff pass short right to J.Reynolds to DET 43 for 18 yards (C.Ward).',
                    '1st & 10 at DET 43'
                ]
            }
        ]
        
        structured_plays = []
        
        for i, card_data in enumerate(sample_cards):
            play_data = self.parse_card_data(card_data, i)
            if play_data:
                structured_plays.append(play_data)
        
        print(f"\n📊 PARSING RESULTS:")
        print(f"   Sample cards: {len(sample_cards)}")
        print(f"   Successfully parsed: {len(structured_plays)}")
        
        for play in structured_plays:
            has_complete_structure = (play['quarter'] and 
                                    play['time_remaining'] and 
                                    play['play_type'] and 
                                    play['play_description'])
            
            print(f"\n{'✅' if has_complete_structure else '❌'} PLAY #{play['play_number']}:")
            print(f"   🕐 Time: {play['time_remaining']} - Q{play['quarter']}")
            print(f"   🏈 Type: {play['play_type']}, Yards: {play['yards_gained']}")
            if play['down']:
                print(f"   📍 Down: {play['down']} & {play['distance']} at {play['yard_line']}")
            print(f"   👥 Players: {play['player_names']}")
            print(f"   📝 Description: {play['play_description'][:60]}...")
        
        success_rate = len(structured_plays) / len(sample_cards) * 100
        print(f"\n🎯 PARSING SUCCESS RATE: {success_rate:.1f}% ({len(structured_plays)}/{len(sample_cards)})")
        
        if len(structured_plays) == len(sample_cards):
            print("✅ SUCCESS: All sample plays parsed with complete structure!")
            print("🚀 Ready to integrate with MCP Playwright browser tools")
            return True
        else:
            print("❌ ISSUE: Some plays failed to parse")
            return False

if __name__ == "__main__":
    tester = PlaywrightIntegrationTest()
    success = tester.test_sample_data()
    exit(0 if success else 1)