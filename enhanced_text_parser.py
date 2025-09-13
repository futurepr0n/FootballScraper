#!/usr/bin/env python3
"""
Enhanced Text Parser for ESPN Play Descriptions
Extract structured data from the text we're already successfully capturing
"""

import re

def enhanced_parse_play_text(play_text, play_index):
    """Enhanced parser that extracts ALL possible structured data from play descriptions"""
    try:
        # Initialize play data structure
        play_data = {
            'play_number': play_index,
            'play_description': play_text.strip()[:500],
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
        
        text = play_text.strip()
        
        # Extract yards gained - IMPROVED PATTERNS
        yards_patterns = [
            r'for (-?\d+) yards?',           # "for 12 yards"
            r'to [A-Z]{2,3} \d+ for (-?\d+) yards?',  # "to BUF 47 for 12 yards"
            r'(-?\d+) yard (?:gain|loss|pass|rush|run)',  # "12 yard gain"
            r'(?:gained|gained).*?(-?\d+) yards?',       # "gained 8 yards"
            r'kicks (-?\d+) yards?',         # "kicks 65 yards"
        ]
        
        for pattern in yards_patterns:
            yards_match = re.search(pattern, text, re.IGNORECASE)
            if yards_match:
                yards = int(yards_match.group(1))
                play_data['yards_gained'] = yards
                break
        
        # Extract yard line (field position)
        yard_line_patterns = [
            r'to ([A-Z]{2,3}) (\d+)',        # "to BUF 47"
            r'at ([A-Z]{2,3}) (\d+)',        # "at NYJ 40"
            r'from ([A-Z]{2,3}) (\d+)',      # "from NYJ 35"
        ]
        
        for pattern in yard_line_patterns:
            yard_match = re.search(pattern, text)
            if yard_match:
                # Convert to numeric yard line (0-100 scale)
                team = yard_match.group(1)
                yard = int(yard_match.group(2))
                play_data['yard_line'] = yard
                break
        
        # Detect play type - ENHANCED PATTERNS
        play_types = {
            'pass': r'(?:pass|threw|completion|incomplete|sack)',
            'rush': r'(?:rush|ran|carry|handoff|left tackle|right tackle|left guard|right guard|left end|right end)',
            'punt': r'punt',
            'field_goal': r'field goal',
            'kickoff': r'kickoff|kicks.*yards from',
            'extra_point': r'extra point',
            'safety': r'safety',
            'kneel': r'kneel'
        }
        
        for play_type, pattern in play_types.items():
            if re.search(pattern, text, re.IGNORECASE):
                play_data['play_type'] = play_type
                break
        
        # Detect special events - ENHANCED
        if re.search(r'touchdown|TD', text, re.IGNORECASE):
            play_data['touchdown'] = True
            
        if re.search(r'PENALTY|penalty', text, re.IGNORECASE):
            play_data['penalty'] = True
            
        if re.search(r'FUMBLES|fumble|interception|intercepted', text, re.IGNORECASE):
            play_data['turnover'] = True
        
        # Extract player names - IMPROVED
        # Look for patterns like "J.Allen pass", "J.Cook right tackle", etc.
        name_patterns = [
            r'([A-Z]\.[A-Z][a-z]+)',          # "J.Allen" format
            r'([A-Z][a-z]+ [A-Z][a-z]+)',     # "Josh Allen" format  
            r'\(([A-Z]\.[A-Z][a-z]+)\)',      # "(J.Allen)" format
        ]
        
        names_found = []
        for pattern in name_patterns:
            names = re.findall(pattern, text)
            names_found.extend(names)
        
        # Remove duplicates and limit to 3 names
        unique_names = list(dict.fromkeys(names_found))[:3]
        play_data['player_names'] = unique_names
        
        # Try to infer down from context (limited success but better than nothing)
        if '1st' in text and '&' in text:
            play_data['down'] = 1
        elif '2nd' in text and '&' in text:
            play_data['down'] = 2
        elif '3rd' in text and '&' in text:
            play_data['down'] = 3
        elif '4th' in text and '&' in text:
            play_data['down'] = 4
            
        # Try to extract distance from down context
        down_distance_pattern = r'(\d+)(?:st|nd|rd|th) & (\d+)'
        down_match = re.search(down_distance_pattern, text, re.IGNORECASE)
        if down_match:
            play_data['down'] = int(down_match.group(1))
            play_data['distance'] = int(down_match.group(2))
        
        return play_data
        
    except Exception as e:
        print(f"   ⚠️  Enhanced parsing error: {e}")
        return {
            'play_number': play_index,
            'play_description': play_text.strip()[:500],
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

# Test the enhanced parser
if __name__ == "__main__":
    test_plays = [
        "T.Morstead kicks 65 yards from NYJ 35 to end zone, Touchback to the BUF 30.",
        "(Shotgun) J.Allen pass short right to K.Shakir pushed ob at BUF 47 for 12 yards (C.Clark).",
        "(Shotgun) J.Cook right tackle to BUF 35 for 5 yards (Q.Williams).",
        "PENALTY on NYJ-T.Adams, Illegal Contact, 5 yards, enforced at BUF 47 - No Play.",
        "J.Allen pass incomplete deep right to D.Kincaid.",
    ]
    
    print("🧪 TESTING ENHANCED TEXT PARSER")
    print("="*50)
    
    for i, play_text in enumerate(test_plays):
        print(f"\n🎯 Play {i+1}: {play_text}")
        result = enhanced_parse_play_text(play_text, i)
        print(f"   ✅ Type: {result['play_type']}, Yards: {result['yards_gained']}, Players: {result['player_names']}")
        print(f"   📊 TD: {result['touchdown']}, Penalty: {result['penalty']}, Turnover: {result['turnover']}")
        if result['down'] or result['distance']:
            print(f"   🏈 Down: {result['down']}, Distance: {result['distance']}")