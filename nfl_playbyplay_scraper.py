#!/usr/bin/env python3
"""
NFL Play-by-Play Scraper for ESPN API
Extracts detailed play-by-play data including formations, downs, quarters, and situational analysis
Similar to MLB system but focused on NFL-specific data points
"""

import json
import requests
import time
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NFLPlayByPlayScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # ESPN API endpoints
        self.api_endpoints = {
            'game_summary': 'http://site.api.espn.com/apis/site/v2/sports/football/nfl/summary',
            'game_plays': 'http://site.api.espn.com/apis/site/v2/sports/football/nfl/summary',
            'game_detail': 'https://cdn.espn.com/core/nfl/game'
        }
        
        # Rate limiting
        self.request_delay = 1.0
        
    def get_game_play_by_play(self, game_id: str) -> Optional[Dict]:
        """Fetch comprehensive play-by-play data for a game"""
        try:
            # ESPN Summary API includes play-by-play
            url = f"{self.api_endpoints['game_summary']}?event={game_id}"
            
            logger.info(f"Fetching play-by-play for game {game_id}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            time.sleep(self.request_delay)
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching play-by-play for game {game_id}: {e}")
            return None
    
    def extract_play_data(self, game_data: Dict) -> Dict:
        """Extract and structure play-by-play data"""
        if not game_data:
            return {}
        
        # Initialize structured data
        structured_data = {
            'game_info': {},
            'drives': [],
            'plays': [],
            'formations': [],
            'situational_stats': {
                'red_zone_plays': [],
                'third_down_plays': [],
                'fourth_down_plays': [],
                'two_minute_drill': [],
                'goal_line_plays': []
            },
            'quarter_breakdown': {1: [], 2: [], 3: [], 4: [], 'OT': []},
            'scoring_plays': []
        }
        
        try:
            # Extract game info
            header = game_data.get('header', {})
            structured_data['game_info'] = {
                'game_id': header.get('id', ''),
                'date': header.get('competitions', [{}])[0].get('date', ''),
                'status': header.get('competitions', [{}])[0].get('status', {}),
                'teams': self.extract_team_info(header),
                'venue': header.get('competitions', [{}])[0].get('venue', {})
            }
            
            # Extract drives and plays
            drives_data = game_data.get('drives', {}).get('previous', [])
            
            for drive in drives_data:
                drive_info = self.extract_drive_info(drive)
                structured_data['drives'].append(drive_info)
                
                # Extract plays from this drive
                plays = drive.get('plays', [])
                for play in plays:
                    play_info = self.extract_play_info(play, drive_info)
                    structured_data['plays'].append(play_info)
                    
                    # Categorize plays
                    self.categorize_play(play_info, structured_data)
            
            # Extract current drive if game is active
            current_drive = game_data.get('drives', {}).get('current', {})
            if current_drive:
                drive_info = self.extract_drive_info(current_drive)
                structured_data['drives'].append(drive_info)
                
                plays = current_drive.get('plays', [])
                for play in plays:
                    play_info = self.extract_play_info(play, drive_info)
                    structured_data['plays'].append(play_info)
                    self.categorize_play(play_info, structured_data)
                    
        except Exception as e:
            logger.error(f"Error extracting play data: {e}")
        
        return structured_data
    
    def extract_team_info(self, header: Dict) -> List[Dict]:
        """Extract team information"""
        teams = []
        competitions = header.get('competitions', [])
        
        if competitions:
            competitors = competitions[0].get('competitors', [])
            for competitor in competitors:
                team = competitor.get('team', {})
                teams.append({
                    'id': team.get('id', ''),
                    'name': team.get('displayName', ''),
                    'abbreviation': team.get('abbreviation', ''),
                    'location': team.get('location', ''),
                    'home_away': competitor.get('homeAway', ''),
                    'score': competitor.get('score', '0')
                })
        
        return teams
    
    def extract_drive_info(self, drive: Dict) -> Dict:
        """Extract drive information"""
        return {
            'id': drive.get('id', ''),
            'team': drive.get('team', {}).get('abbreviation', ''),
            'start': {
                'period': drive.get('start', {}).get('period', {}).get('number', 0),
                'clock': drive.get('start', {}).get('clock', {}).get('displayValue', ''),
                'yardline': drive.get('start', {}).get('yardLine', 0),
                'possession_text': drive.get('start', {}).get('possessionText', '')
            },
            'end': {
                'period': drive.get('end', {}).get('period', {}).get('number', 0),
                'clock': drive.get('end', {}).get('clock', {}).get('displayValue', ''),
                'yardline': drive.get('end', {}).get('yardLine', 0),
                'possession_text': drive.get('end', {}).get('possessionText', '')
            },
            'result': drive.get('result', ''),
            'plays_count': len(drive.get('plays', [])),
            'yards': drive.get('yards', 0),
            'time_elapsed': drive.get('timeElapsed', {}).get('displayValue', '')
        }
    
    def extract_play_info(self, play: Dict, drive_info: Dict) -> Dict:
        """Extract individual play information"""
        return {
            'id': play.get('id', ''),
            'sequence_number': play.get('sequenceNumber', 0),
            'type': play.get('type', {}).get('text', ''),
            'text': play.get('text', ''),
            'short_text': play.get('shortText', ''),
            'down': play.get('start', {}).get('down', 0),
            'distance': play.get('start', {}).get('distance', 0),
            'yard_line': play.get('start', {}).get('yardLine', 0),
            'possession_text': play.get('start', {}).get('possessionText', ''),
            'period': play.get('period', {}).get('number', 0),
            'clock': play.get('clock', {}).get('displayValue', ''),
            'team': play.get('start', {}).get('team', {}).get('abbreviation', ''),
            'yards_gained': play.get('statYardage', 0),
            'scoring_play': play.get('scoringPlay', False),
            'drive_id': drive_info.get('id', ''),
            'formation': self.extract_formation_info(play),
            'players_involved': self.extract_players(play)
        }
    
    def extract_formation_info(self, play: Dict) -> Dict:
        """Extract formation information if available"""
        # ESPN doesn't always provide formation data in the main API
        # This can be enhanced based on available data
        formation_info = {}
        
        # Look for formation clues in play text
        play_text = play.get('text', '').lower()
        
        # Common formation keywords
        formations = {
            'shotgun': 'shotgun' in play_text,
            'pistol': 'pistol' in play_text,
            'i_formation': 'i-formation' in play_text or 'i formation' in play_text,
            'singleback': 'singleback' in play_text,
            'empty': 'empty' in play_text,
            'bunch': 'bunch' in play_text,
            'trips': 'trips' in play_text,
            '4_wide': '4 wide' in play_text or 'four wide' in play_text
        }
        
        formation_info = {k: v for k, v in formations.items() if v}
        
        return formation_info
    
    def extract_players(self, play: Dict) -> List[Dict]:
        """Extract player information from play"""
        players = []
        
        # ESPN provides participant information
        participants = play.get('participants', [])
        for participant in participants:
            athlete = participant.get('athlete', {})
            players.append({
                'id': athlete.get('id', ''),
                'name': athlete.get('displayName', ''),
                'jersey': athlete.get('jersey', ''),
                'position': athlete.get('position', {}).get('abbreviation', ''),
                'team': participant.get('team', {}).get('abbreviation', '')
            })
        
        return players
    
    def categorize_play(self, play_info: Dict, structured_data: Dict):
        """Categorize plays into situational buckets"""
        down = play_info.get('down', 0)
        distance = play_info.get('distance', 0)
        yard_line = play_info.get('yard_line', 0)
        period = play_info.get('period', 0)
        clock = play_info.get('clock', '')
        
        # Red zone (within 20 yards of goal)
        if yard_line <= 20 and yard_line > 0:
            structured_data['situational_stats']['red_zone_plays'].append(play_info)
        
        # Goal line (within 5 yards)
        if yard_line <= 5 and yard_line > 0:
            structured_data['situational_stats']['goal_line_plays'].append(play_info)
        
        # Third down
        if down == 3:
            structured_data['situational_stats']['third_down_plays'].append(play_info)
        
        # Fourth down
        if down == 4:
            structured_data['situational_stats']['fourth_down_plays'].append(play_info)
        
        # Two-minute drill (last 2 minutes of each half)
        if self.is_two_minute_situation(clock, period):
            structured_data['situational_stats']['two_minute_drill'].append(play_info)
        
        # Quarter breakdown
        quarter_key = period if period <= 4 else 'OT'
        if quarter_key in structured_data['quarter_breakdown']:
            structured_data['quarter_breakdown'][quarter_key].append(play_info)
        
        # Scoring plays
        if play_info.get('scoring_play', False):
            structured_data['scoring_plays'].append(play_info)
    
    def is_two_minute_situation(self, clock: str, period: int) -> bool:
        """Determine if play is in two-minute situation"""
        try:
            if not clock or ':' not in clock:
                return False
            
            time_parts = clock.split(':')
            minutes = int(time_parts[0])
            
            # Last 2 minutes of 2nd or 4th quarter
            return (period == 2 or period == 4) and minutes < 2
            
        except:
            return False
    
    def process_games_from_date_file(self, date_filename: str) -> bool:
        """Process all games from a date-based JSON file"""
        try:
            # Read the date file
            with open(date_filename, 'r') as f:
                date_data = json.load(f)
            
            games = date_data.get('games', [])
            date_str = date_data.get('date', 'unknown')
            
            logger.info(f"Processing {len(games)} games from {date_str}")
            
            success_count = 0
            
            for game in games:
                game_id = game.get('game_id', '')
                if not game_id:
                    continue
                
                # Get play-by-play data
                play_data = self.get_game_play_by_play(game_id)
                
                if play_data:
                    # Extract structured data
                    structured_data = self.extract_play_data(play_data)
                    
                    # Save play-by-play file
                    pbp_filename = f"playbyplay_{date_str}_{game_id}.json"
                    
                    with open(pbp_filename, 'w') as f:
                        json.dump(structured_data, f, indent=2)
                    
                    logger.info(f"Saved play-by-play: {pbp_filename}")
                    success_count += 1
                
                # Rate limiting
                time.sleep(self.request_delay)
            
            logger.info(f"Successfully processed {success_count}/{len(games)} games from {date_str}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error processing date file {date_filename}: {e}")
            return False
    
    def process_recent_games(self, days: int = 1) -> int:
        """Process play-by-play for recent games"""
        # Find recent date files
        date_files = [f for f in os.listdir('.') if f.endswith('_2025.json') and 'playbyplay' not in f]
        
        # Sort by date (most recent first)
        date_files.sort(reverse=True)
        
        processed_count = 0
        
        for i, date_file in enumerate(date_files[:days]):
            logger.info(f"Processing date file: {date_file}")
            
            if self.process_games_from_date_file(date_file):
                processed_count += 1
        
        return processed_count

def main():
    print("🏈 NFL Play-by-Play Scraper")
    print("=" * 50)
    
    scraper = NFLPlayByPlayScraper()
    
    # Check if we have any date files
    date_files = [f for f in os.listdir('.') if f.endswith('_2025.json') and 'playbyplay' not in f]
    
    if not date_files:
        print("❌ No NFL schedule files found. Run nfl_date_organizer.py first.")
        return
    
    print(f"📄 Found {len(date_files)} date files")
    
    # Process recent games
    days_to_process = int(input("Enter number of recent date files to process (1-10): ") or "1")
    days_to_process = max(1, min(10, days_to_process))
    
    processed = scraper.process_recent_games(days_to_process)
    
    print(f"\n✅ Processed play-by-play data for {processed} game dates")
    print("📊 Play-by-play files include:")
    print("   • Drive information and results")
    print("   • Individual play details with downs/distance")
    print("   • Formation analysis (where available)")
    print("   • Situational stats (red zone, third down, etc.)")
    print("   • Quarter-by-quarter breakdown")
    print("   • Player involvement tracking")

if __name__ == "__main__":
    main()