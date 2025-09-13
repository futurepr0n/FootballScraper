#!/usr/bin/env python3
"""
Historical NFL Data Loader
Processes historical game data and loads directly into PostgreSQL database.
Handles both boxscore stats and play-by-play data.

Usage:
    python historical_data_loader.py --season 2023 --season-type regular
    python historical_data_loader.py --file historical_schedules/2023/regular_week1_2023.txt
    python historical_data_loader.py --directory historical_schedules/2023/
"""

import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import logging
from pathlib import Path
from datetime import datetime
import sys
import os
from typing import Dict, List, Optional, Tuple
import traceback

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from enhanced_nfl_scraper import EnhancedNFLScraper
from nfl_playbyplay_scraper import NFLPlayByPlayScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/historical_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HistoricalDataLoader:
    """Loads historical NFL data directly into PostgreSQL database"""
    
    def __init__(self):
        """Initialize database connection and scrapers"""
        self.db_config = {
            'host': os.getenv('DB_HOST', '192.168.1.23'),
            'database': os.getenv('DB_NAME', 'football_tracker'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'korn5676'),
            'port': int(os.getenv('DB_PORT', 5432))
        }
        
        self.boxscore_scraper = EnhancedNFLScraper()
        self.playbyplay_scraper = NFLPlayByPlayScraper()
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Connected to database")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from database")
    
    def get_or_create_team(self, abbreviation: str) -> int:
        """Get team ID or create if not exists"""
        self.cursor.execute(
            "SELECT id FROM teams WHERE abbreviation = %s",
            (abbreviation,)
        )
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # Create team with placeholder data
        self.cursor.execute(
            """INSERT INTO teams (abbreviation, name, conference) 
               VALUES (%s, %s, %s) RETURNING id""",
            (abbreviation, abbreviation, 'TBD')
        )
        team_id = self.cursor.fetchone()['id']
        self.conn.commit()
        logger.info(f"Created new team: {abbreviation}")
        return team_id
    
    def get_or_create_player(self, name: str, team_id: int, position: str = None) -> int:
        """Get player ID or create if not exists"""
        self.cursor.execute(
            "SELECT id FROM players WHERE name = %s AND team_id = %s",
            (name, team_id)
        )
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # Create player
        self.cursor.execute(
            """INSERT INTO players (name, team_id, position, active) 
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (name, team_id, position or '', True)
        )
        player_id = self.cursor.fetchone()['id']
        self.conn.commit()
        logger.debug(f"Created new player: {name} ({position})")
        return player_id
    
    def create_or_update_game(self, game_info: Dict) -> int:
        """Create or update game record"""
        # Check if game exists
        self.cursor.execute(
            "SELECT id FROM games WHERE game_id = %s",
            (game_info['game_id'],)
        )
        result = self.cursor.fetchone()
        
        if result:
            # Update existing game
            self.cursor.execute(
                """UPDATE games 
                   SET home_team_id = %s, away_team_id = %s, 
                       home_score = %s, away_score = %s,
                       status = %s, completed = %s,
                       has_play_by_play = %s
                   WHERE game_id = %s
                   RETURNING id""",
                (game_info.get('home_team_id'), game_info.get('away_team_id'),
                 game_info.get('home_score'), game_info.get('away_score'),
                 game_info.get('status', 'completed'), 
                 game_info.get('completed', True),
                 game_info.get('has_play_by_play', False),
                 game_info['game_id'])
            )
            return self.cursor.fetchone()['id']
        else:
            # Create new game
            self.cursor.execute(
                """INSERT INTO games 
                   (game_id, date, week, season, season_type,
                    home_team_id, away_team_id, home_score, away_score,
                    status, completed, has_play_by_play)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (game_info['game_id'], game_info.get('date', datetime.now().date()),
                 game_info['week'], game_info['season'], game_info['season_type'],
                 game_info.get('home_team_id'), game_info.get('away_team_id'),
                 game_info.get('home_score'), game_info.get('away_score'),
                 game_info.get('status', 'completed'), 
                 game_info.get('completed', True),
                 game_info.get('has_play_by_play', False))
            )
            return self.cursor.fetchone()['id']
    
    def load_player_stats(self, game_id: int, team_id: int, stats_data: Dict):
        """Load player statistics for a game"""
        for stat_type, players in stats_data.items():
            if not players:
                continue
                
            for player_data in players:
                player_name = player_data.get('player', player_data.get('Player'))
                if not player_name or player_name == 'Team':
                    continue
                
                # Determine position based on stat type
                position_map = {
                    'passing': 'QB',
                    'rushing': 'RB',
                    'receiving': 'WR',
                    'kicking': 'K',
                    'punting': 'P'
                }
                position = position_map.get(stat_type, '')
                
                # Get or create player
                player_id = self.get_or_create_player(player_name, team_id, position)
                
                # Check if stats exist
                self.cursor.execute(
                    """SELECT id FROM player_game_stats 
                       WHERE player_id = %s AND game_id = %s""",
                    (player_id, game_id)
                )
                existing = self.cursor.fetchone()
                
                # Prepare stats based on type
                stats_dict = self.prepare_stats_dict(stat_type, player_data)
                
                if existing:
                    # Update existing stats
                    self.update_player_stats(existing['id'], stats_dict)
                else:
                    # Insert new stats
                    self.insert_player_stats(player_id, game_id, team_id, stats_dict)
    
    def prepare_stats_dict(self, stat_type: str, player_data: Dict) -> Dict:
        """Prepare statistics dictionary from raw data"""
        stats = {}
        
        if stat_type == 'passing':
            # Parse C/ATT format
            c_att = player_data.get('c_att', player_data.get('C/ATT', '0/0'))
            if '/' in c_att:
                comp, att = c_att.split('/')
                stats['passing_completions'] = int(comp) if comp.isdigit() else 0
                stats['passing_attempts'] = int(att) if att.isdigit() else 0
            
            stats['passing_yards'] = self.parse_int(player_data.get('yds', player_data.get('YDS')))
            stats['passing_touchdowns'] = self.parse_int(player_data.get('td', player_data.get('TD')))
            stats['interceptions'] = self.parse_int(player_data.get('int', player_data.get('INT')))
            
        elif stat_type == 'rushing':
            stats['rushing_attempts'] = self.parse_int(player_data.get('car', player_data.get('CAR')))
            stats['rushing_yards'] = self.parse_int(player_data.get('yds', player_data.get('YDS')))
            stats['rushing_touchdowns'] = self.parse_int(player_data.get('td', player_data.get('TD')))
            stats['longest_rush'] = self.parse_int(player_data.get('long', player_data.get('LONG')))
            
        elif stat_type == 'receiving':
            stats['receptions'] = self.parse_int(player_data.get('rec', player_data.get('REC')))
            stats['targets'] = self.parse_int(player_data.get('tgts', player_data.get('TGTS')))
            stats['receiving_yards'] = self.parse_int(player_data.get('yds', player_data.get('YDS')))
            stats['receiving_touchdowns'] = self.parse_int(player_data.get('td', player_data.get('TD')))
            stats['longest_reception'] = self.parse_int(player_data.get('long', player_data.get('LONG')))
            
        elif stat_type == 'kicking':
            # Parse FG format
            fg = player_data.get('fg', player_data.get('FG', '0/0'))
            if '/' in fg:
                made, att = fg.split('/')
                stats['field_goals_made'] = int(made) if made.isdigit() else 0
                stats['field_goals_attempted'] = int(att) if att.isdigit() else 0
            
            # Parse XP format
            xp = player_data.get('xp', player_data.get('XP', '0/0'))
            if '/' in xp:
                made, att = xp.split('/')
                stats['extra_points_made'] = int(made) if made.isdigit() else 0
                stats['extra_points_attempted'] = int(att) if att.isdigit() else 0
            
        elif stat_type == 'punting':
            stats['punts'] = self.parse_int(player_data.get('punts', player_data.get('PUNTS')))
            stats['punt_yards'] = self.parse_int(player_data.get('yds', player_data.get('YDS')))
            stats['longest_punt'] = self.parse_int(player_data.get('long', player_data.get('LONG')))
        
        return stats
    
    def parse_int(self, value) -> int:
        """Safely parse integer from various formats"""
        if value is None:
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            # Remove any non-digit characters except minus sign
            cleaned = ''.join(c for c in value if c.isdigit() or c == '-')
            return int(cleaned) if cleaned else 0
        return 0
    
    def insert_player_stats(self, player_id: int, game_id: int, team_id: int, stats: Dict):
        """Insert player statistics"""
        # Build dynamic insert query
        fields = ['player_id', 'game_id', 'team_id'] + list(stats.keys())
        values = [player_id, game_id, team_id] + list(stats.values())
        placeholders = ', '.join(['%s'] * len(fields))
        
        if stats:
            update_clause = f"DO UPDATE SET {', '.join([f'{k} = EXCLUDED.{k}' for k in stats.keys()])}"
        else:
            update_clause = "DO NOTHING"
        
        query = f"""
            INSERT INTO player_game_stats ({', '.join(fields)})
            VALUES ({placeholders})
            ON CONFLICT (player_id, game_id) {update_clause}
        """
        
        self.cursor.execute(query, values)
    
    def update_player_stats(self, stats_id: int, stats: Dict):
        """Update existing player statistics"""
        if not stats:
            return
            
        set_clause = ', '.join([f"{k} = %s" for k in stats.keys()])
        values = list(stats.values()) + [stats_id]
        
        query = f"""
            UPDATE player_game_stats
            SET {set_clause}
            WHERE id = %s
        """
        
        self.cursor.execute(query, values)
    
    def load_play_by_play(self, game_info: Dict, game_db_id: int):
        """Load play-by-play data for a game"""
        try:
            # Fetch play-by-play data
            pbp_data = self.playbyplay_scraper.fetch_game_plays(game_info['game_id'])
            
            if not pbp_data or 'drives' not in pbp_data:
                logger.warning(f"No play-by-play data for game {game_info['game_id']}")
                return
            
            for drive_num, drive_data in enumerate(pbp_data['drives'], 1):
                # Create drive record
                drive_id = self.create_drive(game_db_id, drive_num, drive_data)
                
                # Load plays for this drive
                if 'plays' in drive_data:
                    for play_num, play_data in enumerate(drive_data['plays'], 1):
                        self.create_play(game_db_id, drive_id, play_num, play_data)
            
            # Mark game as having play-by-play data
            self.cursor.execute(
                "UPDATE games SET has_play_by_play = TRUE WHERE id = %s",
                (game_db_id,)
            )
            
            logger.info(f"Loaded play-by-play for game {game_info['game_id']}")
            
        except Exception as e:
            logger.error(f"Error loading play-by-play for game {game_info['game_id']}: {e}")
    
    def create_drive(self, game_id: int, drive_num: int, drive_data: Dict) -> int:
        """Create drive record"""
        self.cursor.execute(
            """INSERT INTO drives 
               (game_id, drive_number, quarter, start_time, end_time,
                start_field_position, end_field_position, plays_count,
                yards_gained, result, scoring_play, points_scored)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (game_id, drive_number) DO UPDATE SET
                plays_count = EXCLUDED.plays_count,
                yards_gained = EXCLUDED.yards_gained
               RETURNING id""",
            (game_id, drive_num, 
             drive_data.get('quarter'), drive_data.get('start_time'),
             drive_data.get('end_time'), drive_data.get('start_position'),
             drive_data.get('end_position'), drive_data.get('plays_count', 0),
             drive_data.get('yards', 0), drive_data.get('result'),
             drive_data.get('scoring', False), drive_data.get('points', 0))
        )
        return self.cursor.fetchone()['id']
    
    def create_play(self, game_id: int, drive_id: int, play_num: int, play_data: Dict):
        """Create play record"""
        self.cursor.execute(
            """INSERT INTO plays 
               (game_id, drive_id, play_number, quarter, time_remaining,
                down, distance, yard_line, play_type, play_description,
                yards_gained, first_down, touchdown, scoring_play, points_scored)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (game_id, play_number) DO UPDATE SET
                yards_gained = EXCLUDED.yards_gained""",
            (game_id, drive_id, play_num,
             play_data.get('quarter'), play_data.get('time'),
             play_data.get('down'), play_data.get('distance'),
             play_data.get('yard_line'), play_data.get('play_type'),
             play_data.get('description', '')[:1000],  # Truncate long descriptions
             play_data.get('yards', 0), play_data.get('first_down', False),
             play_data.get('touchdown', False), play_data.get('scoring', False),
             play_data.get('points', 0))
        )
    
    def process_game_url(self, game_url: str, game_info: Dict) -> bool:
        """Process a single game URL"""
        try:
            logger.info(f"Processing game: {game_url}")
            
            # Extract game ID from URL
            import re
            game_id_match = re.search(r'gameId/(\d+)', game_url)
            if not game_id_match:
                logger.error(f"Could not extract game ID from URL: {game_url}")
                return False
            
            game_info['game_id'] = game_id_match.group(1)
            game_info['espn_game_id'] = game_info['game_id']
            
            # Extract team names from URL (format: /team1-team2)
            teams_match = re.search(r'/([^/]+)-([^/]+)$', game_url)
            if teams_match:
                game_info['away_team'] = teams_match.group(1).upper()
                game_info['home_team'] = teams_match.group(2).upper()
            else:
                # Default values if not found
                game_info['away_team'] = "AWAY"
                game_info['home_team'] = "HOME"
            
            # Fetch boxscore data
            all_teams_data = self.boxscore_scraper.scrape_game_boxscore(game_info)
            
            if not all_teams_data:
                logger.warning(f"No boxscore data for game {game_info['game_id']}")
                return False
            
            # Process each team's data
            home_team_id = None
            away_team_id = None
            
            for team_abbr, team_data in all_teams_data.items():
                # Get or create team
                team_id = self.get_or_create_team(team_abbr)
                
                # Determine if home or away (simplified logic)
                if home_team_id is None:
                    home_team_id = team_id
                else:
                    away_team_id = team_id
                
                # Update game info
                game_info['home_team_id'] = home_team_id
                game_info['away_team_id'] = away_team_id
            
            # Create or update game record
            game_db_id = self.create_or_update_game(game_info)
            
            # Load player statistics
            for team_abbr, team_data in all_teams_data.items():
                team_id = self.get_or_create_team(team_abbr)
                self.load_player_stats(game_db_id, team_id, team_data)
            
            # Always load play-by-play for historical completeness
            try:
                self.load_play_by_play(game_info, game_db_id)
                logger.info(f"Loaded play-by-play for game {game_info['game_id']}")
            except Exception as e:
                logger.warning(f"Could not load play-by-play for {game_info['game_id']}: {e}")
            
            self.conn.commit()
            logger.info(f"Successfully loaded game {game_info['game_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing game {game_url}: {e}")
            logger.error(traceback.format_exc())
            self.conn.rollback()
            return False
    
    def process_file(self, filepath: str, load_playbyplay: bool = True) -> Dict:
        """Process a file containing game URLs"""
        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return {'success': False, 'error': 'File not found'}
        
        # Extract metadata from filename
        filename = filepath.stem
        parts = filename.split('_')
        
        season_type = 'regular'
        week = 1
        year = 2024
        
        for i, part in enumerate(parts):
            if part == 'week' and i + 1 < len(parts):
                week = int(''.join(c for c in parts[i+1] if c.isdigit()))
            elif part.isdigit() and len(part) == 4:
                year = int(part)
            elif part in ['preseason', 'regular', 'playoffs']:
                season_type = part
        
        # Read game URLs
        game_urls = []
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and line.startswith('http') and 'gameId' in line:
                    game_urls.append(line)
        
        logger.info(f"Found {len(game_urls)} games in {filepath}")
        
        # Process each game
        successful = 0
        failed = 0
        
        for game_url in game_urls:
            game_info = {
                'season': year,
                'season_type': season_type,
                'week': week,
                'load_playbyplay': True  # Always load play-by-play
            }
            
            if self.process_game_url(game_url, game_info):
                successful += 1
            else:
                failed += 1
        
        return {
            'success': True,
            'file': str(filepath),
            'total_games': len(game_urls),
            'successful': successful,
            'failed': failed
        }
    
    def process_directory(self, directory: str, load_playbyplay: bool = False) -> Dict:
        """Process all game files in a directory"""
        dir_path = Path(directory)
        if not dir_path.exists():
            logger.error(f"Directory not found: {directory}")
            return {'success': False, 'error': 'Directory not found'}
        
        # Find all .txt files
        game_files = list(dir_path.glob('*.txt'))
        logger.info(f"Found {len(game_files)} game files in {directory}")
        
        results = []
        for game_file in sorted(game_files):
            result = self.process_file(str(game_file), load_playbyplay)
            results.append(result)
        
        total_games = sum(r.get('total_games', 0) for r in results)
        total_successful = sum(r.get('successful', 0) for r in results)
        total_failed = sum(r.get('failed', 0) for r in results)
        
        return {
            'success': True,
            'directory': str(directory),
            'files_processed': len(game_files),
            'total_games': total_games,
            'successful': total_successful,
            'failed': total_failed,
            'results': results
        }

def main():
    parser = argparse.ArgumentParser(description='Load historical NFL data into database')
    parser.add_argument('--file', type=str, help='Process a single game file')
    parser.add_argument('--directory', type=str, help='Process all files in directory')
    parser.add_argument('--season', type=int, help='Season year')
    parser.add_argument('--season-type', type=str, default='regular',
                       choices=['preseason', 'regular', 'playoffs'],
                       help='Type of season')
    parser.add_argument('--week', type=int, help='Week number')
    parser.add_argument('--playbyplay', action='store_true',
                       help='Also load play-by-play data')
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = HistoricalDataLoader()
    
    try:
        loader.connect_db()
        
        if args.file:
            # Process single file
            result = loader.process_file(args.file, args.playbyplay)
            print(json.dumps(result, indent=2))
            
        elif args.directory:
            # Process directory
            result = loader.process_directory(args.directory, args.playbyplay)
            print(json.dumps(result, indent=2))
            
        else:
            parser.error('Either --file or --directory required')
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    finally:
        loader.disconnect_db()

if __name__ == "__main__":
    main()