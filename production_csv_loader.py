#!/usr/bin/env python3
"""
Production CSV Database Loader
Loads clean boxscore CSV files into PostgreSQL database with proper team mapping.

Features:
- Handles new BOXSCORE_CSV format
- Uses official NFL team abbreviations  
- Transaction-based loading
- Comprehensive error handling
"""

import os
import csv
import re
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('production_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Official NFL team abbreviations for validation
OFFICIAL_NFL_TEAMS = {
    "BUF", "MIA", "NE", "NYJ",  # AFC East
    "BAL", "CIN", "CLE", "PIT",  # AFC North  
    "HOU", "IND", "JAX", "TEN",  # AFC South
    "DEN", "KC", "LAC", "LV",    # AFC West
    "DAL", "NYG", "PHI", "WSH",  # NFC East
    "CHI", "DET", "GB", "MIN",   # NFC North
    "ATL", "CAR", "NO", "TB",    # NFC South
    "ARI", "LAR", "SF", "SEA"    # NFC West
}

class ProductionCSVLoader:
    def __init__(self, db_config: Dict = None):
        """Initialize the production CSV loader"""
        if db_config is None:
            db_config = {
                'host': os.getenv('DB_HOST', '192.168.1.23'),
                'database': os.getenv('DB_NAME', 'football_tracker'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'korn5676'),
                'port': int(os.getenv('DB_PORT', 5432))
            }
        
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.stats_loaded = 0
        self.errors = []
        
        # Connect to database
        self._connect()
    
    def _connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info(f"Connected to database: {self.db_config['database']}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def validate_team_abbreviation(self, team_abbr: str) -> bool:
        """Validate team abbreviation against official NFL teams"""
        return team_abbr.upper() in OFFICIAL_NFL_TEAMS
    
    def parse_csv_filename(self, filename: str) -> Optional[Dict]:
        """
        Parse production CSV filename format:
        nfl_{TEAM}_{STAT_CATEGORY}_week{WEEK}_{DATE}_{GAME_ID}.csv
        """
        pattern = r'nfl_([A-Z]+)_([a-z_]+)_week(\d+)_(\d{8}|UNKNOWN_DATE)_(\d+)\.csv'
        match = re.match(pattern, filename)
        
        if not match:
            logger.warning(f"Filename does not match expected format: {filename}")
            return None
        
        team_abbr, stat_category, week, game_date, game_id = match.groups()
        
        # Validate team abbreviation
        if not self.validate_team_abbreviation(team_abbr):
            logger.warning(f"Invalid team abbreviation in filename: {team_abbr} ({filename})")
        
        return {
            'team_abbr': team_abbr,
            'stat_category': stat_category,
            'week': int(week),
            'game_date': game_date,
            'game_id': game_id,
            'filename': filename
        }
    
    def get_or_create_team(self, team_abbr: str) -> Optional[int]:
        """Get team ID, creating if necessary with validation"""
        team_abbr = team_abbr.upper().strip()
        
        # Validate against official teams
        if not self.validate_team_abbreviation(team_abbr):
            logger.error(f"Invalid team abbreviation: {team_abbr}")
            return None
        
        try:
            # Check if team exists
            self.cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr,))
            result = self.cursor.fetchone()
            
            if result:
                return result['id']
            
            # Create team if not exists
            logger.info(f"Creating new team: {team_abbr}")
            self.cursor.execute("""
                INSERT INTO teams (abbreviation, name, conference, division) 
                VALUES (%s, %s, 'TBD', 'TBD') 
                RETURNING id
            """, (team_abbr, team_abbr))
            
            return self.cursor.fetchone()['id']
            
        except Exception as e:
            logger.error(f"Error getting/creating team {team_abbr}: {e}")
            return None
    
    def get_or_create_player(self, player_name: str, team_id: int, position: str = None) -> Optional[int]:
        """Get player ID, creating if necessary"""
        try:
            # Check if player exists
            self.cursor.execute("""
                SELECT id FROM players 
                WHERE name = %s AND team_id = %s
            """, (player_name, team_id))
            result = self.cursor.fetchone()
            
            if result:
                return result['id']
            
            # Determine position from stat category if not provided
            if not position:
                position = 'UNK'
            
            # Create player if not exists
            logger.debug(f"Creating new player: {player_name} ({position})")
            self.cursor.execute("""
                INSERT INTO players (name, team_id, position, jersey_number) 
                VALUES (%s, %s, %s, %s) 
                RETURNING id
            """, (player_name, team_id, position, 0))
            
            return self.cursor.fetchone()['id']
            
        except Exception as e:
            logger.error(f"Error getting/creating player {player_name}: {e}")
            return None
    
    def get_or_create_game(self, game_id: str, season: int, week: int, season_type: str = 'regular') -> Optional[int]:
        """Get or create game record and return the database ID"""
        try:
            # Check if game exists by ESPN game_id
            self.cursor.execute("SELECT id FROM games WHERE game_id = %s", (game_id,))
            result = self.cursor.fetchone()

            if result:
                return result['id']

            # Determine teams from CSV files for this game
            teams_in_game = self.determine_teams_for_game(game_id, week)
            if len(teams_in_game) != 2:
                logger.error(f"Could not determine exactly 2 teams for game {game_id}: found {teams_in_game}")
                # Fallback to placeholder if we can't determine teams
                home_team_id = 1
                away_team_id = 1
            else:
                # Get team IDs
                teams_list = list(teams_in_game)
                home_team_id = self.get_or_create_team(teams_list[0])
                away_team_id = self.get_or_create_team(teams_list[1])

                if not home_team_id or not away_team_id:
                    logger.error(f"Could not get team IDs for {teams_list}")
                    home_team_id = 1
                    away_team_id = 1

            # Create game with actual teams
            logger.debug(f"Creating game record: {game_id} with teams {home_team_id} vs {away_team_id}")
            self.cursor.execute("""
                INSERT INTO games (game_id, season, week, season_type, home_team_id, away_team_id, date)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE)
                RETURNING id
            """, (game_id, season, week, season_type, home_team_id, away_team_id))

            return self.cursor.fetchone()['id']

        except Exception as e:
            logger.error(f"Error getting/creating game {game_id}: {e}")
            return None

    def determine_teams_for_game(self, game_id: str, week: int) -> set:
        """Determine which teams play in a game by scanning CSV files"""
        teams = set()

        # Look for CSV files with this game_id
        if hasattr(self, 'boxscore_dir'):
            boxscore_path = Path(self.boxscore_dir)
            pattern = f'nfl_*_week{week}_*_{game_id}.csv'

            for csv_file in boxscore_path.glob(pattern):
                file_info = self.parse_csv_filename(csv_file.name)
                if file_info and file_info['game_id'] == game_id:
                    teams.add(file_info['team_abbr'])

        return teams
    
    def determine_position_from_stats(self, stat_category: str) -> str:
        """Determine player position based on stat category"""
        position_mapping = {
            'passing': 'QB',
            'rushing': 'RB', 
            'receiving': 'WR',
            'kicking': 'K',
            'punting': 'P',
            'kick_returns': 'WR',
            'punt_returns': 'WR',
            'interceptions': 'DEF',
            'fumbles': 'DEF',
            'defensive': 'DEF'
        }
        return position_mapping.get(stat_category, 'UNK')
    
    def parse_stats_from_row(self, row: Dict, stat_category: str) -> Dict:
        """Parse stats from CSV row based on category"""
        stats = {}
        
        # Initialize all fields to 0
        stat_fields = [
            'passing_attempts', 'passing_completions', 'passing_yards', 'passing_touchdowns',
            'rushing_attempts', 'rushing_yards', 'rushing_touchdowns',
            'receiving_yards', 'receiving_touchdowns', 'receptions', 'targets'
        ]
        
        for field in stat_fields:
            stats[field] = 0
        
        try:
            if stat_category == 'passing':
                # Parse C/ATT format
                c_att = row.get('c/att', '0/0')
                if '/' in c_att:
                    parts = c_att.split('/')
                    stats['passing_completions'] = int(parts[0]) if parts[0].isdigit() else 0
                    stats['passing_attempts'] = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
                
                stats['passing_yards'] = int(row.get('yds', 0) or 0)
                stats['passing_touchdowns'] = int(row.get('td', 0) or 0)
                
            elif stat_category == 'rushing':
                stats['rushing_attempts'] = int(row.get('car', 0) or 0)
                stats['rushing_yards'] = int(row.get('yds', 0) or 0)
                stats['rushing_touchdowns'] = int(row.get('td', 0) or 0)
                
            elif stat_category == 'receiving':
                stats['receiving_yards'] = int(row.get('yds', 0) or 0)
                stats['receiving_touchdowns'] = int(row.get('td', 0) or 0)
                stats['receptions'] = int(row.get('rec', 0) or 0)
                stats['targets'] = int(row.get('tgts', 0) or 0)

            elif stat_category == 'kicking':
                # Parse field goals: "2/3" format
                fg_str = row.get('fg', '0/0')
                if '/' in fg_str:
                    fg_parts = fg_str.split('/')
                    stats['field_goals_made'] = int(fg_parts[0]) if fg_parts[0].isdigit() else 0
                    stats['field_goals_attempted'] = int(fg_parts[1]) if len(fg_parts) > 1 and fg_parts[1].isdigit() else 0
                else:
                    stats['field_goals_made'] = 0
                    stats['field_goals_attempted'] = 0

                # Parse extra points: "2/2" format
                xp_str = row.get('xp', '0/0')
                if '/' in xp_str:
                    xp_parts = xp_str.split('/')
                    stats['extra_points_made'] = int(xp_parts[0]) if xp_parts[0].isdigit() else 0
                    stats['extra_points_attempted'] = int(xp_parts[1]) if len(xp_parts) > 1 and xp_parts[1].isdigit() else 0
                else:
                    stats['extra_points_made'] = 0
                    stats['extra_points_attempted'] = 0

                stats['kicking_points'] = int(row.get('pts', 0) or 0)
                stats['longest_field_goal'] = int(row.get('long', 0) or 0)

        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing stats from row: {e}, row: {row}")
        
        return stats
    
    def process_csv_file(self, csv_path: Path, season: int = 2025) -> int:
        """Process a single CSV file and load into database"""
        filename = csv_path.name
        
        # Parse filename
        file_info = self.parse_csv_filename(filename)
        if not file_info:
            self.errors.append(f"Could not parse filename: {filename}")
            return 0
        
        team_abbr = file_info['team_abbr']
        stat_category = file_info['stat_category']
        week = file_info['week']
        game_id = file_info['game_id']
        
        logger.info(f"Processing {filename}: {team_abbr} {stat_category} Week {week}")
        
        try:
            # Get IDs
            team_id = self.get_or_create_team(team_abbr)
            if not team_id:
                self.errors.append(f"Could not get team ID for {team_abbr}")
                return 0

            # IMPORTANT: Get the database internal ID for the game based on ESPN game_id
            game_db_id = self.get_or_create_game(game_id, season, week)
            if not game_db_id:
                self.errors.append(f"Could not get game ID for {game_id}")
                return 0
            
            # Read CSV file
            records_loaded = 0
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    if not row.get('player'):
                        continue

                    # Skip team summary rows
                    if row.get('player', '').lower() == 'team':
                        continue
                    
                    # Get player
                    position = self.determine_position_from_stats(stat_category)
                    player_id = self.get_or_create_player(row['player'], team_id, position)
                    if not player_id:
                        continue
                    
                    # Parse stats
                    stats = self.parse_stats_from_row(row, stat_category)
                    
                    # Insert/update player_game_stats using the database internal game ID
                    self.cursor.execute("""
                        INSERT INTO player_game_stats (
                            player_id, game_id, team_id,
                            passing_attempts, passing_completions, passing_yards, passing_touchdowns,
                            rushing_attempts, rushing_yards, rushing_touchdowns,
                            receiving_yards, receiving_touchdowns, receptions, targets,
                            field_goals_made, field_goals_attempted, extra_points_made, extra_points_attempted,
                            kicking_points, longest_field_goal
                        ) VALUES (
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s
                        )
                        ON CONFLICT (player_id, game_id) DO UPDATE SET
                            passing_attempts = GREATEST(player_game_stats.passing_attempts, EXCLUDED.passing_attempts),
                            passing_completions = GREATEST(player_game_stats.passing_completions, EXCLUDED.passing_completions),
                            passing_yards = GREATEST(player_game_stats.passing_yards, EXCLUDED.passing_yards),
                            passing_touchdowns = GREATEST(player_game_stats.passing_touchdowns, EXCLUDED.passing_touchdowns),
                            rushing_attempts = GREATEST(player_game_stats.rushing_attempts, EXCLUDED.rushing_attempts),
                            rushing_yards = GREATEST(player_game_stats.rushing_yards, EXCLUDED.rushing_yards),
                            rushing_touchdowns = GREATEST(player_game_stats.rushing_touchdowns, EXCLUDED.rushing_touchdowns),
                            receiving_yards = GREATEST(player_game_stats.receiving_yards, EXCLUDED.receiving_yards),
                            receiving_touchdowns = GREATEST(player_game_stats.receiving_touchdowns, EXCLUDED.receiving_touchdowns),
                            receptions = GREATEST(player_game_stats.receptions, EXCLUDED.receptions),
                            targets = GREATEST(player_game_stats.targets, EXCLUDED.targets),
                            field_goals_made = CASE WHEN EXCLUDED.field_goals_made > 0 OR EXCLUDED.field_goals_attempted > 0 THEN EXCLUDED.field_goals_made ELSE player_game_stats.field_goals_made END,
                            field_goals_attempted = CASE WHEN EXCLUDED.field_goals_attempted > 0 THEN EXCLUDED.field_goals_attempted ELSE player_game_stats.field_goals_attempted END,
                            extra_points_made = CASE WHEN EXCLUDED.extra_points_made > 0 OR EXCLUDED.extra_points_attempted > 0 THEN EXCLUDED.extra_points_made ELSE player_game_stats.extra_points_made END,
                            extra_points_attempted = CASE WHEN EXCLUDED.extra_points_attempted > 0 THEN EXCLUDED.extra_points_attempted ELSE player_game_stats.extra_points_attempted END,
                            kicking_points = CASE WHEN EXCLUDED.kicking_points > 0 THEN EXCLUDED.kicking_points ELSE player_game_stats.kicking_points END,
                            longest_field_goal = CASE WHEN EXCLUDED.longest_field_goal > 0 THEN EXCLUDED.longest_field_goal ELSE player_game_stats.longest_field_goal END
                    """, (
                        player_id, game_db_id, team_id,  # game_db_id is the internal database ID
                        stats.get('passing_attempts', 0), stats.get('passing_completions', 0),
                        stats.get('passing_yards', 0), stats.get('passing_touchdowns', 0),
                        stats.get('rushing_attempts', 0), stats.get('rushing_yards', 0), stats.get('rushing_touchdowns', 0),
                        stats.get('receiving_yards', 0), stats.get('receiving_touchdowns', 0),
                        stats.get('receptions', 0), stats.get('targets', 0),
                        stats.get('field_goals_made', 0), stats.get('field_goals_attempted', 0),
                        stats.get('extra_points_made', 0), stats.get('extra_points_attempted', 0),
                        stats.get('kicking_points', 0), stats.get('longest_field_goal', 0)
                    ))
                    
                    records_loaded += 1
            
            return records_loaded
            
        except Exception as e:
            logger.error(f"Error processing CSV file {filename}: {e}")
            self.errors.append(f"Error processing {filename}: {e}")
            return 0
    
    def load_boxscore_directory(self, boxscore_dir: str, season: int = 2025, week: int = None) -> Dict:
        """Load all CSV files from BOXSCORE_CSV directory"""
        boxscore_path = Path(boxscore_dir)
        self.boxscore_dir = boxscore_dir  # Store for use in determine_teams_for_game

        if not boxscore_path.exists():
            raise FileNotFoundError(f"BOXSCORE_CSV directory not found: {boxscore_path}")
        
        # Find CSV files
        pattern = 'nfl_*.csv'
        if week is not None:
            pattern = f'nfl_*_week{week}_*.csv'
        
        csv_files = list(boxscore_path.glob(pattern))
        logger.info(f"Found {len(csv_files)} CSV files in {boxscore_path}")
        
        if not csv_files:
            return {
                'success': False,
                'message': 'No CSV files found',
                'files_processed': 0,
                'stats_loaded': 0,
                'errors': []
            }
        
        # Process files in transaction
        total_stats = 0
        processed_files = 0
        
        try:
            for csv_file in csv_files:
                records = self.process_csv_file(csv_file, season)
                if records > 0:
                    total_stats += records
                    processed_files += 1
                    logger.info(f"Loaded {records} records from {csv_file.name}")
            
            # Commit transaction
            self.conn.commit()
            
            logger.info(f"Successfully loaded {total_stats} total records from {processed_files} files")
            
            return {
                'success': True,
                'files_processed': processed_files,
                'stats_loaded': total_stats,
                'errors': self.errors,
                'season': season,
                'week': week
            }
            
        except Exception as e:
            # Rollback on error
            self.conn.rollback()
            logger.error(f"Transaction failed, rolling back: {e}")
            
            return {
                'success': False,
                'message': f"Transaction failed: {e}",
                'files_processed': processed_files,
                'stats_loaded': 0,
                'errors': self.errors + [str(e)]
            }
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

def main():
    """Main entry point for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Production CSV Database Loader')
    parser.add_argument('--boxscore-dir', required=True, help='BOXSCORE_CSV directory path')
    parser.add_argument('--season', type=int, default=2025, help='Season year')
    parser.add_argument('--week', type=int, help='Specific week to load (optional)')
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = ProductionCSVLoader()
    
    try:
        # Load data
        result = loader.load_boxscore_directory(args.boxscore_dir, args.season, args.week)
        
        if result['success']:
            print(f"\n✅ Successfully loaded CSV data")
            print(f"   Season: {result['season']}")
            if result.get('week'):
                print(f"   Week: {result['week']}")
            print(f"   Files processed: {result['files_processed']}")
            print(f"   Stats loaded: {result['stats_loaded']}")
            
            if result['errors']:
                print(f"   Warnings: {len(result['errors'])}")
                for error in result['errors'][:5]:  # Show first 5 errors
                    print(f"     • {error}")
                if len(result['errors']) > 5:
                    print(f"     ... and {len(result['errors']) - 5} more")
        else:
            print(f"\n❌ Failed to load CSV data: {result['message']}")
            if result['errors']:
                print("   Errors:")
                for error in result['errors']:
                    print(f"     • {error}")
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Fatal error: {e}")
    
    finally:
        loader.close()

if __name__ == "__main__":
    main()