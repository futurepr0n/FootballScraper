#!/usr/bin/env python3
"""
Load NFL CSV data from ESPN scraping into PostgreSQL database
Properly associates player stats with games and updates game scores
"""

import os
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
import re
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVDatabaseLoader:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
    def map_team_from_stat_category(self, team_abbr, stat_category, filename=None):
        """Map team abbreviation based on stat category for special cases"""
        # Handle ESPN scraper naming issues
        if 'vegas' in stat_category.lower():
            return 'LV'  # Las Vegas Raiders
        elif 'orleans' in stat_category.lower():
            return 'NO'  # New Orleans Saints
        elif 'england' in stat_category.lower():
            return 'NE'  # New England Patriots
        elif 'angeles' in stat_category.lower():
            # Special case: distinguish between LAC (Chargers) and LAR (Rams)
            # If the team_abbr is already LAC, keep it as LAC (Chargers)
            if team_abbr == 'LAC':
                return 'LAC'  # Los Angeles Chargers
            else:
                return 'LAR'  # Los Angeles Rams (for other cases)
        elif 'york' in stat_category.lower():
            # Specific game context handling for NYJ vs NYG
            if filename and '401776263' in filename:
                # NYG @ BUF game - NE_york files are NYG
                return 'NYG'  # New York Giants
            else:
                # Default to NYJ for other york games
                return 'NYJ'  # New York Jets
        elif team_abbr == 'NE' and filename:
            # Handle TEN @ TB game where NE files (without york) should be TEN
            if '401774029' in filename:
                return 'TEN'  # Tennessee Titans
        
        return team_abbr
        
    def get_or_create_team(self, team_abbr):
        """Get team ID, creating if necessary"""
        # Clean team abbreviation
        team_abbr = team_abbr.upper().strip()
        
        # Check if team exists
        self.cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr,))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # Create team if not exists
        logger.warning(f"Creating new team: {team_abbr}")
        self.cursor.execute("""
            INSERT INTO teams (abbreviation, name, conference, division) 
            VALUES (%s, %s, 'TBD', 'TBD') 
            RETURNING id
        """, (team_abbr, team_abbr))
        self.conn.commit()
        return self.cursor.fetchone()['id']
    
    def get_or_create_player(self, player_name, team_id, position):
        """Get player ID, creating if necessary"""
        # Check if player exists
        self.cursor.execute("""
            SELECT id FROM players 
            WHERE name = %s AND team_id = %s
        """, (player_name, team_id))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # Create player if not exists
        logger.info(f"Creating new player: {player_name} ({position})")
        self.cursor.execute("""
            INSERT INTO players (name, team_id, position, jersey_number) 
            VALUES (%s, %s, %s, %s) 
            RETURNING id
        """, (player_name, team_id, position, 0))
        self.conn.commit()
        return self.cursor.fetchone()['id']
    
    def get_or_create_game(self, game_id, season, week, season_type):
        """Get or create game record"""
        # Check if game exists
        self.cursor.execute("SELECT id FROM games WHERE game_id = %s", (game_id,))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # Create placeholder game with current date
        logger.info(f"Creating game record: {game_id}")
        self.cursor.execute("""
            INSERT INTO games (game_id, season, week, season_type, home_team_id, away_team_id, date) 
            VALUES (%s, %s, %s, %s, 1, 1, CURRENT_DATE) 
            RETURNING id
        """, (game_id, season, week, season_type))
        self.conn.commit()
        return self.cursor.fetchone()['id']
    
    def determine_position(self, stat_category):
        """Determine position based on stat category"""
        if 'passing' in stat_category:
            return 'QB'
        elif 'rushing' in stat_category:
            return 'RB'
        elif 'receiving' in stat_category:
            return 'WR'
        elif 'kicking' in stat_category:
            return 'K'
        elif 'punting' in stat_category:
            return 'P'
        elif 'interceptions' in stat_category or 'fumbles' in stat_category:
            return 'DEF'
        else:
            return 'UNKNOWN'
    
    def process_csv_file(self, filepath, season, week, season_type):
        """Process a single CSV file and load into database"""
        filename = os.path.basename(filepath)
        
        # Parse filename: nfl_TEAM_statcategory_week#_date_gameid.csv
        match = re.match(r'nfl_([A-Z]+)_(.+)_week(\d+)_(\d+)_(\d+)\.csv', filename)
        if not match:
            logger.warning(f"Skipping file with unexpected format: {filename}")
            return 0
        
        team_abbr = match.group(1)
        stat_category = match.group(2)
        game_id = match.group(5)
        
        # Map team abbreviation based on stat category (handle ESPN naming issues)
        team_abbr = self.map_team_from_stat_category(team_abbr, stat_category, filename)
        
        # Skip TBD teams
        if team_abbr == 'TBD':
            logger.info(f"Skipping TBD team file: {filename}")
            return 0
        
        # Get IDs
        team_id = self.get_or_create_team(team_abbr)
        game_db_id = self.get_or_create_game(game_id, season, week, season_type)
        
        records_loaded = 0
        
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if not row.get('player'):
                    continue
                
                position = self.determine_position(stat_category)
                player_id = self.get_or_create_player(row['player'], team_id, position)
                
                # Insert or update player_game_stats
                stats = self.parse_stats_from_row(row, stat_category)
                
                self.cursor.execute("""
                    INSERT INTO player_game_stats (
                        player_id, game_id, team_id,
                        passing_attempts, passing_completions, passing_yards, passing_touchdowns,
                        rushing_attempts, rushing_yards, rushing_touchdowns,
                        receiving_yards, receiving_touchdowns
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
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
                        receiving_touchdowns = GREATEST(player_game_stats.receiving_touchdowns, EXCLUDED.receiving_touchdowns)
                """, (
                    player_id, game_db_id, team_id,
                    stats.get('passing_attempts', 0), stats.get('passing_completions', 0), 
                    stats.get('passing_yards', 0), stats.get('passing_touchdowns', 0),
                    stats.get('rushing_attempts', 0), stats.get('rushing_yards', 0), stats.get('rushing_touchdowns', 0),
                    stats.get('receiving_yards', 0), stats.get('receiving_touchdowns', 0)
                ))
                
                records_loaded += 1
        
        self.conn.commit()
        return records_loaded
    
    def parse_stats_from_row(self, row, stat_category):
        """Parse stats from CSV row based on category"""
        stats = {}
        
        if 'passing' in stat_category:
            # Parse C/ATT format
            c_att = row.get('c_att', '0/0')
            parts = c_att.split('/')
            stats['passing_completions'] = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
            stats['passing_attempts'] = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            stats['passing_yards'] = int(row.get('yds', 0) or 0)
            stats['passing_touchdowns'] = int(row.get('td', 0) or 0)
            
        elif 'rushing' in stat_category:
            stats['rushing_attempts'] = int(row.get('car', 0) or 0)
            stats['rushing_yards'] = int(row.get('yds', 0) or 0)
            stats['rushing_touchdowns'] = int(row.get('td', 0) or 0)
            
        elif 'receiving' in stat_category:
            # Note: receptions column doesn't exist in our DB, only yards and TDs
            stats['receiving_yards'] = int(row.get('yds', 0) or 0)
            stats['receiving_touchdowns'] = int(row.get('td', 0) or 0)
        
        return stats
    
    def load_week_data(self, csv_dir, season, week, season_type='regular'):
        """Load all CSV files for a specific week"""
        csv_path = Path(csv_dir)
        pattern = f'nfl_*_week{week}_*.csv'
        
        files = list(csv_path.glob(pattern))
        logger.info(f"Found {len(files)} CSV files for week {week}")
        
        total_records = 0
        for filepath in files:
            records = self.process_csv_file(str(filepath), season, week, season_type)
            total_records += records
            logger.info(f"Loaded {records} records from {filepath.name}")
        
        logger.info(f"Total records loaded for week {week}: {total_records}")
        return total_records

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Load NFL CSV data into database')
    parser.add_argument('--csv-dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--season', type=int, required=True, help='Season year')
    parser.add_argument('--week', type=int, required=True, help='Week number')
    parser.add_argument('--season-type', default='regular', help='Season type (regular/playoffs)')
    
    args = parser.parse_args()
    
    loader = CSVDatabaseLoader()
    loader.load_week_data(args.csv_dir, args.season, args.week, args.season_type)
    
    logger.info("Database loading complete!")

if __name__ == "__main__":
    main()