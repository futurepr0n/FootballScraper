#!/usr/bin/env python3
"""
Fixed CSV Database Loader for NFL Data
Properly handles team assignments by looking up actual matchups
"""

import os
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import requests
from pathlib import Path
import logging
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FixedCSVLoader:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.game_cache = {}  # Cache game info to avoid repeated ESPN calls
        
    def get_or_create_team(self, team_abbr):
        """Get team ID, creating if necessary"""
        team_abbr = team_abbr.upper().strip()
        
        self.cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (team_abbr,))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        logger.warning(f"Creating new team: {team_abbr}")
        self.cursor.execute("""
            INSERT INTO teams (abbreviation, name, conference, division) 
            VALUES (%s, %s, 'AFC', 'North') 
            RETURNING id
        """, (team_abbr, team_abbr))
        self.conn.commit()
        return self.cursor.fetchone()['id']
    
    def get_game_matchup_from_espn(self, game_id):
        """Get the actual team matchup from ESPN using game ID"""
        if game_id in self.game_cache:
            return self.game_cache[game_id]
            
        try:
            url = f"https://www.espn.com/nfl/game/_/gameId/{game_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                content = response.text
                
                # Look for team abbreviations in the page
                import re
                # Find team names/abbreviations - this is a simplified approach
                # A more robust solution would parse the JSON data embedded in the page
                team_pattern = r'teams":\[{"team":{"abbreviation":"([A-Z]+)".*?"abbreviation":"([A-Z]+)"'
                match = re.search(team_pattern, content)
                
                if match:
                    away_team = match.group(1)
                    home_team = match.group(2)
                    
                    result = {
                        'home_team': home_team,
                        'away_team': away_team
                    }
                    self.game_cache[game_id] = result
                    logger.info(f"Found matchup for {game_id}: {away_team} @ {home_team}")
                    time.sleep(2)  # Rate limiting
                    return result
                    
        except Exception as e:
            logger.warning(f"Could not fetch matchup for game {game_id}: {e}")
            
        # Fallback: use placeholder teams
        result = {'home_team': 'UNK', 'away_team': 'UNK'}
        self.game_cache[game_id] = result
        return result
    
    def get_or_create_player(self, player_name, team_id, position):
        """Get player ID, creating if necessary"""
        self.cursor.execute("""
            SELECT id FROM players 
            WHERE name = %s AND team_id = %s
        """, (player_name, team_id))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        logger.info(f"Creating new player: {player_name} ({position})")
        self.cursor.execute("""
            INSERT INTO players (name, team_id, position, jersey_number) 
            VALUES (%s, %s, %s, %s) 
            RETURNING id
        """, (player_name, team_id, position, 0))
        self.conn.commit()
        return self.cursor.fetchone()['id']
    
    def get_or_create_game(self, game_id, season, week, season_type, home_team_abbr, away_team_abbr):
        """Get or create game record with proper team assignments"""
        # Check if game exists
        self.cursor.execute("SELECT id FROM games WHERE game_id = %s", (game_id,))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # Get team IDs
        home_team_id = self.get_or_create_team(home_team_abbr)
        away_team_id = self.get_or_create_team(away_team_abbr)
        
        logger.info(f"Creating game: {away_team_abbr} @ {home_team_abbr} (Game {game_id})")
        self.cursor.execute("""
            INSERT INTO games (game_id, season, week, season_type, home_team_id, away_team_id, date) 
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE) 
            RETURNING id
        """, (game_id, season, week, season_type, home_team_id, away_team_id))
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
        
        # Skip TBD teams
        if team_abbr == 'TBD':
            logger.info(f"Skipping TBD team file: {filename}")
            return 0
        
        # Get the actual game matchup from ESPN
        matchup = self.get_game_matchup_from_espn(game_id)
        
        # Get IDs
        team_id = self.get_or_create_team(team_abbr)
        game_db_id = self.get_or_create_game(game_id, season, week, season_type, 
                                           matchup['home_team'], matchup['away_team'])
        
        records_loaded = 0
        
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            return 0
            
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
    parser = argparse.ArgumentParser(description='Load NFL CSV data with fixed team assignments')
    parser.add_argument('--csv-dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--season', type=int, required=True, help='Season year')
    parser.add_argument('--week', type=int, required=True, help='Week number')
    parser.add_argument('--season-type', default='regular', help='Season type (regular/playoffs)')
    
    args = parser.parse_args()
    
    loader = FixedCSVLoader()
    loader.load_week_data(args.csv_dir, args.season, args.week, args.season_type)
    
    logger.info("Fixed database loading complete!")

if __name__ == "__main__":
    main()