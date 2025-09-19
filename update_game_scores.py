#!/usr/bin/env python3
"""
Update Game Scores in PostgreSQL
Calculates home/away scores from player stats after boxscore scraping
"""

import os
import sys
import logging
import psycopg2
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, Optional
from datetime import datetime
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': '192.168.1.23',
    'port': 5432,
    'user': 'postgres',
    'password': 'korn5676',
    'database': 'football_tracker'
}

class GameScoreUpdater:
    def __init__(self, csv_dir: str = None):
        """Initialize the score updater"""
        if csv_dir is None:
            current_dir = Path(__file__).parent
            self.csv_dir = current_dir.parent / 'FootballData' / 'BOXSCORE_CSV'
        else:
            self.csv_dir = Path(csv_dir)

        logger.info(f"CSV directory: {self.csv_dir}")

        # Connect to database
        self.conn = None
        self.cursor = None
        self._connect_db()

    def _connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def calculate_game_score(self, game_id: str) -> Optional[Tuple[int, int, str, str]]:
        """
        Calculate total score for a game from player stats CSVs
        Returns: (home_score, away_score, home_team, away_team)
        """
        # Find all CSV files for this game
        pattern = f"*_{game_id}.csv"
        game_files = list(self.csv_dir.glob(pattern))

        if not game_files:
            logger.warning(f"No CSV files found for game {game_id}")
            return None

        logger.info(f"Found {len(game_files)} files for game {game_id}")

        # Track scores by team
        team_scores = {}
        teams_found = set()

        for csv_file in game_files:
            try:
                # Extract team from filename (e.g., nfl_KC_passing_week1_20250905_401772936.csv)
                filename = csv_file.name
                match = re.search(r'nfl_([A-Z]+)_', filename)
                if not match:
                    continue

                team = match.group(1)
                teams_found.add(team)

                if team not in team_scores:
                    team_scores[team] = 0

                # Read CSV and calculate points based on stat category
                df = pd.read_csv(csv_file)

                if 'passing' in filename:
                    # Count passing touchdowns
                    if 'td' in df.columns:
                        td_count = df['td'].fillna(0).astype(float).sum()
                        team_scores[team] += td_count * 6
                        logger.debug(f"{team} passing TDs: {td_count}")

                elif 'rushing' in filename:
                    # Count rushing touchdowns
                    if 'td' in df.columns:
                        td_count = df['td'].fillna(0).astype(float).sum()
                        team_scores[team] += td_count * 6
                        logger.debug(f"{team} rushing TDs: {td_count}")

                elif 'receiving' in filename:
                    # Receiving TDs are already counted in passing
                    pass

                elif 'kicking' in filename:
                    # Count field goals and extra points
                    if 'fg' in df.columns:
                        # Parse field goal column (e.g., "2/3" means 2 made)
                        for fg_str in df['fg'].fillna('0/0'):
                            if '/' in str(fg_str):
                                made = int(str(fg_str).split('/')[0])
                                team_scores[team] += made * 3
                                logger.debug(f"{team} FGs made: {made}")

                    if 'xp' in df.columns:
                        # Parse extra points (e.g., "3/3")
                        for xp_str in df['xp'].fillna('0/0'):
                            if '/' in str(xp_str):
                                made = int(str(xp_str).split('/')[0])
                                team_scores[team] += made
                                logger.debug(f"{team} XPs made: {made}")

                elif 'defensive' in filename or 'interceptions' in filename:
                    # Count defensive/return touchdowns if available
                    if 'td' in df.columns:
                        td_count = df['td'].fillna(0).astype(float).sum()
                        team_scores[team] += td_count * 6
                        logger.debug(f"{team} defensive/return TDs: {td_count}")

                elif 'punt_returns' in filename or 'kick_returns' in filename:
                    # Count return touchdowns
                    if 'td' in df.columns:
                        td_count = df['td'].fillna(0).astype(float).sum()
                        team_scores[team] += td_count * 6
                        logger.debug(f"{team} return TDs: {td_count}")

            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")
                continue

        if len(teams_found) != 2:
            logger.warning(f"Expected 2 teams, found {len(teams_found)}: {teams_found}")
            if len(teams_found) < 2:
                return None

        # Get the two teams
        teams = list(teams_found)[:2]

        # Determine home/away from database
        try:
            self.cursor.execute("""
                SELECT home_team_id, away_team_id,
                       ht.abbreviation as home_abbr,
                       at.abbreviation as away_abbr
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.id
                JOIN teams at ON g.away_team_id = at.id
                WHERE g.game_id = %s
            """, (game_id,))

            result = self.cursor.fetchone()
            if result:
                home_team_id, away_team_id, home_abbr, away_abbr = result

                home_score = team_scores.get(home_abbr, 0)
                away_score = team_scores.get(away_abbr, 0)

                logger.info(f"Game {game_id}: {away_abbr} @ {home_abbr} = {away_score}-{home_score}")
                return (home_score, away_score, home_abbr, away_abbr)
            else:
                logger.warning(f"Game {game_id} not found in database")
                # If not in DB, just return the scores we found
                team1, team2 = teams[0], teams[1]
                return (team_scores.get(team1, 0), team_scores.get(team2, 0), team1, team2)

        except Exception as e:
            logger.error(f"Error querying database for game {game_id}: {e}")
            return None

    def update_game_scores_for_week(self, season: int, week: int):
        """
        Update scores for all games in a specific week
        """
        logger.info(f"Updating scores for Season {season}, Week {week}")

        try:
            # Get all games for this week that need score updates
            self.cursor.execute("""
                SELECT g.id, g.game_id, g.home_score, g.away_score,
                       ht.abbreviation as home_team,
                       at.abbreviation as away_team
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.id
                JOIN teams at ON g.away_team_id = at.id
                WHERE g.season = %s AND g.week = %s
                  AND g.season_type = 'regular'
                  AND (g.home_score = 0 AND g.away_score = 0)
            """, (season, week))

            games = self.cursor.fetchall()
            logger.info(f"Found {len(games)} games needing score updates")

            updated_count = 0
            for game_row in games:
                game_db_id, game_id, current_home, current_away, home_team, away_team = game_row

                # Calculate scores from CSV files
                score_data = self.calculate_game_score(game_id)

                if score_data:
                    home_score, away_score, _, _ = score_data

                    # Update the database (convert to int to avoid numpy type issues)
                    self.cursor.execute("""
                        UPDATE games
                        SET home_score = %s,
                            away_score = %s,
                            completed = true,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (int(home_score), int(away_score), game_db_id))

                    logger.info(f"Updated game {game_id}: {away_team} {away_score} @ {home_team} {home_score}")
                    updated_count += 1
                else:
                    logger.warning(f"Could not calculate scores for game {game_id}")

            # Commit all updates
            self.conn.commit()
            logger.info(f"Successfully updated {updated_count} games")

            # Also update games that might have CSV data but aren't marked as 0-0
            self._update_additional_games(season, week)

        except Exception as e:
            logger.error(f"Error updating game scores: {e}")
            self.conn.rollback()
            raise

    def _update_additional_games(self, season: int, week: int):
        """
        Check for games that might have been marked completed but have wrong scores
        """
        try:
            # Find all CSV files for this week
            pattern = f"*_week{week}_*.csv"
            week_files = list(self.csv_dir.glob(pattern))

            # Extract unique game IDs
            game_ids = set()
            for f in week_files:
                match = re.search(r'_(\d{9})\.csv$', f.name)
                if match:
                    game_ids.add(match.group(1))

            logger.info(f"Found CSV data for {len(game_ids)} games in week {week}")

            for game_id in game_ids:
                # Check if this game needs updating
                self.cursor.execute("""
                    SELECT id, home_score, away_score
                    FROM games
                    WHERE game_id = %s AND season = %s
                """, (game_id, season))

                result = self.cursor.fetchone()
                if result:
                    db_id, current_home, current_away = result

                    # Calculate actual scores
                    score_data = self.calculate_game_score(game_id)
                    if score_data:
                        calc_home, calc_away, _, _ = score_data

                        # Update if different
                        if calc_home != current_home or calc_away != current_away:
                            self.cursor.execute("""
                                UPDATE games
                                SET home_score = %s,
                                    away_score = %s,
                                    completed = true,
                                    updated_at = NOW()
                                WHERE id = %s
                            """, (int(calc_home), int(calc_away), db_id))

                            logger.info(f"Corrected scores for game {game_id}: {calc_away}-{calc_home} (was {current_away}-{current_home})")

            self.conn.commit()

        except Exception as e:
            logger.error(f"Error in additional game updates: {e}")

    def update_latest_week(self):
        """
        Update scores for the most recent week with CSV data
        """
        # Find the latest week with CSV files
        csv_files = list(self.csv_dir.glob("nfl_*_week*.csv"))

        if not csv_files:
            logger.warning("No CSV files found")
            return

        # Extract week numbers
        weeks = set()
        for f in csv_files:
            match = re.search(r'_week(\d+)_', f.name)
            if match:
                weeks.add(int(match.group(1)))

        if weeks:
            latest_week = max(weeks)
            logger.info(f"Latest week with data: {latest_week}")

            # Assume current season (you might want to make this configurable)
            current_season = 2025
            self.update_game_scores_for_week(current_season, latest_week)

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Update NFL game scores from player stats')
    parser.add_argument('--season', type=int, default=2025, help='Season year')
    parser.add_argument('--week', type=int, help='Week number to update')
    parser.add_argument('--latest', action='store_true', help='Update the latest week with data')
    parser.add_argument('--csv-dir', help='Directory containing CSV files')
    parser.add_argument('--game-id', help='Update specific game by ID')

    args = parser.parse_args()

    # Initialize updater
    updater = GameScoreUpdater(csv_dir=args.csv_dir)

    try:
        if args.game_id:
            # Update specific game
            result = updater.calculate_game_score(args.game_id)
            if result:
                home, away, home_team, away_team = result
                print(f"Game {args.game_id}: {away_team} {away} @ {home_team} {home}")
            else:
                print(f"Could not calculate scores for game {args.game_id}")

        elif args.latest:
            # Update latest week
            updater.update_latest_week()

        elif args.week:
            # Update specific week
            updater.update_game_scores_for_week(args.season, args.week)

        else:
            print("Must specify --week, --latest, or --game-id")
            return

        print("✅ Score update complete")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"❌ Error: {e}")

    finally:
        updater.close()

if __name__ == "__main__":
    main()