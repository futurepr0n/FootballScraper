#!/usr/bin/env python3
"""
Direct PBP CSV Loader
Loads play-by-play CSV files directly into the pbp_csv table with correct schema mapping.
"""

import os
import csv
import psycopg2
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        return conn
    except Exception as error:
        logger.error(f"Database connection failed: {error}")
        raise

def extract_game_id_from_filename(filename):
    """Extract game ID from play_by_play_GAMEID.csv filename"""
    return filename.replace('play_by_play_', '').replace('.csv', '')

def load_pbp_csv_file(csv_path: Path, conn, cursor):
    """Load a single play-by-play CSV file into pbp_csv table"""
    filename = csv_path.name
    game_id = extract_game_id_from_filename(filename)

    logger.info(f"Processing {filename} (Game ID: {game_id})")

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Get expected columns
            expected_columns = ['Playcall', 'Time', 'Quarter', 'Play']
            if not all(col in reader.fieldnames for col in expected_columns):
                logger.warning(f"Missing columns in {filename}. Expected: {expected_columns}, Found: {reader.fieldnames}")
                return 0

            plays_loaded = 0
            play_sequence = 1

            for row in reader:
                try:
                    # Map CSV columns to database columns
                    cursor.execute("""
                        INSERT INTO pbp_csv (game_id, play_sequence, quarter, time, play, playcall)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        game_id,
                        play_sequence,
                        row.get('Quarter', '').strip(),
                        row.get('Time', '').strip(),
                        row.get('Play', '').strip(),
                        row.get('Playcall', '').strip()
                    ))

                    plays_loaded += 1
                    play_sequence += 1

                except Exception as e:
                    logger.error(f"Error inserting play {play_sequence} for game {game_id}: {e}")
                    continue

            return plays_loaded

    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}")
        return 0

def update_game_completion_status(conn, cursor, season: int = 2025, week: int = None):
    """
    Update game completion status based on loaded player statistics.
    Marks games as completed if they have player statistics loaded.
    """
    try:
        if week is not None:
            # Update specific week
            logger.info(f"Updating completion status for Season {season}, Week {week}")
            cursor.execute("""
                UPDATE games
                SET completed = true,
                    home_score = COALESCE(
                        (SELECT SUM(pgs.kicking_points + pgs.rushing_touchdowns * 6 + pgs.receiving_touchdowns * 6 + pgs.passing_touchdowns * 6)
                         FROM player_game_stats pgs
                         JOIN players p ON pgs.player_id = p.id
                         WHERE pgs.game_id = games.id AND p.team_id = games.home_team_id), 0),
                    away_score = COALESCE(
                        (SELECT SUM(pgs.kicking_points + pgs.rushing_touchdowns * 6 + pgs.receiving_touchdowns * 6 + pgs.passing_touchdowns * 6)
                         FROM player_game_stats pgs
                         JOIN players p ON pgs.player_id = p.id
                         WHERE pgs.game_id = games.id AND p.team_id = games.away_team_id), 0)
                WHERE season = %s AND week = %s
                  AND completed = false
                  AND EXISTS (
                      SELECT 1 FROM player_game_stats pgs
                      WHERE pgs.game_id = games.id
                  )
            """, (season, week))
        else:
            # Update entire season
            logger.info(f"Updating completion status for Season {season} (all weeks)")
            cursor.execute("""
                UPDATE games
                SET completed = true,
                    home_score = COALESCE(
                        (SELECT SUM(pgs.kicking_points + pgs.rushing_touchdowns * 6 + pgs.receiving_touchdowns * 6 + pgs.passing_touchdowns * 6)
                         FROM player_game_stats pgs
                         JOIN players p ON pgs.player_id = p.id
                         WHERE pgs.game_id = games.id AND p.team_id = games.home_team_id), 0),
                    away_score = COALESCE(
                        (SELECT SUM(pgs.kicking_points + pgs.rushing_touchdowns * 6 + pgs.receiving_touchdowns * 6 + pgs.passing_touchdowns * 6)
                         FROM player_game_stats pgs
                         JOIN players p ON pgs.player_id = p.id
                         WHERE pgs.game_id = games.id AND p.team_id = games.away_team_id), 0)
                WHERE season = %s
                  AND completed = false
                  AND EXISTS (
                      SELECT 1 FROM player_game_stats pgs
                      WHERE pgs.game_id = games.id
                  )
            """, (season,))

        updated_count = cursor.rowcount
        logger.info(f"Updated completion status for {updated_count} games")
        return updated_count

    except Exception as e:
        logger.error(f"Error updating game completion status: {e}")
        return 0

def load_all_pbp_files(pbp_directory: str, week_filter: int = None):
    """Load all play-by-play CSV files from directory"""
    pbp_path = Path(pbp_directory)

    if not pbp_path.exists():
        raise FileNotFoundError(f"PBP directory not found: {pbp_path}")

    # Find CSV files
    pattern = 'play_by_play_*.csv'
    csv_files = list(pbp_path.glob(pattern))

    # Filter for specific week if provided (Week 3 game IDs start with 4017727xx)
    if week_filter == 3:
        week3_patterns = ['4017727', '401772812', '401772920']  # Week 3 game ID patterns
        csv_files = [f for f in csv_files if any(pattern in f.name for pattern in week3_patterns)]

    logger.info(f"Found {len(csv_files)} play-by-play CSV files to process")

    if not csv_files:
        logger.warning("No CSV files found to process")
        return {'success': False, 'message': 'No files found'}

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    total_plays = 0
    files_processed = 0
    errors = []

    try:
        for csv_file in sorted(csv_files):
            plays_loaded = load_pbp_csv_file(csv_file, conn, cursor)
            if plays_loaded > 0:
                total_plays += plays_loaded
                files_processed += 1
                logger.info(f"✓ Loaded {plays_loaded} plays from {csv_file.name}")
            else:
                errors.append(f"Failed to load {csv_file.name}")

        # AUTOMATIC COMPLETION STATUS UPDATE
        # Update game completion status for any games with loaded stats
        updated_games = update_game_completion_status(conn, cursor, 2025, week_filter)
        logger.info(f"Automatically marked {updated_games} games as completed with calculated scores")

        # Commit all changes (includes both PBP data and completion status updates)
        conn.commit()

        logger.info(f"Successfully loaded {total_plays} total plays from {files_processed} files")

        return {
            'success': True,
            'files_processed': files_processed,
            'total_plays': total_plays,
            'games_completed': updated_games,
            'errors': errors
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"Transaction failed: {e}")
        return {
            'success': False,
            'message': f"Transaction failed: {e}",
            'errors': errors
        }

    finally:
        cursor.close()
        conn.close()

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Load play-by-play CSV files to pbp_csv table')
    parser.add_argument('--pbp-dir', default='../FootballData/PBP_CSV', help='PBP CSV directory')
    parser.add_argument('--week', type=int, help='Filter for specific week (e.g., 3 for Week 3)')

    args = parser.parse_args()

    try:
        result = load_all_pbp_files(args.pbp_dir, args.week)

        if result['success']:
            print(f"\n✅ Successfully loaded play-by-play data")
            print(f"   Files processed: {result['files_processed']}")
            print(f"   Total plays loaded: {result['total_plays']}")
            print(f"   Games marked completed: {result.get('games_completed', 0)}")

            if result['errors']:
                print(f"   ⚠️ Errors: {len(result['errors'])}")
                for error in result['errors'][:5]:
                    print(f"     • {error}")
        else:
            print(f"\n❌ Failed to load play-by-play data: {result.get('message', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Fatal error: {e}")

if __name__ == "__main__":
    main()