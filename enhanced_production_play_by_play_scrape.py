#!/usr/bin/env python3
"""
Enhanced Production Play-by-Play Scraper
Adds week and season filtering to avoid re-processing existing data.
"""

import os
import sys
import argparse
import subprocess
import psycopg2
import shutil
import time
import random


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
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to database: {error}")
        sys.exit(1)


def get_game_ids(season=None, week=None):
    """Fetches unique game_ids from the public.games table with optional filters."""
    game_ids = []
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Build query based on parameters
        base_query = "SELECT DISTINCT game_id FROM public.games WHERE date < NOW()"
        params = []

        if season:
            base_query += " AND season = %s"
            params.append(season)

        if week:
            base_query += " AND week = %s"
            params.append(week)

        base_query += " ORDER BY game_id"

        print(f"Query: {base_query}")
        print(f"Parameters: {params}")

        cur.execute(base_query, params)
        rows = cur.fetchall()
        game_ids = [row[0] for row in rows]
        cur.close()

        print(f"Found {len(game_ids)} games to process")
        if game_ids:
            print(f"Game IDs: {game_ids[:5]}{'...' if len(game_ids) > 5 else ''}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error fetching game IDs: {error}")
    finally:
        if conn is not None:
            conn.close()
    return game_ids


def main():
    """
    Enhanced version with week/season filtering:
    - Fetches game_ids from the database with optional filters.
    - Runs the scrape_play_by_play.py script for each game_id.
    - Moves the generated CSV to the FootballData/PBP_CSV directory.
    """
    parser = argparse.ArgumentParser(description='Enhanced NFL Play-by-Play Scraper')
    parser.add_argument('--season', type=int, help='Season year (e.g., 2025)')
    parser.add_argument('--week', type=int, help='Week number (e.g., 3)')
    parser.add_argument('--dry-run', action='store_true', help='Show games that would be processed without scraping')

    args = parser.parse_args()

    # Get filtered game IDs
    game_ids = get_game_ids(season=args.season, week=args.week)
    if not game_ids:
        print("No game IDs found matching the criteria.")
        return

    if args.dry_run:
        print(f"DRY RUN: Would process {len(game_ids)} games:")
        for game_id in game_ids:
            print(f"  - {game_id}")
        return

    scraper_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.abspath(os.path.join(scraper_dir, '../FootballData/PBP_CSV'))

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    success_count = 0
    error_count = 0

    for i, game_id in enumerate(game_ids, 1):
        print(f"\n[{i}/{len(game_ids)}] Scraping play-by-play data for game_id: {game_id}")

        # Check if file already exists
        destination_file = os.path.join(output_dir, f'play_by_play_{game_id}.csv')
        if os.path.exists(destination_file):
            print(f"  ✓ File already exists, skipping: {destination_file}")
            success_count += 1
            continue

        # Path to the original scraper script
        scraper_script_path = os.path.join(scraper_dir, 'scrape_play_by_play.py')

        # Run the scraper script
        try:
            result = subprocess.run(
                ['python3', scraper_script_path, str(game_id)],
                check=True,
                cwd=scraper_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per game
            )

            # Move the generated file
            source_file = os.path.join(scraper_dir, f'play_by_play_{game_id}.csv')

            if os.path.exists(source_file):
                shutil.move(source_file, destination_file)
                print(f"  ✓ Successfully moved to {destination_file}")
                success_count += 1
            else:
                print(f"  ✗ Error: Output file not found for game_id {game_id}")
                error_count += 1

        except subprocess.TimeoutExpired:
            print(f"  ✗ Timeout: Game {game_id} took too long to scrape")
            error_count += 1
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Error scraping game_id {game_id}:")
            print(f"    stdout: {e.stdout}")
            print(f"    stderr: {e.stderr}")
            error_count += 1
        except Exception as e:
            print(f"  ✗ Unexpected error for game_id {game_id}: {e}")
            error_count += 1

        # Add a random delay to mimic human behavior (except for last game)
        if i < len(game_ids):
            sleep_time = random.uniform(3, 8)
            print(f"  Sleeping for {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)

    # Final summary
    print(f"\n{'='*50}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*50}")
    print(f"Total games: {len(game_ids)}")
    print(f"Successful: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()