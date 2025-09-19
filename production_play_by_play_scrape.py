
import os
import sys
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


def get_game_ids():
    """Fetches unique game_ids from the public.games table."""
    game_ids = []
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_id FROM public.games WHERE date < NOW()")
        rows = cur.fetchall()
        game_ids = [row[0] for row in rows]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error fetching game IDs: {error}")
    finally:
        if conn is not None:
            conn.close()
    return game_ids

def main():
    """
    - Fetches all unique game_ids from the database.
    - Runs the scrape_play_by_play.py script for each game_id.
    - Moves the generated CSV to the FootballData/PBP_CSV directory.
    """
    game_ids = get_game_ids()
    if not game_ids:
        print("No game IDs found in the database.")
        return

    scraper_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.abspath(os.path.join(scraper_dir, '../FootballData/PBP_CSV'))
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for game_id in game_ids:
        print(f"Scraping play-by-play data for game_id: {game_id}")
        
        # Path to the original scraper script
        scraper_script_path = os.path.join(scraper_dir, 'scrape_play_by_play.py')

        # Run the scraper script
        try:
            # It's better to run the script from its own directory
            subprocess.run(
                ['python3', scraper_script_path, str(game_id)],
                check=True,
                cwd=scraper_dir,
                capture_output=True,
                text=True
            )

            # Move the generated file
            source_file = os.path.join(scraper_dir, f'play_by_play_{game_id}.csv')
            destination_file = os.path.join(output_dir, f'play_by_play_{game_id}.csv')

            if os.path.exists(source_file):
                shutil.move(source_file, destination_file)
                print(f"Successfully moved {source_file} to {destination_file}")
            else:
                print(f"Error: Output file not found for game_id {game_id}")

        except subprocess.CalledProcessError as e:
            print(f"Error scraping game_id {game_id}:")
            print(e.stdout)
            print(e.stderr)
        except Exception as e:
            print(f"An unexpected error occurred for game_id {game_id}: {e}")

        # Add a random delay to mimic human behavior
        sleep_time = random.uniform(5, 15)
        print(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
