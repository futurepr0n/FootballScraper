import os
import csv
import re
import psycopg2
import sys

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

def parse_situation(play_text):
    """Extracts the situation from the play description."""
    if not play_text:
        return None
    # The situation is the last sentence, usually after a period.
    parts = play_text.split('. ')
    if len(parts) > 1:
        # Check if the last part looks like a situation
        last_part = parts[-1].strip()
        if re.search(r'^\d+(st|nd|rd|th)\s*&', last_part):
            return last_part
    # Fallback for plays that might not have a period but end with down and distance info
    match = re.search(r'(\d+(?:st|nd|rd|th)\s*&\s*\d+\s*at\s*.*)$|(\d+(?:st|nd|rd|th)\s*&\s*\d+\s*for\s*.*)$', play_text)
    if match:
        return match.group(1) or match.group(2)
    return None

def parse_down(situation_text):
    """Extracts the down from the situation text."""
    if not situation_text:
        return None
    match = re.search(r'^(\d)', situation_text)
    if match:
        return int(match.group(1))
    return None

def parse_distance(situation_text):
    """Extracts the distance from the situation text."""
    if not situation_text:
        return None
    match = re.search(r'&\s*(\d+)', situation_text)
    if match:
        return int(match.group(1))
    return None

def load_pbp_data(directory):
    """Loads play-by-play data from CSV files into the database."""
    conn = get_db_connection()
    cur = conn.cursor()

    files_processed = 0
    rows_inserted = 0

    # Get a sorted list of files to process them in a consistent order
    filenames = sorted([f for f in os.listdir(directory) if f.startswith('play_by_play_') and f.endswith('.csv')])

    for filename in filenames:
        game_id = filename.replace('play_by_play_', '').replace('.csv', '')
        filepath = os.path.join(directory, filename)
        files_processed += 1
        print(f"Processing file: {filename}")

        try:
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                # Check for empty file
                first_line = csvfile.readline()
                if not first_line:
                    print(f"Skipping empty file: {filename}")
                    continue
                csvfile.seek(0)

                reader = csv.DictReader(csvfile)
                play_sequence = 1
                for row in reader:
                    play_summary = row.get('Playcall')
                    time_quarter = row.get('Time')
                    play_descriptor = row.get('Play')
                    quarter_str = row.get('Quarter')
                    
                    try:
                        quarter = int(quarter_str) if quarter_str and quarter_str.isdigit() else None
                    except (ValueError, TypeError):
                        quarter = None

                    situation = parse_situation(play_descriptor)
                    down = parse_down(situation)
                    distance = parse_distance(situation)

                    try:
                        cur.execute(
                            """
                            INSERT INTO plays (
                                game_id, play_sequence, play_summary, time_quarter,
                                play_descriptor, situation, quarter, time_remaining,
                                down, distance
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (game_id, play_sequence) DO NOTHING;
                            """,
                            (
                                game_id, play_sequence, play_summary, time_quarter,
                                play_descriptor, situation, quarter, time_quarter, # time_remaining is same as time_quarter
                                down, distance
                            )
                        )
                        rows_inserted += 1
                        play_sequence += 1
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(f"Error inserting row for game_id {game_id}, sequence {play_sequence}: {error}")
                        conn.rollback()
                conn.commit()

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print(f"\nProcessed {files_processed} files and inserted {rows_inserted} rows.")


if __name__ == '__main__':
    pbp_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../FootballData/PBP_CSV'))
    if not os.path.exists(pbp_dir):
        print(f"Directory not found: {pbp_dir}")
        sys.exit(1)
    load_pbp_data(pbp_dir)
    print("Data loading process completed.")