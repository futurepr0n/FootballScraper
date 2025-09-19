import os
import csv
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

def setup_new_staging_table(cursor):
    """Creates or recreates the pbp_csv staging table with the user-specified schema."""
    print("Setting up 'pbp_csv' staging table with the new schema...")
    cursor.execute("DROP TABLE IF EXISTS pbp_csv;")
    cursor.execute("""
        CREATE TABLE pbp_csv (
            game_id VARCHAR(50),
            play_sequence INTEGER,
            Quarter TEXT,
            Time TEXT,
            Play TEXT,
            Playcall TEXT
        );
    """)
    print("Table 'pbp_csv' created successfully with the new schema.")

def load_data_to_new_staging(directory, conn, cur):
    """Loads play-by-play data from CSV files into the new staging table."""
    print("\nStarting data loading process...")
    files_processed = 0
    rows_inserted = 0

    filenames = sorted([f for f in os.listdir(directory) if f.startswith('play_by_play_') and f.endswith('.csv')])

    for filename in filenames:
        game_id = filename.replace('play_by_play_', '').replace('.csv', '')
        filepath = os.path.join(directory, filename)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline()
                if not first_line or not first_line.strip():
                    print(f"Skipping empty or header-only file: {filename}")
                    continue
                csvfile.seek(0)

                reader = csv.DictReader(csvfile)
                
                play_sequence = 1
                for row in reader:
                    try:
                        cur.execute(
                            """
                            INSERT INTO pbp_csv (
                                game_id, play_sequence, Quarter, Time, Play, Playcall
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                game_id,
                                play_sequence,
                                row.get('Quarter'),
                                row.get('Time'),
                                row.get('Play'),
                                row.get('Playcall')
                            )
                        )
                        rows_inserted += 1
                        play_sequence += 1
                    except (Exception, psycopg2.DatabaseError) as error:
                        print(f"Error inserting row for game_id {game_id}, sequence {play_sequence}: {error}")
                        conn.rollback()
            
            conn.commit()
            files_processed += 1
            print(f"Successfully processed file: {filename}")

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            conn.rollback()

    print(f"\nProcessed {files_processed} files and inserted {rows_inserted} rows.")

if __name__ == '__main__':
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        setup_new_staging_table(cur)
        conn.commit()

        pbp_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../FootballData/PBP_CSV'))
        if not os.path.exists(pbp_dir):
            print(f"Directory not found: {pbp_dir}")
            sys.exit(1)
        
        load_data_to_new_staging(pbp_dir, conn, cur)

    except (Exception, psycopg2.DatabaseError) as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
        print("\nData loading process completed.")
