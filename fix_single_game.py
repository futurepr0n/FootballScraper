#!/usr/bin/env python3
"""
Fix a single game's score calculation
"""

import psycopg2
import pandas as pd
from pathlib import Path
import re

# Database configuration
DB_CONFIG = {
    'host': '192.168.1.23',
    'port': 5432,
    'user': 'postgres',
    'password': 'korn5676',
    'database': 'football_tracker'
}

def calculate_gb_wsh_score():
    """Calculate the correct score for GB @ WSH game"""

    csv_dir = Path('/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/BOXSCORE_CSV')
    game_id = '401772936'

    # Track scores - ignore TB files
    gb_score = 0
    wsh_score = 0

    # Process GB files
    gb_files = list(csv_dir.glob(f'nfl_GB_*_{game_id}.csv'))
    print(f"Found {len(gb_files)} GB files")

    for csv_file in gb_files:
        try:
            df = pd.read_csv(csv_file)
            filename = csv_file.name

            if 'passing' in filename and 'td' in df.columns:
                td_count = df['td'].fillna(0).astype(float).sum()
                gb_score += td_count * 6
                print(f"GB passing TDs: {td_count}")

            elif 'rushing' in filename and 'td' in df.columns:
                td_count = df['td'].fillna(0).astype(float).sum()
                gb_score += td_count * 6
                print(f"GB rushing TDs: {td_count}")

            elif 'kicking' in filename:
                if 'fg' in df.columns:
                    for fg_str in df['fg'].fillna('0/0'):
                        if '/' in str(fg_str):
                            made = int(str(fg_str).split('/')[0])
                            gb_score += made * 3
                            print(f"GB FGs made: {made}")

                if 'xp' in df.columns:
                    for xp_str in df['xp'].fillna('0/0'):
                        if '/' in str(xp_str):
                            made = int(str(xp_str).split('/')[0])
                            gb_score += made
                            print(f"GB XPs made: {made}")

        except Exception as e:
            print(f"Error processing {csv_file}: {e}")

    # Process WSH files
    wsh_files = list(csv_dir.glob(f'nfl_WSH_*_{game_id}.csv'))
    print(f"\nFound {len(wsh_files)} WSH files")

    for csv_file in wsh_files:
        try:
            df = pd.read_csv(csv_file)
            filename = csv_file.name

            if 'passing' in filename and 'td' in df.columns:
                td_count = df['td'].fillna(0).astype(float).sum()
                wsh_score += td_count * 6
                print(f"WSH passing TDs: {td_count}")

            elif 'rushing' in filename and 'td' in df.columns:
                td_count = df['td'].fillna(0).astype(float).sum()
                wsh_score += td_count * 6
                print(f"WSH rushing TDs: {td_count}")

            elif 'kicking' in filename:
                if 'fg' in df.columns:
                    for fg_str in df['fg'].fillna('0/0'):
                        if '/' in str(fg_str):
                            made = int(str(fg_str).split('/')[0])
                            wsh_score += made * 3
                            print(f"WSH FGs made: {made}")

                if 'xp' in df.columns:
                    for xp_str in df['xp'].fillna('0/0'):
                        if '/' in str(xp_str):
                            made = int(str(xp_str).split('/')[0])
                            wsh_score += made
                            print(f"WSH XPs made: {made}")

        except Exception as e:
            print(f"Error processing {csv_file}: {e}")

    print(f"\nFinal scores: GB {gb_score} @ WSH {wsh_score}")

    # Update the database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE games
        SET home_score = %s,
            away_score = %s,
            completed = true
        WHERE game_id = %s
    """, (int(wsh_score), int(gb_score), game_id))

    conn.commit()
    print(f"✅ Updated database: GB {gb_score} @ WSH {wsh_score}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    calculate_gb_wsh_score()