#!/usr/bin/env python3
"""
Fix game data issues in the database
Run this after initial data load or when issues are detected
"""

import psycopg2
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': '192.168.1.23',
    'port': 5432,
    'user': 'postgres',
    'password': 'korn5676',
    'database': 'football_tracker'
}

def get_week_dates(season, week):
    """
    Calculate actual NFL game dates for a given week
    Week 1 starts first Thursday after September 1st
    """
    if season == 2025:
        # 2025 Season schedule
        week_dates = {
            1: {'thursday': '2025-09-05', 'sunday': '2025-09-08', 'monday': '2025-09-09'},
            2: {'thursday': '2025-09-12', 'sunday': '2025-09-15', 'monday': '2025-09-16'},
            3: {'thursday': '2025-09-19', 'sunday': '2025-09-22', 'monday': '2025-09-23'},
            4: {'thursday': '2025-09-26', 'sunday': '2025-09-29', 'monday': '2025-09-30'},
            5: {'thursday': '2025-10-03', 'sunday': '2025-10-06', 'monday': '2025-10-07'},
            6: {'thursday': '2025-10-10', 'sunday': '2025-10-13', 'monday': '2025-10-14'},
            7: {'thursday': '2025-10-17', 'sunday': '2025-10-20', 'monday': '2025-10-21'},
            8: {'thursday': '2025-10-24', 'sunday': '2025-10-27', 'monday': '2025-10-28'},
            9: {'thursday': '2025-10-31', 'sunday': '2025-11-03', 'monday': '2025-11-04'},
            10: {'thursday': '2025-11-07', 'sunday': '2025-11-10', 'monday': '2025-11-11'},
            11: {'thursday': '2025-11-14', 'sunday': '2025-11-17', 'monday': '2025-11-18'},
            12: {'thursday': '2025-11-21', 'sunday': '2025-11-24', 'monday': '2025-11-25'},
            13: {'thursday': '2025-11-28', 'sunday': '2025-12-01', 'monday': '2025-12-02'},  # Thanksgiving
            14: {'thursday': '2025-12-05', 'sunday': '2025-12-08', 'monday': '2025-12-09'},
            15: {'thursday': '2025-12-12', 'sunday': '2025-12-15', 'monday': '2025-12-16'},
            16: {'thursday': '2025-12-19', 'sunday': '2025-12-22', 'monday': '2025-12-23'},
            17: {'thursday': '2025-12-26', 'sunday': '2025-12-29', 'monday': '2025-12-30'},
            18: {'saturday': '2026-01-03', 'sunday': '2026-01-04', 'monday': '2026-01-05'},
        }
        return week_dates.get(week, {'sunday': f'2025-09-{7 + (week-1)*7:02d}'})

    # Default fallback - calculate proper date
    from datetime import datetime, timedelta
    # NFL Season starts first Thursday after September 1st
    sept_1 = datetime(season, 9, 1)
    # Find first Thursday
    days_until_thursday = (3 - sept_1.weekday()) % 7  # Thursday is 3
    if days_until_thursday == 0 and sept_1.day > 1:
        days_until_thursday = 7
    first_thursday = sept_1 + timedelta(days=days_until_thursday)

    # Calculate week's Thursday
    week_thursday = first_thursday + timedelta(weeks=week-1)
    week_sunday = week_thursday + timedelta(days=3)

    return {'sunday': week_sunday.strftime('%Y-%m-%d')}

def fix_game_dates(conn):
    """Fix incorrect game dates in the database"""
    cursor = conn.cursor()

    try:
        # Get all games that need date fixes - limit to 2025 for now
        cursor.execute("""
            SELECT DISTINCT season, week
            FROM games
            WHERE season_type = 'regular'
              AND season = 2025
            ORDER BY season, week
        """)

        weeks = cursor.fetchall()

        for season, week in weeks:
            dates = get_week_dates(season, week)

            # Get games for this week
            cursor.execute("""
                SELECT game_id, home_team_id, away_team_id
                FROM games
                WHERE season = %s AND week = %s AND season_type = 'regular'
                ORDER BY game_id
            """, (season, week))

            games = cursor.fetchall()

            # Assign dates based on typical NFL schedule patterns
            # First game is usually Thursday night
            # Most games on Sunday
            # Last game on Monday night
            for i, (game_id, home_team, away_team) in enumerate(games):
                if i == 0 and 'thursday' in dates:
                    # First game is Thursday Night Football
                    date = dates['thursday']
                elif i == len(games) - 1 and 'monday' in dates:
                    # Last game is Monday Night Football
                    date = dates['monday']
                else:
                    # Everything else is Sunday
                    date = dates.get('sunday', dates.get('saturday', list(dates.values())[0]))

                cursor.execute("""
                    UPDATE games
                    SET date = %s
                    WHERE game_id = %s
                """, (date, game_id))

            logger.info(f"Fixed dates for Season {season} Week {week}")

        conn.commit()
        logger.info("✅ Game dates fixed successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error fixing game dates: {e}")
        raise

def fix_duplicate_teams(conn):
    """Fix games where both teams are the same (data error)"""
    cursor = conn.cursor()

    try:
        # Find games with duplicate teams
        cursor.execute("""
            SELECT g.id, g.game_id, g.home_team_id, g.away_team_id,
                   ht.abbreviation as home_abbr, at.abbreviation as away_abbr
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.home_team_id = g.away_team_id
        """)

        duplicates = cursor.fetchall()

        if duplicates:
            logger.warning(f"Found {len(duplicates)} games with duplicate teams")

            # Known fixes based on game IDs
            fixes = {
                '401772722': {'away': 'DET', 'home': 'GB'},  # Week 1: DET @ GB
                '401772936': {'away': 'GB', 'home': 'WSH'},  # Week 2: GB @ WSH
            }

            for game_db_id, game_id, home_id, away_id, home_abbr, away_abbr in duplicates:
                if game_id in fixes:
                    correct = fixes[game_id]

                    # Get correct team IDs
                    cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (correct['home'],))
                    new_home_id = cursor.fetchone()[0]

                    cursor.execute("SELECT id FROM teams WHERE abbreviation = %s", (correct['away'],))
                    new_away_id = cursor.fetchone()[0]

                    # Update the game
                    cursor.execute("""
                        UPDATE games
                        SET home_team_id = %s, away_team_id = %s
                        WHERE id = %s
                    """, (new_home_id, new_away_id, game_db_id))

                    logger.info(f"Fixed game {game_id}: {correct['away']} @ {correct['home']}")
                else:
                    logger.warning(f"Unknown duplicate game {game_id}: {away_abbr} @ {home_abbr}")

        conn.commit()
        logger.info("✅ Duplicate team issues fixed")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error fixing duplicate teams: {e}")
        raise

def validate_data(conn):
    """Validate that all fixes have been applied"""
    cursor = conn.cursor()

    issues = []

    # Check for duplicate teams
    cursor.execute("""
        SELECT COUNT(*)
        FROM games
        WHERE home_team_id = away_team_id
    """)

    dup_count = cursor.fetchone()[0]
    if dup_count > 0:
        issues.append(f"Still have {dup_count} games with duplicate teams")

    # Check for incorrect dates (all on Sept 18)
    cursor.execute("""
        SELECT COUNT(*)
        FROM games
        WHERE season = 2025
        AND week = 1
        AND date = '2025-09-18'
    """)

    bad_date_count = cursor.fetchone()[0]
    if bad_date_count > 0:
        issues.append(f"Still have {bad_date_count} Week 1 games with Sept 18 date")

    if issues:
        logger.warning("Validation issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")
        return False
    else:
        logger.info("✅ All data validated successfully")
        return True

def main():
    """Main entry point"""
    conn = None
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to database")

        # Apply fixes
        logger.info("Applying data fixes...")
        fix_game_dates(conn)
        fix_duplicate_teams(conn)

        # Validate
        if validate_data(conn):
            logger.info("✅ All fixes applied successfully")
        else:
            logger.warning("⚠️ Some issues remain - manual intervention may be needed")

    except Exception as e:
        logger.error(f"Fatal error: {e}")

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()