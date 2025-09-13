#!/usr/bin/env python3
"""
Consolidate duplicate games that represent the same NFL matchup
Merge player stats and keep only one game record per team matchup
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GameConsolidator:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
    def find_duplicate_matchups(self, season, week, season_type='regular'):
        """Find games that represent the same team matchup"""
        logger.info(f"Finding duplicate matchups for {season} Week {week}")
        
        # Group games by team matchup (considering both home/away combinations)
        self.cursor.execute("""
            SELECT 
                LEAST(g.home_team_id, g.away_team_id) as team1_id,
                GREATEST(g.home_team_id, g.away_team_id) as team2_id,
                ARRAY_AGG(g.id) as game_ids,
                ARRAY_AGG(g.game_id) as espn_game_ids,
                COUNT(*) as duplicate_count,
                MIN(ht.abbreviation) as team1_abbr,
                MIN(at.abbreviation) as team2_abbr
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id  
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.season = %s AND g.week = %s AND g.season_type = %s
            GROUP BY LEAST(g.home_team_id, g.away_team_id), GREATEST(g.home_team_id, g.away_team_id)
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """, (season, week, season_type))
        
        duplicates = self.cursor.fetchall()
        logger.info(f"Found {len(duplicates)} duplicate matchups")
        
        return duplicates
    
    def consolidate_matchup(self, duplicate_info):
        """Consolidate a single duplicate matchup"""
        game_ids = duplicate_info['game_ids']
        team1_abbr = duplicate_info['team1_abbr'] 
        team2_abbr = duplicate_info['team2_abbr']
        
        logger.info(f"Consolidating {team1_abbr} vs {team2_abbr}: {len(game_ids)} games")
        
        # Keep the first game as the primary
        primary_game_id = game_ids[0]
        duplicate_game_ids = game_ids[1:]
        
        # Merge player stats from duplicates into primary game
        for dup_game_id in duplicate_game_ids:
            logger.info(f"  Merging stats from game {dup_game_id} into {primary_game_id}")
            
            # Get all player stats for the duplicate game
            self.cursor.execute("""
                SELECT * FROM player_game_stats WHERE game_id = %s
            """, (dup_game_id,))
            
            dup_stats = self.cursor.fetchall()
            
            for stat in dup_stats:
                # Try to merge with existing stat for same player in primary game
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
                    stat['player_id'], primary_game_id, stat['team_id'],
                    stat['passing_attempts'] or 0, stat['passing_completions'] or 0,
                    stat['passing_yards'] or 0, stat['passing_touchdowns'] or 0,
                    stat['rushing_attempts'] or 0, stat['rushing_yards'] or 0,
                    stat['rushing_touchdowns'] or 0,
                    stat['receiving_yards'] or 0, stat['receiving_touchdowns'] or 0
                ))
            
            # Delete the duplicate player stats
            self.cursor.execute("DELETE FROM player_game_stats WHERE game_id = %s", (dup_game_id,))
            
            # Delete the duplicate game
            self.cursor.execute("DELETE FROM games WHERE id = %s", (dup_game_id,))
        
        self.conn.commit()
        logger.info(f"  Consolidated into game {primary_game_id}")
    
    def consolidate_week(self, season, week, season_type='regular'):
        """Consolidate all duplicate games for a week"""
        duplicates = self.find_duplicate_matchups(season, week, season_type)
        
        for duplicate in duplicates:
            self.consolidate_matchup(duplicate)
        
        # Verify results
        self.cursor.execute("""
            SELECT COUNT(*) as total_games,
                   COUNT(DISTINCT (LEAST(home_team_id, away_team_id), GREATEST(home_team_id, away_team_id))) as unique_matchups
            FROM games 
            WHERE season = %s AND week = %s AND season_type = %s
        """, (season, week, season_type))
        
        result = self.cursor.fetchone()
        logger.info(f"After consolidation: {result['total_games']} games, {result['unique_matchups']} unique matchups")
        
        return result['total_games']

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Consolidate duplicate NFL games')
    parser.add_argument('--season', type=int, required=True, help='Season year')
    parser.add_argument('--week', type=int, required=True, help='Week number')
    parser.add_argument('--season-type', default='regular', help='Season type (regular/playoffs)')
    
    args = parser.parse_args()
    
    consolidator = GameConsolidator()
    final_games = consolidator.consolidate_week(args.season, args.week, args.season_type)
    
    logger.info(f"Consolidation complete! Final game count: {final_games}")

if __name__ == "__main__":
    main()