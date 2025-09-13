#!/usr/bin/env python3
"""
Historical NFL Matchup Analyzer
Analyzes head-to-head matchups between teams using historical data
Provides insights on play calling, scoring patterns, and key performances
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Tuple
import argparse
import os

class MatchupAnalyzer:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', '192.168.1.23'),
            database=os.getenv('DB_NAME', 'football_tracker'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'korn5676'),
            port=int(os.getenv('DB_PORT', 5432))
        )
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
    
    def get_team_id(self, team_abbr: str) -> int:
        """Get team ID from abbreviation"""
        query = "SELECT id FROM teams WHERE abbreviation = %s"
        self.cursor.execute(query, (team_abbr.upper(),))
        result = self.cursor.fetchone()
        return result['id'] if result else None
    
    def get_historical_matchups(self, team1: str, team2: str, seasons: List[int] = None) -> List[Dict]:
        """Get all historical games between two teams"""
        team1_id = self.get_team_id(team1)
        team2_id = self.get_team_id(team2)
        
        if not team1_id or not team2_id:
            print(f"Team not found: {team1 if not team1_id else team2}")
            return []
        
        query = """
            SELECT DISTINCT
                g.id as game_id,
                g.season,
                g.week,
                g.season_type,
                g.home_team_id,
                g.away_team_id,
                g.home_score,
                g.away_score,
                ht.abbreviation as home_team,
                at.abbreviation as away_team,
                CASE 
                    WHEN g.home_score > g.away_score THEN ht.abbreviation
                    WHEN g.away_score > g.home_score THEN at.abbreviation
                    ELSE 'TIE'
                END as winner
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE ((g.home_team_id = %s AND g.away_team_id = %s)
                OR (g.home_team_id = %s AND g.away_team_id = %s))
        """
        
        params = [team1_id, team2_id, team2_id, team1_id]
        
        if seasons:
            query += " AND g.season IN %s"
            params.append(tuple(seasons))
        
        query += " ORDER BY g.season DESC, g.week DESC"
        
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def analyze_game_stats(self, game_id: int) -> Dict:
        """Analyze detailed stats for a specific game"""
        # Get game info
        self.cursor.execute("""
            SELECT g.*, ht.abbreviation as home_team, at.abbreviation as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.id
            JOIN teams at ON g.away_team_id = at.id
            WHERE g.id = %s
        """, (game_id,))
        game = self.cursor.fetchone()
        
        if not game:
            return None
        
        # Get team stats
        self.cursor.execute("""
            SELECT 
                t.abbreviation as team,
                SUM(pgs.passing_yards) as total_passing_yards,
                SUM(pgs.rushing_yards) as total_rushing_yards,
                SUM(pgs.passing_attempts) as pass_attempts,
                SUM(pgs.rushing_attempts) as rush_attempts,
                SUM(pgs.passing_touchdowns) as passing_tds,
                SUM(pgs.rushing_touchdowns) as rushing_tds,
                SUM(pgs.passing_completions) as completions,
                AVG(CASE WHEN pgs.passing_attempts > 0 
                    THEN pgs.passing_completions::float / pgs.passing_attempts * 100 
                    ELSE 0 END) as completion_pct
            FROM player_game_stats pgs
            JOIN teams t ON pgs.team_id = t.id
            WHERE pgs.game_id = %s
            GROUP BY t.id, t.abbreviation
        """, (game_id,))
        
        team_stats = {}
        for row in self.cursor.fetchall():
            team_stats[row['team']] = dict(row)
        
        # Get top performers
        self.cursor.execute("""
            SELECT 
                p.name,
                p.position,
                t.abbreviation as team,
                pgs.passing_yards,
                pgs.passing_touchdowns,
                pgs.rushing_yards,
                pgs.rushing_touchdowns,
                pgs.receiving_yards,
                pgs.receiving_touchdowns,
                pgs.receptions
            FROM player_game_stats pgs
            JOIN players p ON pgs.player_id = p.id
            JOIN teams t ON pgs.team_id = t.id
            WHERE pgs.game_id = %s
                AND (pgs.passing_yards > 100 
                    OR pgs.rushing_yards > 50 
                    OR pgs.receiving_yards > 50
                    OR pgs.passing_touchdowns > 0
                    OR pgs.rushing_touchdowns > 0
                    OR pgs.receiving_touchdowns > 0)
            ORDER BY 
                COALESCE(pgs.passing_yards, 0) + 
                COALESCE(pgs.rushing_yards, 0) + 
                COALESCE(pgs.receiving_yards, 0) DESC
            LIMIT 10
        """, (game_id,))
        
        top_performers = self.cursor.fetchall()
        
        return {
            'game': dict(game),
            'team_stats': team_stats,
            'top_performers': [dict(p) for p in top_performers]
        }
    
    def analyze_play_by_play_patterns(self, game_id: int) -> Dict:
        """Analyze play-by-play patterns if available"""
        # Check if play-by-play data exists
        self.cursor.execute("""
            SELECT COUNT(*) as play_count
            FROM plays
            WHERE game_id = %s
        """, (game_id,))
        
        play_count = self.cursor.fetchone()['play_count']
        
        if play_count == 0:
            return {'available': False}
        
        # Analyze by quarter
        self.cursor.execute("""
            SELECT 
                quarter,
                play_type,
                COUNT(*) as play_count,
                AVG(yards_gained) as avg_yards,
                SUM(CASE WHEN touchdown THEN 1 ELSE 0 END) as touchdowns,
                SUM(CASE WHEN first_down THEN 1 ELSE 0 END) as first_downs
            FROM plays
            WHERE game_id = %s
                AND play_type IN ('Pass', 'Run')
            GROUP BY quarter, play_type
            ORDER BY quarter, play_type
        """, (game_id,))
        
        quarter_breakdown = {}
        for row in self.cursor.fetchall():
            if row['quarter'] not in quarter_breakdown:
                quarter_breakdown[row['quarter']] = {}
            quarter_breakdown[row['quarter']][row['play_type']] = dict(row)
        
        # Analyze situational football
        self.cursor.execute("""
            SELECT 
                down,
                play_type,
                COUNT(*) as play_count,
                AVG(yards_gained) as avg_yards,
                SUM(CASE WHEN first_down THEN 1 ELSE 0 END) as conversions
            FROM plays
            WHERE game_id = %s
                AND play_type IN ('Pass', 'Run')
                AND down IN (1, 2, 3, 4)
            GROUP BY down, play_type
            ORDER BY down, play_type
        """, (game_id,))
        
        down_breakdown = {}
        for row in self.cursor.fetchall():
            if row['down'] not in down_breakdown:
                down_breakdown[row['down']] = {}
            down_breakdown[row['down']][row['play_type']] = dict(row)
        
        return {
            'available': True,
            'total_plays': play_count,
            'quarter_breakdown': quarter_breakdown,
            'down_breakdown': down_breakdown
        }
    
    def generate_matchup_report(self, team1: str, team2: str, last_n_games: int = 5):
        """Generate comprehensive matchup report"""
        print(f"\n{'='*80}")
        print(f"HISTORICAL MATCHUP ANALYSIS: {team1.upper()} vs {team2.upper()}")
        print(f"{'='*80}\n")
        
        # Get historical matchups
        matchups = self.get_historical_matchups(team1, team2)
        
        if not matchups:
            print(f"No historical matchups found between {team1} and {team2}")
            return
        
        # Overall record
        team1_wins = sum(1 for m in matchups if m['winner'] == team1.upper())
        team2_wins = sum(1 for m in matchups if m['winner'] == team2.upper())
        ties = sum(1 for m in matchups if m['winner'] == 'TIE')
        
        print(f"ALL-TIME RECORD ({len(matchups)} games)")
        print(f"{team1.upper()}: {team1_wins}-{team2_wins}-{ties}")
        print(f"Average Score: ", end="")
        
        # Calculate average scores (filter out NULL scores)
        team1_home_scores = [m['home_score'] for m in matchups if m['home_team'] == team1.upper() and m['home_score'] is not None]
        team1_away_scores = [m['away_score'] for m in matchups if m['away_team'] == team1.upper() and m['away_score'] is not None]
        team2_home_scores = [m['home_score'] for m in matchups if m['home_team'] == team2.upper() and m['home_score'] is not None]
        team2_away_scores = [m['away_score'] for m in matchups if m['away_team'] == team2.upper() and m['away_score'] is not None]
        
        all_team1_scores = team1_home_scores + team1_away_scores
        all_team2_scores = team2_home_scores + team2_away_scores
        
        team1_avg = (sum(all_team1_scores) / len(all_team1_scores)) if all_team1_scores else 0
        team2_avg = (sum(all_team2_scores) / len(all_team2_scores)) if all_team2_scores else 0
        
        print(f"{team1.upper()} {team1_avg:.1f} - {team2.upper()} {team2_avg:.1f}")
        
        # Recent games analysis
        print(f"\n{'='*80}")
        print(f"LAST {min(last_n_games, len(matchups))} GAMES")
        print(f"{'='*80}")
        
        for i, game in enumerate(matchups[:last_n_games]):
            print(f"\n{'-'*60}")
            print(f"Game {i+1}: {game['season']} Season, Week {game['week']} ({game['season_type']})")
            print(f"{game['away_team']} @ {game['home_team']}: {game['away_score']}-{game['home_score']}")
            print(f"Winner: {game['winner']}")
            
            # Get detailed stats
            stats = self.analyze_game_stats(game['game_id'])
            
            if stats and stats['team_stats']:
                print(f"\nTeam Statistics:")
                for team, team_stats in stats['team_stats'].items():
                    if team_stats:
                        print(f"\n{team}:")
                        print(f"  Passing: {team_stats.get('total_passing_yards', 0):.0f} yards "
                              f"({team_stats.get('completions', 0):.0f}/{team_stats.get('pass_attempts', 0):.0f}, "
                              f"{team_stats.get('completion_pct', 0):.1f}%)")
                        print(f"  Rushing: {team_stats.get('total_rushing_yards', 0):.0f} yards "
                              f"({team_stats.get('rush_attempts', 0):.0f} attempts)")
                        print(f"  TDs: {team_stats.get('passing_tds', 0):.0f} passing, "
                              f"{team_stats.get('rushing_tds', 0):.0f} rushing")
                        
                        # Calculate play calling ratio
                        pass_att = team_stats.get('pass_attempts', 0) or 0
                        rush_att = team_stats.get('rush_attempts', 0) or 0
                        total_plays = pass_att + rush_att
                        if total_plays > 0:
                            pass_pct = (pass_att / total_plays) * 100
                            print(f"  Play Calling: {pass_pct:.1f}% pass, {100-pass_pct:.1f}% run")
                
                if stats['top_performers']:
                    print(f"\nTop Performers:")
                    for player in stats['top_performers'][:5]:
                        perf_str = f"  {player['name']} ({player['team']}, {player['position']}): "
                        perfs = []
                        if player['passing_yards'] and player['passing_yards'] > 0:
                            perfs.append(f"{player['passing_yards']} pass yds, {player['passing_touchdowns']} TD")
                        if player['rushing_yards'] and player['rushing_yards'] > 0:
                            perfs.append(f"{player['rushing_yards']} rush yds, {player['rushing_touchdowns']} TD")
                        if player['receiving_yards'] and player['receiving_yards'] > 0:
                            perfs.append(f"{player['receiving_yards']} rec yds, {player['receiving_touchdowns']} TD")
                        print(perf_str + ", ".join(perfs))
            
            # Check for play-by-play data
            pbp = self.analyze_play_by_play_patterns(game['game_id'])
            if pbp['available']:
                print(f"\nPlay-by-Play Analysis ({pbp['total_plays']} plays):")
                
                # Quarter breakdown
                if pbp['quarter_breakdown']:
                    print(f"  Quarter Breakdown:")
                    for quarter in sorted(pbp['quarter_breakdown'].keys()):
                        q_data = pbp['quarter_breakdown'][quarter]
                        print(f"    Q{quarter}:", end="")
                        if 'Pass' in q_data:
                            print(f" Pass: {q_data['Pass']['play_count']} plays, "
                                  f"{q_data['Pass']['avg_yards']:.1f} avg yards", end="")
                        if 'Run' in q_data:
                            print(f" | Run: {q_data['Run']['play_count']} plays, "
                                  f"{q_data['Run']['avg_yards']:.1f} avg yards", end="")
                        print()
        
        # Trends analysis
        print(f"\n{'='*80}")
        print("TRENDS & INSIGHTS")
        print(f"{'='*80}")
        
        recent_games = matchups[:min(5, len(matchups))]
        recent_winners = [g['winner'] for g in recent_games]
        
        if recent_winners.count(team1.upper()) > recent_winners.count(team2.upper()):
            print(f"• {team1.upper()} has won {recent_winners.count(team1.upper())} of last {len(recent_games)} matchups")
        elif recent_winners.count(team2.upper()) > recent_winners.count(team1.upper()):
            print(f"• {team2.upper()} has won {recent_winners.count(team2.upper())} of last {len(recent_games)} matchups")
        
        # Scoring trends (only games with scores)
        recent_total_scores = [g['home_score'] + g['away_score'] for g in recent_games 
                              if g['home_score'] is not None and g['away_score'] is not None]
        if recent_total_scores:
            avg_total = sum(recent_total_scores) / len(recent_total_scores)
            print(f"• Average total score in last {len(recent_games)} games: {avg_total:.1f} points")
            
            high_scoring = sum(1 for s in recent_total_scores if s > 45)
            if high_scoring > len(recent_games) / 2:
                print(f"• Trend: High-scoring affairs ({high_scoring}/{len(recent_games)} games over 45 points)")
            elif sum(1 for s in recent_total_scores if s < 35) > len(recent_games) / 2:
                print(f"• Trend: Defensive battles (majority under 35 points)")
        
        print(f"\n{'='*80}\n")

def main():
    parser = argparse.ArgumentParser(description='Analyze historical NFL matchups')
    parser.add_argument('team1', help='First team abbreviation (e.g., NYG)')
    parser.add_argument('team2', help='Second team abbreviation (e.g., PHI)')
    parser.add_argument('--games', type=int, default=5, help='Number of recent games to analyze in detail')
    parser.add_argument('--seasons', nargs='+', type=int, help='Specific seasons to analyze')
    
    args = parser.parse_args()
    
    analyzer = MatchupAnalyzer()
    analyzer.generate_matchup_report(args.team1, args.team2, args.games)

if __name__ == "__main__":
    main()