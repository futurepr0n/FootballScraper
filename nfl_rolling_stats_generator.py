#!/usr/bin/env python3
"""
NFL Rolling Stats Generator
Creates rolling statistics and team aggregation for NFL data
Similar to BaseballTracker but adapted for weekly NFL structure
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os

class NFLRollingStatsGenerator:
    def __init__(self):
        self.base_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Claude-Code")
        self.data_dir = self.base_dir / "FootballData" / "data"
        self.csv_dir = self.base_dir / "FootballData" / "CSV_BACKUPS"
        self.output_dir = self.data_dir / "rolling_stats"
        self.team_stats_dir = self.data_dir / "team_stats"
        
        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.team_stats_dir.mkdir(parents=True, exist_ok=True)
        
        # NFL position groups for analysis
        self.skill_positions = ['QB', 'RB', 'WR', 'TE']
        self.all_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
        
        # Statistical categories for aggregation
        self.stat_categories = [
            'passing', 'rushing', 'receiving', 'fumbles', 
            'interceptions', 'kick returns', 'punt returns', 'kicking', 'punting'
        ]
        
        # NFL teams
        self.nfl_teams = [
            'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 
            'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 
            'LV', 'LAC', 'LAR', 'MIA', 'MIN', 'NE', 'NO', 'NYG', 
            'NYJ', 'PHI', 'PIT', 'SF', 'SEA', 'TB', 'TEN', 'WSH'
        ]
    
    def load_weekly_data(self, week: int, season: int = 2025) -> Dict[str, Any]:
        """Load processed weekly data from JSON files"""
        try:
            weekly_file = self.data_dir / "weekly" / f"week_{week}_{season}.json"
            if not weekly_file.exists():
                print(f"⚠️  Weekly data not found: {weekly_file}")
                return {}
            
            with open(weekly_file, 'r') as f:
                return json.load(f)
        
        except Exception as e:
            print(f"Error loading weekly data for week {week}: {e}")
            return {}
    
    def aggregate_player_stats(self, weeks_data: List[Dict]) -> Dict[str, Dict]:
        """Aggregate player statistics across multiple weeks"""
        player_aggregates = {}
        
        for week_data in weeks_data:
            if 'teams' not in week_data:
                continue
            
            for team_abbr, team_info in week_data['teams'].items():
                if 'stats' not in team_info:
                    continue
                
                for category, players in team_info['stats'].items():
                    for player_data in players:
                        player_name = player_data.get('player', player_data.get('name', ''))
                        if not player_name:
                            continue
                        
                        # Initialize player entry
                        if player_name not in player_aggregates:
                            player_aggregates[player_name] = {
                                'name': player_name,
                                'team': team_abbr,
                                'position': self.determine_position_from_category(category),
                                'games_played': 0,
                                'weeks_active': set(),
                                'total_stats': {},
                                'weekly_breakdown': []
                            }
                        
                        player_entry = player_aggregates[player_name]
                        
                        # Track weeks active
                        week_num = week_data.get('week', 0)
                        if week_num not in player_entry['weeks_active']:
                            player_entry['weeks_active'].add(week_num)
                            player_entry['games_played'] += 1
                        
                        # Aggregate stats by category
                        if category not in player_entry['total_stats']:
                            player_entry['total_stats'][category] = {}
                        
                        # Aggregate numeric stats
                        for stat_key, stat_value in player_data.items():
                            if stat_key in ['player', 'name', 'team', 'stat_category', 'game_id']:
                                continue
                            
                            if isinstance(stat_value, (int, float)):
                                if stat_key not in player_entry['total_stats'][category]:
                                    player_entry['total_stats'][category][stat_key] = 0
                                player_entry['total_stats'][category][stat_key] += stat_value
                        
                        # Store weekly breakdown
                        player_entry['weekly_breakdown'].append({
                            'week': week_num,
                            'category': category,
                            'stats': player_data.copy()
                        })
        
        # Convert sets to counts and calculate averages
        for player_name, player_data in player_aggregates.items():
            player_data['weeks_active'] = len(player_data['weeks_active'])
            player_data['games_played'] = player_data['weeks_active']  # NFL: 1 game per week
            
            # Calculate per-game averages
            player_data['per_game_averages'] = self.calculate_per_game_averages(
                player_data['total_stats'], player_data['games_played']
            )
        
        return player_aggregates
    
    def determine_position_from_category(self, category: str) -> str:
        """Determine likely position from statistical category"""
        position_mapping = {
            'passing': 'QB',
            'rushing': 'RB',
            'receiving': 'WR',
            'kicking': 'K',
            'punting': 'P',
            'fumbles': 'RB',
            'interceptions': 'DEF',
            'kick returns': 'WR',
            'punt returns': 'WR'
        }
        return position_mapping.get(category.lower(), 'UNK')
    
    def calculate_per_game_averages(self, total_stats: Dict, games_played: int) -> Dict:
        """Calculate per-game averages for all statistical categories"""
        if games_played == 0:
            return {}
        
        averages = {}
        for category, stats in total_stats.items():
            averages[category] = {}
            for stat_key, total_value in stats.items():
                if isinstance(total_value, (int, float)):
                    averages[category][stat_key] = round(total_value / games_played, 2)
        
        return averages
    
    def generate_team_aggregates(self, weeks_data: List[Dict]) -> Dict[str, Dict]:
        """Generate team-level statistical aggregations"""
        team_aggregates = {}
        
        for team_abbr in self.nfl_teams:
            team_aggregates[team_abbr] = {
                'team': team_abbr,
                'games_played': 0,
                'weeks_active': set(),
                'offensive_stats': {
                    'total_points': 0,
                    'total_yards': 0,
                    'passing_yards': 0,
                    'rushing_yards': 0,
                    'touchdowns': 0,
                    'turnovers': 0
                },
                'defensive_stats': {
                    'points_allowed': 0,
                    'yards_allowed': 0,
                    'sacks': 0,
                    'interceptions': 0,
                    'fumbles_recovered': 0
                },
                'special_teams_stats': {
                    'field_goals_made': 0,
                    'field_goals_attempted': 0,
                    'extra_points_made': 0,
                    'punting_average': 0
                }
            }
        
        # Aggregate data from weeks
        for week_data in weeks_data:
            if 'teams' not in week_data:
                continue
            
            week_num = week_data.get('week', 0)
            
            for team_abbr, team_info in week_data['teams'].items():
                if team_abbr not in team_aggregates:
                    continue
                
                team_agg = team_aggregates[team_abbr]
                
                # Track weeks active
                if week_num not in team_agg['weeks_active']:
                    team_agg['weeks_active'].add(week_num)
                    team_agg['games_played'] += 1
                
                # Aggregate team stats
                if 'stats' in team_info:
                    self.aggregate_team_stats_from_players(team_agg, team_info['stats'])
        
        # Calculate per-game averages for teams
        for team_abbr, team_data in team_aggregates.items():
            team_data['weeks_active'] = len(team_data['weeks_active'])
            if team_data['games_played'] > 0:
                team_data['per_game_averages'] = self.calculate_team_per_game_averages(
                    team_data, team_data['games_played']
                )
        
        return team_aggregates
    
    def aggregate_team_stats_from_players(self, team_agg: Dict, player_stats: Dict):
        """Aggregate team statistics from individual player stats"""
        try:
            # Offensive stats from various categories
            if 'passing' in player_stats:
                for player in player_stats['passing']:
                    yards = player.get('yds', player.get('yards', 0))
                    tds = player.get('td', player.get('touchdowns', 0))
                    team_agg['offensive_stats']['passing_yards'] += yards if isinstance(yards, (int, float)) else 0
                    team_agg['offensive_stats']['touchdowns'] += tds if isinstance(tds, (int, float)) else 0
            
            if 'rushing' in player_stats:
                for player in player_stats['rushing']:
                    yards = player.get('yds', player.get('yards', 0))
                    tds = player.get('td', player.get('touchdowns', 0))
                    team_agg['offensive_stats']['rushing_yards'] += yards if isinstance(yards, (int, float)) else 0
                    team_agg['offensive_stats']['touchdowns'] += tds if isinstance(tds, (int, float)) else 0
            
            if 'kicking' in player_stats:
                for player in player_stats['kicking']:
                    fg_made = player.get('fg_made', 0)
                    fg_att = player.get('fg_att', 0)
                    xp_made = player.get('xp_made', 0)
                    pts = player.get('pts', 0)
                    
                    team_agg['special_teams_stats']['field_goals_made'] += fg_made if isinstance(fg_made, (int, float)) else 0
                    team_agg['special_teams_stats']['field_goals_attempted'] += fg_att if isinstance(fg_att, (int, float)) else 0
                    team_agg['special_teams_stats']['extra_points_made'] += xp_made if isinstance(xp_made, (int, float)) else 0
                    team_agg['offensive_stats']['total_points'] += pts if isinstance(pts, (int, float)) else 0
            
            # Calculate total yards
            team_agg['offensive_stats']['total_yards'] = (
                team_agg['offensive_stats']['passing_yards'] + 
                team_agg['offensive_stats']['rushing_yards']
            )
            
        except Exception as e:
            print(f"Error aggregating team stats: {e}")
    
    def calculate_team_per_game_averages(self, team_data: Dict, games_played: int) -> Dict:
        """Calculate per-game averages for team statistics"""
        if games_played == 0:
            return {}
        
        averages = {
            'offensive': {},
            'defensive': {},
            'special_teams': {}
        }
        
        # Offensive averages
        for stat, value in team_data['offensive_stats'].items():
            averages['offensive'][f"{stat}_per_game"] = round(value / games_played, 2)
        
        # Defensive averages
        for stat, value in team_data['defensive_stats'].items():
            averages['defensive'][f"{stat}_per_game"] = round(value / games_played, 2)
        
        # Special teams averages
        for stat, value in team_data['special_teams_stats'].items():
            if stat != 'punting_average':  # Already an average
                averages['special_teams'][f"{stat}_per_game"] = round(value / games_played, 2)
        
        return averages
    
    def generate_position_leaders(self, player_aggregates: Dict) -> Dict[str, List]:
        """Generate position leaders for key statistics"""
        leaders = {
            'passing_leaders': [],
            'rushing_leaders': [],
            'receiving_leaders': [],
            'scoring_leaders': [],
            'kicking_leaders': []
        }
        
        for player_name, player_data in player_aggregates.items():
            position = player_data.get('position', 'UNK')
            total_stats = player_data.get('total_stats', {})
            games_played = player_data.get('games_played', 1)
            
            # Passing leaders (QBs)
            if 'passing' in total_stats and position == 'QB':
                passing_stats = total_stats['passing']
                yards = passing_stats.get('yds', passing_stats.get('yards', 0))
                tds = passing_stats.get('td', passing_stats.get('touchdowns', 0))
                
                leaders['passing_leaders'].append({
                    'player': player_name,
                    'team': player_data['team'],
                    'position': position,
                    'total_yards': yards,
                    'total_touchdowns': tds,
                    'yards_per_game': round(yards / games_played, 1),
                    'touchdowns_per_game': round(tds / games_played, 2),
                    'games_played': games_played
                })
            
            # Rushing leaders (RBs)
            if 'rushing' in total_stats and position == 'RB':
                rushing_stats = total_stats['rushing']
                yards = rushing_stats.get('yds', rushing_stats.get('yards', 0))
                tds = rushing_stats.get('td', rushing_stats.get('touchdowns', 0))
                
                leaders['rushing_leaders'].append({
                    'player': player_name,
                    'team': player_data['team'],
                    'position': position,
                    'total_yards': yards,
                    'total_touchdowns': tds,
                    'yards_per_game': round(yards / games_played, 1),
                    'touchdowns_per_game': round(tds / games_played, 2),
                    'games_played': games_played
                })
            
            # Receiving leaders (WRs, TEs)
            if 'receiving' in total_stats and position in ['WR', 'TE']:
                receiving_stats = total_stats['receiving']
                yards = receiving_stats.get('yds', receiving_stats.get('yards', 0))
                tds = receiving_stats.get('td', receiving_stats.get('touchdowns', 0))
                receptions = receiving_stats.get('rec', receiving_stats.get('receptions', 0))
                
                leaders['receiving_leaders'].append({
                    'player': player_name,
                    'team': player_data['team'],
                    'position': position,
                    'total_yards': yards,
                    'total_touchdowns': tds,
                    'total_receptions': receptions,
                    'yards_per_game': round(yards / games_played, 1),
                    'receptions_per_game': round(receptions / games_played, 1),
                    'games_played': games_played
                })
        
        # Sort leaders by key stats
        leaders['passing_leaders'].sort(key=lambda x: x['total_yards'], reverse=True)
        leaders['rushing_leaders'].sort(key=lambda x: x['total_yards'], reverse=True)
        leaders['receiving_leaders'].sort(key=lambda x: x['total_yards'], reverse=True)
        
        # Limit to top 10 each
        for category in leaders:
            leaders[category] = leaders[category][:10]
        
        return leaders
    
    def generate_rolling_stats(self, weeks_to_include: List[int], season: int = 2025, 
                             window_name: str = "custom") -> Dict[str, Any]:
        """Generate rolling statistics for specified weeks"""
        print(f"🏈 Generating {window_name} rolling stats for weeks {weeks_to_include}")
        
        # Load data for specified weeks
        weeks_data = []
        for week in weeks_to_include:
            week_data = self.load_weekly_data(week, season)
            if week_data:
                weeks_data.append(week_data)
        
        if not weeks_data:
            print(f"⚠️  No data found for weeks {weeks_to_include}")
            return {}
        
        # Generate aggregations
        player_aggregates = self.aggregate_player_stats(weeks_data)
        team_aggregates = self.generate_team_aggregates(weeks_data)
        position_leaders = self.generate_position_leaders(player_aggregates)
        
        rolling_stats = {
            'window_name': window_name,
            'weeks_included': weeks_to_include,
            'season': season,
            'total_weeks': len(weeks_to_include),
            'generated_date': datetime.now().isoformat(),
            'player_aggregates': player_aggregates,
            'team_aggregates': team_aggregates,
            'position_leaders': position_leaders,
            'summary': {
                'total_players': len(player_aggregates),
                'total_teams': len([t for t in team_aggregates.values() if t['games_played'] > 0]),
                'weeks_processed': len(weeks_data)
            }
        }
        
        return rolling_stats
    
    def save_rolling_stats(self, rolling_stats: Dict, filename: str):
        """Save rolling statistics to JSON file"""
        output_file = self.output_dir / filename
        
        with open(output_file, 'w') as f:
            json.dump(rolling_stats, f, indent=2, default=str)
        
        print(f"📄 Saved rolling stats: {output_file}")
    
    def generate_all_rolling_windows(self, current_week: int = 1, season: int = 2025):
        """Generate all standard rolling statistics windows"""
        print("🏈 NFL Rolling Stats Generation Started")
        print("=" * 50)
        
        # Current week only
        current_week_stats = self.generate_rolling_stats(
            [current_week], season, f"week_{current_week}"
        )
        if current_week_stats:
            self.save_rolling_stats(current_week_stats, f"week_{current_week}_{season}.json")
        
        # Season to date (all weeks up to current)
        if current_week > 1:
            season_weeks = list(range(1, current_week + 1))
            season_stats = self.generate_rolling_stats(
                season_weeks, season, "season_to_date"
            )
            if season_stats:
                self.save_rolling_stats(season_stats, f"season_to_date_{season}.json")
        
        # Last 4 weeks (if enough data)
        if current_week >= 4:
            last_4_weeks = list(range(max(1, current_week - 3), current_week + 1))
            last_4_stats = self.generate_rolling_stats(
                last_4_weeks, season, "last_4_weeks"
            )
            if last_4_stats:
                self.save_rolling_stats(last_4_stats, f"last_4_weeks_{season}.json")
        
        # Last 2 weeks (if enough data)
        if current_week >= 2:
            last_2_weeks = list(range(max(1, current_week - 1), current_week + 1))
            last_2_stats = self.generate_rolling_stats(
                last_2_weeks, season, "last_2_weeks"
            )
            if last_2_stats:
                self.save_rolling_stats(last_2_stats, f"last_2_weeks_{season}.json")
        
        print("\n🎯 NFL Rolling Stats Generation Complete!")
        
        return {
            'current_week': current_week,
            'season': season,
            'files_generated': [
                f"week_{current_week}_{season}.json",
                f"season_to_date_{season}.json" if current_week > 1 else None,
                f"last_4_weeks_{season}.json" if current_week >= 4 else None,
                f"last_2_weeks_{season}.json" if current_week >= 2 else None
            ]
        }

def main():
    generator = NFLRollingStatsGenerator()
    
    # Generate rolling stats for current available data
    result = generator.generate_all_rolling_windows(current_week=1, season=2025)
    
    print(f"\n📊 Rolling Stats Summary:")
    print(f"Current Week: {result['current_week']}")
    print(f"Season: {result['season']}")
    print(f"Files Generated: {len([f for f in result['files_generated'] if f])}")

if __name__ == "__main__":
    main()