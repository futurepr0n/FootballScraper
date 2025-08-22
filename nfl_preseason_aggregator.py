#!/usr/bin/env python3
"""
NFL Preseason Data Aggregator
Processes backfilled preseason data to create organized summaries for dashboard components
Creates leaderboards, team stats, and player performance summaries
"""

import json
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NFLPreseasonAggregator:
    def __init__(self):
        self.football_data_dir = Path("../FootballData")
        self.preseason_dir = self.football_data_dir / "data" / "preseason"
        self.output_dir = self.football_data_dir / "data" / "preseason" / "aggregated"
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistical categories for aggregation
        self.stat_categories = {
            'passing': ['yards', 'touchdowns', 'interceptions', 'completions', 'attempts', 'rating'],
            'rushing': ['yards', 'touchdowns', 'attempts', 'long'],
            'receiving': ['yards', 'touchdowns', 'receptions', 'targets', 'long'],
            'defense': ['tackles', 'sacks', 'interceptions', 'forced_fumbles'],
            'kicking': ['field_goals_made', 'field_goals_attempted', 'extra_points_made', 'extra_points_attempted']
        }
        
    def load_all_preseason_games(self) -> List[Dict]:
        """Load all processed preseason game data"""
        games_data = []
        
        # Look for comprehensive game files
        complete_files = list(self.preseason_dir.glob("*_complete.json"))
        
        logger.info(f"Found {len(complete_files)} complete game files")
        
        for file_path in complete_files:
            try:
                with open(file_path, 'r') as f:
                    game_data = json.load(f)
                    games_data.append(game_data)
                    
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
        
        return games_data
    
    def aggregate_player_stats(self, games_data: List[Dict]) -> Dict:
        """Aggregate player statistics across all preseason games"""
        player_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        player_info = {}
        
        for game in games_data:
            player_performance = game.get('player_performance', {})
            
            for team_abbr, players in player_performance.items():
                for player_id, player_data in players.items():
                    player_name = player_data.get('name', '')
                    player_position = player_data.get('position', '')
                    
                    # Store player info
                    if player_id not in player_info:
                        player_info[player_id] = {
                            'name': player_name,
                            'position': player_position,
                            'team': team_abbr
                        }
                    
                    # Aggregate stats by category
                    for category, stat_data in player_data.get('stats', {}).items():
                        labels = stat_data.get('labels', [])
                        values = stat_data.get('values', [])
                        
                        # Map labels to values and aggregate
                        for i, label in enumerate(labels):
                            if i < len(values) and values[i] != '--':
                                try:
                                    # Clean and convert value
                                    value = str(values[i]).replace(',', '')
                                    if '.' in value:
                                        numeric_value = float(value)
                                    else:
                                        numeric_value = int(value)
                                    
                                    player_stats[player_id][category][label.lower().replace(' ', '_')] += numeric_value
                                    
                                except (ValueError, TypeError):
                                    continue
        
        # Convert to regular dict and add player info
        result = {}
        for player_id, categories in player_stats.items():
            result[player_id] = {
                'info': player_info.get(player_id, {}),
                'stats': dict(categories)
            }
        
        return result
    
    def create_touchdown_leaderboard(self, player_stats: Dict) -> List[Dict]:
        """Create touchdown leaderboard across all categories"""
        touchdown_leaders = []
        
        for player_id, data in player_stats.items():
            player_info = data.get('info', {})
            stats = data.get('stats', {})
            
            total_tds = 0
            td_breakdown = {}
            
            # Passing touchdowns
            passing_tds = stats.get('passing', {}).get('td', 0)
            if passing_tds > 0:
                td_breakdown['passing'] = int(passing_tds)
                total_tds += int(passing_tds)
            
            # Rushing touchdowns  
            rushing_tds = stats.get('rushing', {}).get('td', 0)
            if rushing_tds > 0:
                td_breakdown['rushing'] = int(rushing_tds)
                total_tds += int(rushing_tds)
            
            # Receiving touchdowns
            receiving_tds = stats.get('receiving', {}).get('td', 0)
            if receiving_tds > 0:
                td_breakdown['receiving'] = int(receiving_tds)
                total_tds += int(receiving_tds)
            
            if total_tds > 0:
                touchdown_leaders.append({
                    'player_id': player_id,
                    'name': player_info.get('name', ''),
                    'position': player_info.get('position', ''),
                    'team': player_info.get('team', ''),
                    'total_touchdowns': total_tds,
                    'touchdown_breakdown': td_breakdown
                })
        
        # Sort by total touchdowns
        touchdown_leaders.sort(key=lambda x: x['total_touchdowns'], reverse=True)
        
        return touchdown_leaders
    
    def create_interception_leaderboard(self, player_stats: Dict) -> List[Dict]:
        """Create interception leaderboard (both thrown and caught)"""
        interception_data = {
            'defensive_interceptions': [],
            'quarterback_interceptions': []
        }
        
        for player_id, data in player_stats.items():
            player_info = data.get('info', {})
            stats = data.get('stats', {})
            
            # Defensive interceptions
            def_ints = stats.get('interceptions', {}).get('int', 0) or stats.get('defense', {}).get('int', 0)
            if def_ints > 0:
                interception_data['defensive_interceptions'].append({
                    'player_id': player_id,
                    'name': player_info.get('name', ''),
                    'position': player_info.get('position', ''),
                    'team': player_info.get('team', ''),
                    'interceptions': int(def_ints)
                })
            
            # Quarterback interceptions (thrown)
            qb_ints = stats.get('passing', {}).get('int', 0)
            if qb_ints > 0:
                interception_data['quarterback_interceptions'].append({
                    'player_id': player_id,
                    'name': player_info.get('name', ''),
                    'position': player_info.get('position', ''),
                    'team': player_info.get('team', ''),
                    'interceptions_thrown': int(qb_ints)
                })
        
        # Sort both lists
        interception_data['defensive_interceptions'].sort(key=lambda x: x['interceptions'], reverse=True)
        interception_data['quarterback_interceptions'].sort(key=lambda x: x['interceptions_thrown'], reverse=True)
        
        return interception_data
    
    def create_position_leaders(self, player_stats: Dict) -> Dict:
        """Create leaderboards by position"""
        position_leaders = defaultdict(list)
        
        for player_id, data in player_stats.items():
            player_info = data.get('info', {})
            stats = data.get('stats', {})
            position = player_info.get('position', '')
            
            if not position:
                continue
            
            player_summary = {
                'player_id': player_id,
                'name': player_info.get('name', ''),
                'team': player_info.get('team', ''),
                'position': position
            }
            
            # Add relevant stats based on position
            if position == 'QB':
                passing_stats = stats.get('passing', {})
                player_summary.update({
                    'passing_yards': int(passing_stats.get('yds', 0) or 0),
                    'passing_tds': int(passing_stats.get('td', 0) or 0),
                    'interceptions': int(passing_stats.get('int', 0) or 0),
                    'completions': int(passing_stats.get('c/att', '0/0').split('/')[0] or 0),
                    'attempts': int(passing_stats.get('c/att', '0/0').split('/')[1] if '/' in str(passing_stats.get('c/att', '0/0')) else 0),
                    'rating': float(passing_stats.get('rtg', 0) or 0)
                })
                
            elif position in ['RB', 'FB']:
                rushing_stats = stats.get('rushing', {})
                player_summary.update({
                    'rushing_yards': int(rushing_stats.get('yds', 0) or 0),
                    'rushing_tds': int(rushing_stats.get('td', 0) or 0),
                    'rushing_attempts': int(rushing_stats.get('car', 0) or 0),
                    'yards_per_carry': float(rushing_stats.get('avg', 0) or 0)
                })
                
            elif position in ['WR', 'TE']:
                receiving_stats = stats.get('receiving', {})
                player_summary.update({
                    'receiving_yards': int(receiving_stats.get('yds', 0) or 0),
                    'receiving_tds': int(receiving_stats.get('td', 0) or 0),
                    'receptions': int(receiving_stats.get('rec', 0) or 0),
                    'targets': int(receiving_stats.get('tgt', 0) or 0),
                    'yards_per_reception': float(receiving_stats.get('avg', 0) or 0)
                })
            
            position_leaders[position].append(player_summary)
        
        # Sort each position group by primary stat
        sort_keys = {
            'QB': 'passing_yards',
            'RB': 'rushing_yards', 
            'FB': 'rushing_yards',
            'WR': 'receiving_yards',
            'TE': 'receiving_yards'
        }
        
        for position, players in position_leaders.items():
            sort_key = sort_keys.get(position, 'name')
            if sort_key != 'name':
                position_leaders[position] = sorted(players, 
                                                 key=lambda x: x.get(sort_key, 0), 
                                                 reverse=True)
        
        return dict(position_leaders)
    
    def create_team_summaries(self, games_data: List[Dict]) -> Dict:
        """Create team-level summaries and statistics"""
        team_stats = defaultdict(lambda: defaultdict(int))
        team_games = defaultdict(int)
        
        for game in games_data:
            game_summary = game.get('game_summary', {})
            teams = game_summary.get('teams', [])
            
            for team in teams:
                team_abbr = team.get('abbreviation', '')
                if not team_abbr:
                    continue
                
                team_games[team_abbr] += 1
                team_stats[team_abbr]['games'] += 1
                team_stats[team_abbr]['points'] += int(team.get('score', 0) or 0)
                
                if team.get('winner', False):
                    team_stats[team_abbr]['wins'] += 1
                else:
                    team_stats[team_abbr]['losses'] += 1
        
        # Calculate averages and create summaries
        team_summaries = {}
        for team_abbr, stats in team_stats.items():
            games_played = stats.get('games', 1)
            
            team_summaries[team_abbr] = {
                'team': team_abbr,
                'games_played': games_played,
                'wins': stats.get('wins', 0),
                'losses': stats.get('losses', 0),
                'win_percentage': round(stats.get('wins', 0) / games_played, 3),
                'total_points': stats.get('points', 0),
                'points_per_game': round(stats.get('points', 0) / games_played, 1)
            }
        
        return team_summaries
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate comprehensive preseason report"""
        logger.info("Loading all preseason games...")
        games_data = self.load_all_preseason_games()
        
        if not games_data:
            logger.error("No preseason game data found")
            return {}
        
        logger.info(f"Processing {len(games_data)} preseason games...")
        
        # Aggregate all player statistics
        logger.info("Aggregating player statistics...")
        player_stats = self.aggregate_player_stats(games_data)
        
        # Create various leaderboards
        logger.info("Creating touchdown leaderboard...")
        touchdown_leaders = self.create_touchdown_leaderboard(player_stats)
        
        logger.info("Creating interception data...")
        interception_data = self.create_interception_leaderboard(player_stats)
        
        logger.info("Creating position leaders...")
        position_leaders = self.create_position_leaders(player_stats)
        
        logger.info("Creating team summaries...")
        team_summaries = self.create_team_summaries(games_data)
        
        # Compile comprehensive report
        report = {
            'metadata': {
                'generated_timestamp': datetime.now().isoformat(),
                'total_games_processed': len(games_data),
                'total_players_tracked': len(player_stats),
                'teams_involved': len(team_summaries)
            },
            'leaderboards': {
                'touchdowns': touchdown_leaders[:25],  # Top 25
                'interceptions': interception_data,
                'by_position': position_leaders
            },
            'team_summaries': team_summaries,
            'player_stats_summary': {
                'total_players': len(player_stats),
                'quarterbacks': len([p for p in player_stats.values() 
                                   if p.get('info', {}).get('position') == 'QB']),
                'running_backs': len([p for p in player_stats.values() 
                                    if p.get('info', {}).get('position') == 'RB']),
                'wide_receivers': len([p for p in player_stats.values() 
                                     if p.get('info', {}).get('position') == 'WR']),
                'tight_ends': len([p for p in player_stats.values() 
                                 if p.get('info', {}).get('position') == 'TE'])
            }
        }
        
        return report
    
    def save_aggregated_data(self):
        """Save all aggregated data files"""
        logger.info("Generating comprehensive preseason report...")
        
        # Generate main report
        report = self.generate_comprehensive_report()
        
        if not report:
            logger.error("Failed to generate report")
            return False
        
        # Save main report
        main_report_file = self.output_dir / "preseason_2025_report.json"
        with open(main_report_file, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Saved main report: {main_report_file}")
        
        # Save individual components for easy access
        components = {
            'touchdown_leaders.json': report['leaderboards']['touchdowns'],
            'interception_data.json': report['leaderboards']['interceptions'],
            'position_leaders.json': report['leaderboards']['by_position'],
            'team_summaries.json': report['team_summaries'],
            'metadata.json': report['metadata']
        }
        
        for filename, data in components.items():
            file_path = self.output_dir / filename
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved component: {file_path}")
        
        # Create dashboard-ready summary
        dashboard_summary = {
            'last_updated': report['metadata']['generated_timestamp'],
            'games_processed': report['metadata']['total_games_processed'],
            'top_touchdown_scorers': report['leaderboards']['touchdowns'][:10],
            'top_defensive_interceptions': report['leaderboards']['interceptions']['defensive_interceptions'][:10],
            'top_quarterbacks': report['leaderboards']['by_position'].get('QB', [])[:10],
            'top_running_backs': report['leaderboards']['by_position'].get('RB', [])[:10],
            'top_receivers': report['leaderboards']['by_position'].get('WR', [])[:10],
            'team_standings': sorted(report['team_summaries'].values(), 
                                   key=lambda x: x['win_percentage'], reverse=True)
        }
        
        dashboard_file = self.output_dir / "dashboard_summary.json"
        with open(dashboard_file, 'w') as f:
            json.dump(dashboard_summary, f, indent=2)
        logger.info(f"Saved dashboard summary: {dashboard_file}")
        
        return True

def main():
    print("🏈 NFL Preseason Data Aggregator")
    print("=" * 50)
    
    aggregator = NFLPreseasonAggregator()
    
    # Check if preseason data exists
    if not aggregator.preseason_dir.exists():
        print("❌ No preseason data directory found")
        print("Run nfl_preseason_backfill.py first to collect the data")
        return
    
    # Count available game files
    complete_files = list(aggregator.preseason_dir.glob("*_complete.json"))
    print(f"📊 Found {len(complete_files)} complete game files to process")
    
    if len(complete_files) == 0:
        print("❌ No complete game files found")
        print("Run nfl_preseason_backfill.py first to collect the data")
        return
    
    print(f"🎯 Data will be aggregated and saved to: {aggregator.output_dir}")
    print()
    
    confirm = input("Proceed with aggregation? (y/n): ").lower()
    if not confirm.startswith('y'):
        print("Aggregation cancelled")
        return
    
    # Run aggregation
    try:
        success = aggregator.save_aggregated_data()
        
        if success:
            print("\n✅ Aggregation completed successfully!")
            print("\n📋 Generated files:")
            print("   • preseason_2025_report.json - Complete report")
            print("   • touchdown_leaders.json - TD leaderboard")
            print("   • interception_data.json - INT statistics")
            print("   • position_leaders.json - Leaders by position")
            print("   • team_summaries.json - Team statistics")
            print("   • dashboard_summary.json - Ready for dashboard")
            print(f"\n💾 All files saved to: {aggregator.output_dir}")
        else:
            print("❌ Aggregation failed - check logs for details")
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logger.error(f"Aggregation error: {e}")

if __name__ == "__main__":
    main()