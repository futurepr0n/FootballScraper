#!/usr/bin/env python3
"""
NFL Preseason Data Backfill System
Comprehensive ESPN API integration for box scores and play-by-play data
Created for Football platform dashboard components
"""

import json
import requests
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('preseason_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NFLPreseasonBackfill:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Data paths
        self.root_path = Path('/Users/futurepr0n/Development/Capping.Pro/Claude-Code')
        self.scraper_path = self.root_path / 'FootballScraper'
        self.data_path = self.root_path / 'FootballData' / 'data' / 'preseason'
        
        # Create data directories
        self.data_path.mkdir(parents=True, exist_ok=True)
        (self.data_path / 'box_scores').mkdir(exist_ok=True)
        (self.data_path / 'play_by_play').mkdir(exist_ok=True)
        (self.data_path / 'comprehensive').mkdir(exist_ok=True)
        (self.data_path / 'aggregated').mkdir(exist_ok=True)
        
        # ESPN API endpoints
        self.api_base = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"
        
        # Rate limiting
        self.request_delay = 1.5
        self.processed_games = set()
        
        # Data collectors
        self.player_stats = defaultdict(lambda: defaultdict(int))
        self.team_stats = defaultdict(lambda: defaultdict(int))
        self.game_results = []
        
    def load_preseason_games(self) -> List[Dict]:
        """Load all preseason games from date files"""
        preseason_games = []
        
        # Get all August JSON files (preseason month)
        schedule_files = list(self.scraper_path.glob('august_*.json'))
        
        logger.info(f"Found {len(schedule_files)} August date files")
        
        for file_path in schedule_files:
            try:
                with open(file_path, 'r') as f:
                    date_data = json.load(f)
                
                games = date_data.get('games', [])
                preseason_only = [g for g in games if g.get('season_type_name') == 'preseason']
                
                if preseason_only:
                    logger.info(f"{file_path.name}: {len(preseason_only)} preseason games")
                    preseason_games.extend(preseason_only)
                    
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
        
        logger.info(f"Total preseason games loaded: {len(preseason_games)}")
        return preseason_games
    
    def fetch_game_data(self, game_id: str) -> Optional[Dict]:
        """Fetch comprehensive game data from ESPN API"""
        try:
            url = f"{self.api_base}?event={game_id}"
            
            logger.info(f"Fetching data for game {game_id}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            time.sleep(self.request_delay)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching game {game_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for game {game_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching game {game_id}: {e}")
            return None
    
    def extract_box_score_data(self, game_data: Dict) -> Dict:
        """Extract box score statistics"""
        try:
            box_score = {
                'game_info': {},
                'team_stats': {},
                'player_stats': {},
                'scoring_summary': []
            }
            
            # Extract game info
            header = game_data.get('header', {})
            competition = header.get('competitions', [{}])[0]
            
            box_score['game_info'] = {
                'game_id': header.get('id', ''),
                'date': competition.get('date', ''),
                'status': competition.get('status', {}),
                'venue': competition.get('venue', {}),
                'attendance': competition.get('attendance', 0)
            }
            
            # Extract team stats
            competitors = competition.get('competitors', [])
            for competitor in competitors:
                team_abbrev = competitor.get('team', {}).get('abbreviation', '')
                if team_abbrev:
                    box_score['team_stats'][team_abbrev] = {
                        'score': int(competitor.get('score', 0)),
                        'record': competitor.get('records', [{}])[0].get('summary', '0-0'),
                        'home_away': competitor.get('homeAway', '')
                    }
            
            # Extract player stats from boxscore
            boxscore_data = game_data.get('boxscore', {})
            if boxscore_data:
                players = boxscore_data.get('players', [])
                for team_data in players:
                    team_abbrev = team_data.get('team', {}).get('abbreviation', '')
                    if not team_abbrev:
                        continue
                    
                    # Process different stat categories
                    for category in team_data.get('statistics', []):
                        category_name = category.get('name', '')
                        athletes = category.get('athletes', [])
                        
                        for athlete_data in athletes:
                            athlete = athlete_data.get('athlete', {})
                            stats = athlete_data.get('stats', [])
                            
                            player_info = {
                                'name': athlete.get('displayName', ''),
                                'position': athlete.get('position', {}).get('abbreviation', ''),
                                'jersey': athlete.get('jersey', ''),
                                'team': team_abbrev,
                                'stats': {}
                            }
                            
                            # Map stats to readable format
                            for stat in stats:
                                stat_value = stat
                                if stat_value and stat_value != '--':
                                    try:
                                        stat_value = float(stat_value) if '.' in str(stat_value) else int(stat_value)
                                    except ValueError:
                                        pass
                                    player_info['stats'][len(player_info['stats'])] = stat_value
                            
                            if player_info['name']:
                                player_key = f"{player_info['name']}_{team_abbrev}"
                                box_score['player_stats'][player_key] = player_info
            
            return box_score
            
        except Exception as e:
            logger.error(f"Error extracting box score: {e}")
            return {'game_info': {}, 'team_stats': {}, 'player_stats': {}, 'scoring_summary': []}
    
    def extract_play_by_play_data(self, game_data: Dict) -> Dict:
        """Extract play-by-play data using existing logic"""
        try:
            pbp_data = {
                'game_info': {},
                'drives': [],
                'plays': [],
                'scoring_plays': [],
                'touchdowns': [],
                'interceptions': [],
                'fumbles': []
            }
            
            # Extract basic game info
            header = game_data.get('header', {})
            pbp_data['game_info'] = {
                'game_id': header.get('id', ''),
                'teams': [],
                'final_score': {}
            }
            
            # Extract team info
            competition = header.get('competitions', [{}])[0]
            competitors = competition.get('competitors', [])
            
            for competitor in competitors:
                team = competitor.get('team', {})
                pbp_data['game_info']['teams'].append({
                    'abbreviation': team.get('abbreviation', ''),
                    'name': team.get('displayName', ''),
                    'score': int(competitor.get('score', 0)),
                    'home_away': competitor.get('homeAway', '')
                })
            
            # Process drives and plays
            drives_data = game_data.get('drives', {})
            all_drives = drives_data.get('previous', []) + [drives_data.get('current', {})]
            
            for drive in all_drives:
                if not drive:
                    continue
                
                drive_info = {
                    'id': drive.get('id', ''),
                    'team': drive.get('team', {}).get('abbreviation', ''),
                    'plays_count': len(drive.get('plays', [])),
                    'result': drive.get('result', ''),
                    'yards': drive.get('yards', 0)
                }
                pbp_data['drives'].append(drive_info)
                
                # Process plays in this drive
                plays = drive.get('plays', [])
                for play in plays:
                    play_info = {
                        'id': play.get('id', ''),
                        'text': play.get('text', ''),
                        'type': play.get('type', {}).get('text', ''),
                        'down': play.get('start', {}).get('down', 0),
                        'distance': play.get('start', {}).get('distance', 0),
                        'yard_line': play.get('start', {}).get('yardLine', 0),
                        'period': play.get('period', {}).get('number', 0),
                        'clock': play.get('clock', {}).get('displayValue', ''),
                        'team': play.get('start', {}).get('team', {}).get('abbreviation', ''),
                        'scoring_play': play.get('scoringPlay', False)
                    }
                    
                    pbp_data['plays'].append(play_info)
                    
                    # Categorize special plays
                    play_text = play_info['text'].lower()
                    
                    if play_info['scoring_play'] and 'touchdown' in play_text:
                        # Extract touchdown info
                        td_info = {
                            'play_id': play_info['id'],
                            'text': play_info['text'],
                            'team': play_info['team'],
                            'period': play_info['period'],
                            'players_involved': self.extract_players_from_play(play)
                        }
                        pbp_data['touchdowns'].append(td_info)
                    
                    if 'interception' in play_text or 'intercepted' in play_text:
                        int_info = {
                            'play_id': play_info['id'],
                            'text': play_info['text'],
                            'team': play_info['team'],  # Team that threw the INT
                            'period': play_info['period'],
                            'players_involved': self.extract_players_from_play(play)
                        }
                        pbp_data['interceptions'].append(int_info)
            
            return pbp_data
            
        except Exception as e:
            logger.error(f"Error extracting play-by-play: {e}")
            return {'game_info': {}, 'drives': [], 'plays': [], 'scoring_plays': [], 'touchdowns': [], 'interceptions': []}
    
    def extract_players_from_play(self, play: Dict) -> List[Dict]:
        """Extract player information from a play"""
        players = []
        participants = play.get('participants', [])
        
        for participant in participants:
            athlete = participant.get('athlete', {})
            if athlete:
                players.append({
                    'id': athlete.get('id', ''),
                    'name': athlete.get('displayName', ''),
                    'jersey': athlete.get('jersey', ''),
                    'position': athlete.get('position', {}).get('abbreviation', ''),
                    'team': participant.get('team', {}).get('abbreviation', '')
                })
        
        return players
    
    def save_game_data(self, game_id: str, date_str: str, box_score: Dict, pbp_data: Dict):
        """Save processed game data to files"""
        try:
            # Save box score
            box_score_file = self.data_path / 'box_scores' / f"{date_str}_{game_id}_box_score.json"
            with open(box_score_file, 'w') as f:
                json.dump(box_score, f, indent=2)
            
            # Save play-by-play
            pbp_file = self.data_path / 'play_by_play' / f"{date_str}_{game_id}_play_by_play.json"
            with open(pbp_file, 'w') as f:
                json.dump(pbp_data, f, indent=2)
            
            # Save comprehensive combined data
            comprehensive_data = {
                'game_id': game_id,
                'date': date_str,
                'box_score': box_score,
                'play_by_play': pbp_data,
                'processing_timestamp': datetime.now().isoformat()
            }
            
            comp_file = self.data_path / 'comprehensive' / f"{date_str}_{game_id}_complete.json"
            with open(comp_file, 'w') as f:
                json.dump(comprehensive_data, f, indent=2)
            
            logger.info(f"Saved game {game_id} data to all formats")
            
        except Exception as e:
            logger.error(f"Error saving game {game_id}: {e}")
    
    def process_all_preseason_games(self):
        """Main processing function for all preseason games"""
        logger.info("🏈 Starting NFL Preseason Data Backfill")
        logger.info("=" * 60)
        
        # Load all preseason games
        games = self.load_preseason_games()
        if not games:
            logger.error("No preseason games found!")
            return
        
        total_games = len(games)
        processed_count = 0
        success_count = 0
        
        for i, game in enumerate(games, 1):
            game_id = game.get('game_id', '')
            if not game_id:
                logger.warning(f"Game {i}/{total_games}: No game_id found")
                continue
            
            if game_id in self.processed_games:
                logger.info(f"Game {i}/{total_games}: Already processed {game_id}")
                continue
            
            # Extract date for file organization
            game_date = game.get('date', '2025-08-01')
            try:
                date_obj = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
                date_str = date_obj.strftime('%Y-%m-%d')
            except:
                date_str = '2025-08-01'
            
            logger.info(f"Processing game {i}/{total_games}: {game.get('matchup', 'Unknown')} ({game_id})")
            
            # Fetch game data
            game_data = self.fetch_game_data(game_id)
            if not game_data:
                logger.warning(f"Failed to fetch data for game {game_id}")
                processed_count += 1
                continue
            
            # Extract data
            box_score = self.extract_box_score_data(game_data)
            pbp_data = self.extract_play_by_play_data(game_data)
            
            # Save data
            self.save_game_data(game_id, date_str, box_score, pbp_data)
            
            # Track progress
            self.processed_games.add(game_id)
            processed_count += 1
            success_count += 1
            
            # Update aggregated stats
            self.update_aggregated_stats(box_score, pbp_data)
            
            # Progress update
            if processed_count % 5 == 0:
                logger.info(f"Progress: {processed_count}/{total_games} games processed")
        
        # Generate final summary
        self.generate_summary_report(total_games, success_count)
        logger.info("🎉 Preseason backfill completed!")
    
    def update_aggregated_stats(self, box_score: Dict, pbp_data: Dict):
        """Update running aggregated statistics"""
        try:
            # Update player stats from play-by-play touchdowns
            for td in pbp_data.get('touchdowns', []):
                for player in td.get('players_involved', []):
                    if player.get('name'):
                        key = f"{player['name']}_{player.get('team', 'UNK')}"
                        self.player_stats[key]['touchdowns'] += 1
                        self.player_stats[key]['team'] = player.get('team', 'UNK')
                        self.player_stats[key]['position'] = player.get('position', 'UNK')
                        self.player_stats[key]['name'] = player.get('name', 'Unknown')
            
            # Update interception stats
            for int_play in pbp_data.get('interceptions', []):
                for player in int_play.get('players_involved', []):
                    if player.get('name'):
                        key = f"{player['name']}_{player.get('team', 'UNK')}"
                        # This is simplified - in real implementation, need to distinguish
                        # between who threw the INT vs who caught it
                        self.player_stats[key]['interceptions'] += 1
                        self.player_stats[key]['team'] = player.get('team', 'UNK')
                        self.player_stats[key]['position'] = player.get('position', 'UNK')
                        self.player_stats[key]['name'] = player.get('name', 'Unknown')
            
            # Update team stats
            teams = pbp_data.get('game_info', {}).get('teams', [])
            if len(teams) >= 2:
                game_result = {
                    'teams': teams,
                    'touchdowns': len(pbp_data.get('touchdowns', [])),
                    'interceptions': len(pbp_data.get('interceptions', [])),
                    'date': pbp_data.get('game_info', {}).get('game_id', '')
                }
                self.game_results.append(game_result)
            
        except Exception as e:
            logger.error(f"Error updating aggregated stats: {e}")
    
    def generate_summary_report(self, total_games: int, success_count: int):
        """Generate final summary and leaderboard files"""
        logger.info("Generating summary reports...")
        
        try:
            # Create dashboard summary
            dashboard_data = {
                'generated_timestamp': datetime.now().isoformat(),
                'total_games_processed': success_count,
                'total_games_attempted': total_games,
                'touchdown_leaders': [],
                'interception_data': [],
                'position_leaders': {},
                'processing_summary': {
                    'success_rate': f"{success_count/total_games*100:.1f}%" if total_games > 0 else "0%",
                    'data_types': ['box_scores', 'play_by_play', 'comprehensive'],
                    'files_created': success_count * 3  # 3 files per game
                }
            }
            
            # Generate touchdown leaderboard
            td_leaders = []
            for player_key, stats in self.player_stats.items():
                if stats.get('touchdowns', 0) > 0:
                    td_leaders.append({
                        'name': stats.get('name', 'Unknown'),
                        'team': stats.get('team', 'UNK'),
                        'position': stats.get('position', 'UNK'),
                        'touchdowns': stats.get('touchdowns', 0)
                    })
            
            td_leaders.sort(key=lambda x: x['touchdowns'], reverse=True)
            dashboard_data['touchdown_leaders'] = td_leaders[:10]
            
            # Generate interception data
            int_leaders = []
            for player_key, stats in self.player_stats.items():
                if stats.get('interceptions', 0) > 0:
                    int_leaders.append({
                        'name': stats.get('name', 'Unknown'),
                        'team': stats.get('team', 'UNK'),
                        'position': stats.get('position', 'UNK'),
                        'interceptions': stats.get('interceptions', 0)
                    })
            
            int_leaders.sort(key=lambda x: x['interceptions'], reverse=True)
            dashboard_data['interception_data'] = int_leaders
            
            # Save dashboard summary
            dashboard_file = self.data_path / 'aggregated' / 'dashboard_summary.json'
            with open(dashboard_file, 'w') as f:
                json.dump(dashboard_data, f, indent=2)
            
            # Save individual leaderboards
            td_file = self.data_path / 'aggregated' / 'touchdown_leaders.json'
            with open(td_file, 'w') as f:
                json.dump(td_leaders, f, indent=2)
            
            int_file = self.data_path / 'aggregated' / 'interception_data.json'
            with open(int_file, 'w') as f:
                json.dump(int_leaders, f, indent=2)
            
            logger.info(f"Dashboard data saved with {len(td_leaders)} TD leaders and {len(int_leaders)} INT leaders")
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")

def main():
    backfill = NFLPreseasonBackfill()
    backfill.process_all_preseason_games()

if __name__ == "__main__":
    main()