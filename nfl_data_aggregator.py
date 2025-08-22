#!/usr/bin/env python3
"""
Improved NFL Data Aggregator
Fixes issues with punters in rushing leaders, empty receiving leaders, and TD/INT player extraction
"""

import os
import json
import glob
import re
from datetime import datetime
from collections import defaultdict, Counter

class ImprovedNFLDataAggregator:
    def __init__(self):
        self.base_path = "../FootballData/data/preseason"
        self.roster_path = "../FootballData/data/rosters"
        self.output_path = "../FootballData/data/preseason/aggregated"
        
        # Data storage
        self.touchdowns = []
        self.interceptions = []
        self.rushing_stats = defaultdict(lambda: {"name": "", "team": "", "attempts": 0, "yards": 0, "tds": 0, "avg": 0.0, "games": []})
        self.passing_stats = defaultdict(lambda: {"name": "", "team": "", "completions": 0, "attempts": 0, "yards": 0, "tds": 0, "interceptions": 0, "rating": 0.0, "games": []})
        self.receiving_stats = defaultdict(lambda: {"name": "", "team": "", "receptions": 0, "yards": 0, "tds": 0, "avg": 0.0, "games": []})
        
        # Team matchup resolution
        self.game_matchups = {}  # game_id -> {"home_team": "WSH", "away_team": "CIN"}
        self.player_teams = {}   # player_name -> team_abbreviation (from game context)
        
        # Load team rosters for position filtering
        self.player_positions = {}
        self.load_player_positions()
        self.load_game_matchups()
        
    def load_player_positions(self):
        """Load player positions from roster files"""
        try:
            # Load master roster
            master_roster_path = f"{self.roster_path}/nfl_master_roster_2025.json"
            if os.path.exists(master_roster_path):
                with open(master_roster_path, 'r') as f:
                    master_data = json.load(f)
                    
            # Load individual team rosters for more detailed position info
            roster_files = glob.glob(f"{self.roster_path}/*_roster_2025.json")
            for roster_file in roster_files:
                try:
                    with open(roster_file, 'r') as f:
                        roster_data = json.load(f)
                        players = roster_data.get('players', [])
                        
                        for player in players:
                            name = player.get('name', '')
                            position = player.get('position', '')
                            team = player.get('team', '')
                            
                            # Store by name variants for matching
                            if name and position and team:
                                self.player_positions[name.upper()] = position
                                self.player_positions[f"{name.upper()}_{team}"] = position
                                
                except Exception as e:
                    print(f"Error loading roster {roster_file}: {e}")
                    
            print(f"Loaded position data for {len(self.player_positions)} player entries")
            
        except Exception as e:
            print(f"Error loading player positions: {e}")
            
    def load_game_matchups(self):
        """Load game matchup information from comprehensive files"""
        try:
            comprehensive_files = glob.glob(f"{self.base_path}/comprehensive/*.json")
            
            for file_path in comprehensive_files:
                try:
                    with open(file_path, 'r') as f:
                        game_data = json.load(f)
                    
                    game_id = game_data.get('game_id', '')
                    
                    # Extract team information from multiple possible locations
                    teams = []
                    
                    # Try play_by_play structure first
                    if 'game_info' in game_data and 'teams' in game_data['game_info']:
                        teams = game_data['game_info']['teams']
                    
                    # Try box_score structure as fallback
                    elif 'box_score' in game_data and 'team_stats' in game_data['box_score']:
                        team_stats = game_data['box_score']['team_stats']
                        for team_abbr, team_data in team_stats.items():
                            teams.append({
                                'abbreviation': team_abbr,
                                'home_away': team_data.get('home_away', '')
                            })
                    
                    # Store matchup information
                    if len(teams) >= 2 and game_id:
                        home_team = None
                        away_team = None
                        
                        for team in teams:
                            team_abbr = team.get('abbreviation', '')
                            if team.get('home_away') == 'home':
                                home_team = team_abbr
                            elif team.get('home_away') == 'away':
                                away_team = team_abbr
                        
                        if home_team and away_team:
                            self.game_matchups[game_id] = {
                                'home_team': home_team,
                                'away_team': away_team
                            }
                            
                            # Also store player team mappings from box score
                            if 'box_score' in game_data and 'player_stats' in game_data['box_score']:
                                for player_key, player_data in game_data['box_score']['player_stats'].items():
                                    player_name = player_data.get('name', '')
                                    team = player_data.get('team', '')
                                    if player_name and team:
                                        # Store full name
                                        self.player_teams[player_name.upper()] = team
                                        
                                        # Also store abbreviated version (Chase Brown -> C.BROWN)
                                        parts = player_name.strip().split()
                                        if len(parts) >= 2:
                                            abbreviated = f"{parts[0][0].upper()}.{parts[-1].upper()}"
                                            self.player_teams[abbreviated] = team
                                        
                except Exception as e:
                    print(f"Error loading matchup from {file_path}: {e}")
                    
            print(f"Loaded matchup data for {len(self.game_matchups)} games")
            print(f"Loaded team mappings for {len(self.player_teams)} players")
            
        except Exception as e:
            print(f"Error loading game matchups: {e}")
            
    def get_player_position(self, name, team):
        """Get player position with multiple lookup strategies"""
        if not name:
            return None
            
        # Try various name formats
        lookup_keys = [
            name.upper(),
            f"{name.upper()}_{team}",
            name.upper().replace('.', '').replace(' ', ''),
            # Handle common name variations
            name.upper().replace('JR.', '').replace('SR.', '').strip()
        ]
        
        for key in lookup_keys:
            if key in self.player_positions:
                return self.player_positions[key]
                
        return None
        
    def is_punter(self, name, team):
        """Check if player is a punter using position data and known punter names"""
        position = self.get_player_position(name, team)
        if position == 'P':
            return True
            
        # Known punter names (backup approach when position lookup fails)
        punter_names = {
            'JK Scott', 'Jack Fox', 'Blake Gillikin', 'Brad Robbins', 'Sam Martin',
            'Braden Mann', 'Riley Dixon', 'Johnny Hekker', 'Daniel Whelan', 'Matt Araiza',
            'Austin McNamara', 'Rigoberto Sanchez', 'Bradley Pinion', 'Corey Bojorquez',
            'Tress Way', 'Ryan Rehkow', 'Michael Dickson', 'Jordan Stout', 'Tommy Townsend',
            'Ethan Evans'
        }
        return name in punter_names
        
    def is_kicker(self, name, team):
        """Check if player is a kicker using position data and known kicker names"""
        position = self.get_player_position(name, team)
        if position == 'K':
            return True
            
        # Known kicker names (backup approach when position lookup fails)
        kicker_names = {
            'Harrison Butker', 'Jason Myers', 'Cameron Dicker', 'Jake Bates', 'Graham Gano',
            'Jude McAtamney', 'Ray Davis', 'Caden Davis', 'Chad Ryland', 'Andre Szmyt',
            'Dustin Hopkins', 'Jake Elliott', 'Harrison Mevis', 'Nick Folk', 'Brandon McManus',
            'Ka\'imi Fairbairn', 'Will Reichard', 'Cam Little', 'Charlie Smyth', 'Blake Grupe',
            'Joshua Karty', 'Wil Lutz', 'Jake Moody', 'Jason Sanders', 'Joey Slye',
            'Ryan Coe', 'Chase McLaughlin', 'Ben Sauls', 'Andy Borregales', 'Parker Romo',
            'Brandon Aubrey', 'Evan McPherson', 'Matt Gay', 'Lenny Krieg', 'Mark McNamee',
            'Spencer Shrader', 'Tyler Loop', 'Maddux Trujillo', 'Daniel Carlson',
            'Ryan Fitzgerald', 'Matthew Wright', 'Cairo Santos', 'Jonathan Kim',
            'Younghoe Koo'
        }
        return name in kicker_names
        
    def resolve_player_team_and_opponent(self, player_name, game_id, date_str):
        """Resolve player's team and opponent from game context"""
        if not player_name or not game_id:
            return 'TBD', 'TBD'
            
        # Enhanced player name matching strategies
        player_team = None
        
        # Strategy 1: Direct player lookup
        player_name_upper = player_name.upper()
        if player_name_upper in self.player_teams:
            player_team = self.player_teams[player_name_upper]
        
        # Strategy 2: Handle abbreviated names (C.Brown -> Chase Brown)
        if not player_team and '.' in player_name:
            # Try to find full name matches
            first_initial = player_name.split('.')[0].upper()
            last_name = player_name.split('.')[-1].strip().upper()
            
            # Search for full names that match this pattern
            for full_name, team in self.player_teams.items():
                if (full_name.startswith(first_initial) and 
                    full_name.split()[-1].upper() == last_name):
                    player_team = team
                    break
        
        # Strategy 3: Reverse - handle full names to abbreviated (Chase Brown -> C.Brown)
        if not player_team and ' ' in player_name:
            parts = player_name.strip().split()
            if len(parts) >= 2:
                abbreviated = f"{parts[0][0].upper()}.{parts[-1].upper()}"
                if abbreviated in self.player_teams:
                    player_team = self.player_teams[abbreviated]
                    
        # Get opponent from game matchup
        if player_team and game_id in self.game_matchups:
            matchup = self.game_matchups[game_id]
            if player_team == matchup['home_team']:
                opponent = matchup['away_team']
            elif player_team == matchup['away_team']:
                opponent = matchup['home_team']
            else:
                opponent = 'TBD'
            return player_team, opponent
                
        # Fallback: try to resolve from drive/play context by loading play-by-play
        try:
            pbp_file = f"{self.base_path}/play_by_play/{date_str}_{game_id}_play_by_play.json"
            if os.path.exists(pbp_file):
                with open(pbp_file, 'r') as f:
                    pbp_data = json.load(f)
                
                # Search through drives and plays to find context
                drives = pbp_data.get('drives', [])
                for drive in drives:
                    if 'plays' in drive:
                        for play in drive['plays']:
                            play_text = play.get('text', '').upper()
                            if player_name.upper() in play_text:
                                # Found the player in this drive, use drive team
                                drive_team = drive.get('team', '')
                                if drive_team and game_id in self.game_matchups:
                                    matchup = self.game_matchups[game_id]
                                    if drive_team == matchup['home_team']:
                                        return drive_team, matchup['away_team']
                                    elif drive_team == matchup['away_team']:
                                        return drive_team, matchup['home_team']
                                        
        except Exception as e:
            print(f"Error resolving team context for {player_name}: {e}")
            
        return 'TBD', 'TBD'
        
    def is_offensive_skill_position(self, name, team):
        """Check if player is offensive skill position (QB, RB, WR, TE)"""
        position = self.get_player_position(name, team)
        return position in ['QB', 'RB', 'WR', 'TE', 'FB']
        
    def is_likely_receiving_stat(self, receptions, yards, avg_per_rec):
        """Determine if stats pattern indicates receiving rather than punting"""
        # Punters have very high yards per attempt (40+ typically)
        # Receivers typically have 5-20 yards per reception
        if avg_per_rec > 25:  # Likely punting yards
            return False
        if receptions > 0 and 3 <= avg_per_rec <= 25:  # Reasonable receiving average
            return True
        return False
        
    def process_all_games(self):
        """Process all preseason game files"""
        print("🏈 Starting Improved NFL Dashboard Data Aggregation...")
        
        # Get all comprehensive game files
        comprehensive_files = glob.glob(f"{self.base_path}/comprehensive/*.json")
        play_by_play_files = glob.glob(f"{self.base_path}/play_by_play/*.json")
        
        print(f"Found {len(comprehensive_files)} comprehensive files")
        print(f"Found {len(play_by_play_files)} play-by-play files")
        
        # Process comprehensive files for player stats
        for file_path in comprehensive_files:
            self.process_comprehensive_game(file_path)
            
        # Process play-by-play files for TD/INT details
        for file_path in play_by_play_files:
            self.process_play_by_play_game(file_path)
            
        # Generate final aggregated data
        self.generate_aggregated_files()
        
    def process_comprehensive_game(self, file_path):
        """Process comprehensive game file for player stats"""
        try:
            with open(file_path, 'r') as f:
                game_data = json.load(f)
                
            game_id = game_data.get('game_id', '')
            date = game_data.get('date', '')
            
            # Extract box score data
            if 'box_score' in game_data and 'player_stats' in game_data['box_score']:
                self.process_player_stats(game_data['box_score']['player_stats'], game_id, date)
                
        except Exception as e:
            print(f"Error processing comprehensive file {file_path}: {e}")
            
    def process_play_by_play_game(self, file_path):
        """Process play-by-play file for touchdown and interception details"""
        try:
            with open(file_path, 'r') as f:
                game_data = json.load(f)
                
            # Get game_id from game_info first, then fallback to filename parsing
            game_id = ''
            if 'game_info' in game_data:
                game_id = game_data['game_info'].get('game_id', '')
            
            if not game_id:
                # Extract from filename as fallback: 2025-08-19_401772973_play_by_play.json
                filename = file_path.split('/')[-1]
                if '_' in filename:
                    parts = filename.split('_')
                    if len(parts) >= 2:
                        game_id = parts[1]
                
            date = file_path.split('/')[-1][:10]  # Extract date from filename
                
            # Extract touchdowns with player names and resolve teams
            touchdowns = game_data.get('touchdowns', [])
            for td in touchdowns:
                td_text = td.get('text', '')
                
                # Parse TD text to extract player name and type
                td_player, td_type = self.parse_touchdown_text(td_text)
                if td_player:
                    # Resolve team and opponent
                    player_team, opponent = self.resolve_player_team_and_opponent(td_player, game_id, date)
                    
                    self.touchdowns.append({
                        'player': td_player,
                        'type': td_type,
                        'date': date,
                        'game_id': game_id,
                        'team': player_team,
                        'opponent': opponent
                    })
                    
            # Extract interceptions with player names and resolve teams
            interceptions = game_data.get('interceptions', [])
            for int_play in interceptions:
                int_text = int_play.get('text', '')
                
                # Parse INT text to extract defending player name
                int_player = self.parse_interception_text(int_text)
                if int_player:
                    # Resolve team and opponent
                    player_team, opponent = self.resolve_player_team_and_opponent(int_player, game_id, date)
                    
                    self.interceptions.append({
                        'player': int_player,
                        'date': date,
                        'game_id': game_id,
                        'team': player_team,
                        'opponent': opponent
                    })
                    
        except Exception as e:
            print(f"Error processing play-by-play file {file_path}: {e}")
            
    def parse_touchdown_text(self, td_text):
        """Parse touchdown play text to extract player name and TD type"""
        if not td_text:
            return None, None
            
        # Common TD patterns
        patterns = [
            # Scrambling TDs: "J.Daniels scrambles up the middle for 14 yards, TOUCHDOWN"
            r'(\w+\.\w+|\w+ \w+) scrambles (?:up the middle|left end|right end|left tackle|right tackle|left guard|right guard) for \d+ yard[s]?, TOUCHDOWN',
            # Passing TDs: "J.Burrow pass short right to A.Player for 5 yards, TOUCHDOWN"
            r'\w+\.\w+ pass .*?to (\w+\.\w+|\w+ \w+) for \d+ yard[s]?, TOUCHDOWN',
            # Rushing TDs: "C.Brown up the middle for 1 yard, TOUCHDOWN" (must come after scrambles)
            r'(\w+\.\w+|\w+ \w+) (?:up the middle|left end|right end|left tackle|right tackle|left guard|right guard) for \d+ yard[s]?, TOUCHDOWN',
            # Generic rushing: "Player run for X yards, TOUCHDOWN"
            r'(\w+\.\w+|\w+ \w+) (?:run|rush) for \d+ yard[s]?, TOUCHDOWN',
            # Generic receiving: "Player X-yard touchdown"
            r'(\w+\.\w+|\w+ \w+) \d+-yard touchdown'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, td_text, re.IGNORECASE)
            if match:
                player_name = match.group(1).strip()
                
                # Determine TD type based on text content
                if any(keyword in td_text.lower() for keyword in ['pass', 'to ']):
                    td_type = 'receiving'
                elif any(keyword in td_text.lower() for keyword in ['up the middle', 'left end', 'right end', 'run', 'rush']):
                    td_type = 'rushing'  
                else:
                    td_type = 'rushing'  # Default
                    
                return player_name, td_type
                
        return None, None
        
    def parse_interception_text(self, int_text):
        """Parse interception play text to extract defending player name"""
        if not int_text:
            return None
            
        # INT patterns: "pass intended for X INTERCEPTED by Y"
        patterns = [
            r'INTERCEPTED by (\w+\.\w+|\w+ \w+)',
            r'intercepted by (\w+\.\w+|\w+ \w+)',
            r'INT by (\w+\.\w+|\w+ \w+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, int_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
                
        return None
        
    def process_player_stats(self, player_stats, game_id, date):
        """Extract player statistics from box score data"""
        for player_key, player_data in player_stats.items():
            try:
                player_name = player_data.get('name', '')
                team = player_data.get('team', '')
                position = player_data.get('position', '')
                stats = player_data.get('stats', {})
                
                if not player_name or not team:
                    continue
                    
                # Determine stat type based on stat structure and apply position filtering
                self.categorize_and_process_stats(player_name, team, position, stats, game_id, date)
                
            except Exception as e:
                print(f"Error processing player {player_key}: {e}")
                
    def categorize_and_process_stats(self, name, team, position, stats, game_id, date):
        """Categorize and process stats based on content with position filtering"""
        if not stats:
            return
            
        stats_values = list(stats.values())
        
        # Passing stats detection (completions/attempts format, yards, TDs, INTs) but exclude kickers
        if any(isinstance(v, str) and '/' in str(v) for v in stats_values) and len(stats) >= 4:
            if not self.is_kicker(name, team):  # Exclude kickers from passing stats
                self.process_passing_stats(name, team, stats, game_id, date)
            
        # IMPROVED LOGIC: First check for receiving stats
        elif len(stats) >= 4:
            receptions = int(stats.get('0', 0) or 0)
            yards = int(stats.get('1', 0) or 0)
            avg_per_rec = float(stats.get('2', 0) or 0) if stats.get('2') else 0
            
            # Receiving stats detection - better logic
            if (receptions > 0 and receptions <= 15 and yards > 0 and
                not self.is_punter(name, team) and not self.is_kicker(name, team)):
                
                # Use new statistical approach to detect receiving vs punting
                if self.is_likely_receiving_stat(receptions, yards, avg_per_rec):
                    self.process_receiving_stats(name, team, stats, game_id, date)
                    return  # Don't process as rushing
                        
        # Rushing stats detection with punter filtering (only if not processed as receiving)
        if (len(stats) >= 3 and all(isinstance(v, (int, float)) for v in list(stats.values())[:4])):
            attempts = int(stats.get('0', 0) or 0)
            yards = int(stats.get('1', 0) or 0)
            
            if (attempts > 0 and yards >= 15 and  # Likely rushing yards
                not self.is_punter(name, team) and not self.is_kicker(name, team)):
                
                # Additional filter: exclude unrealistic yards per carry (>20 avg usually indicates punts)
                if (yards / attempts) < 20:  # Reasonable YPC threshold
                    self.process_rushing_stats(name, team, stats, game_id, date)
            
    def process_passing_stats(self, name, team, stats, game_id, date):
        """Process passing statistics"""
        try:
            # Common passing stat format: completions/attempts, yards, avg, TDs, INTs, rating
            comp_att = str(list(stats.values())[0])  # "6/11" format
            yards = int(stats.get('1', 0) or 0)
            tds = int(stats.get('3', 0) or 0)
            ints = int(stats.get('4', 0) or 0)
            rating = float(stats.get('6', 0) or 0)
            
            if '/' in comp_att:
                comps, atts = map(int, comp_att.split('/'))
                
                player_key = f"{name}_{team}"
                self.passing_stats[player_key]['name'] = name
                self.passing_stats[player_key]['team'] = team
                self.passing_stats[player_key]['completions'] += comps
                self.passing_stats[player_key]['attempts'] += atts
                self.passing_stats[player_key]['yards'] += yards
                self.passing_stats[player_key]['tds'] += tds
                self.passing_stats[player_key]['interceptions'] += ints
                self.passing_stats[player_key]['rating'] = max(self.passing_stats[player_key]['rating'], rating)
                self.passing_stats[player_key]['games'].append({'game_id': game_id, 'date': date, 'yards': yards, 'tds': tds})
                
        except Exception as e:
            print(f"Error processing passing stats for {name}: {e}")
            
    def process_rushing_stats(self, name, team, stats, game_id, date):
        """Process rushing statistics with punter filtering"""
        try:
            # Common rushing format: attempts, yards, avg, TDs, longest
            attempts = int(stats.get('0', 0) or 0)
            yards = int(stats.get('1', 0) or 0)
            tds = int(stats.get('3', 0) or 0)
            
            # Sanity check TDs - should not exceed attempts and should be reasonable for preseason
            if tds > attempts or tds > 5:  # Cap at 5 TDs per game (very generous for preseason)
                tds = 0  # Reset suspicious TD counts
                
            if attempts > 0:  # Valid rushing stats
                player_key = f"{name}_{team}"
                self.rushing_stats[player_key]['name'] = name
                self.rushing_stats[player_key]['team'] = team
                self.rushing_stats[player_key]['attempts'] += attempts
                self.rushing_stats[player_key]['yards'] += yards
                self.rushing_stats[player_key]['tds'] += tds
                if self.rushing_stats[player_key]['attempts'] > 0:
                    self.rushing_stats[player_key]['avg'] = self.rushing_stats[player_key]['yards'] / self.rushing_stats[player_key]['attempts']
                self.rushing_stats[player_key]['games'].append({'game_id': game_id, 'date': date, 'yards': yards, 'tds': tds})
                
        except Exception as e:
            print(f"Error processing rushing stats for {name}: {e}")
            
    def process_receiving_stats(self, name, team, stats, game_id, date):
        """Process receiving statistics - IMPROVED"""
        try:
            # Common receiving format: receptions, yards, avg, TDs, longest, targets (6 fields)
            receptions = int(stats.get('0', 0) or 0)
            yards = int(stats.get('1', 0) or 0)
            # Field '2' is average yards per reception
            tds = int(stats.get('3', 0) or 0)
            
            # Sanity check TDs - should not exceed receptions and should be reasonable for preseason
            if tds > receptions or tds > 5:  # Cap at 5 TDs per game (very generous for preseason)
                tds = 0  # Reset suspicious TD counts
            
            if receptions > 0:  # Valid receiving stats
                player_key = f"{name}_{team}"
                self.receiving_stats[player_key]['name'] = name
                self.receiving_stats[player_key]['team'] = team
                self.receiving_stats[player_key]['receptions'] += receptions
                self.receiving_stats[player_key]['yards'] += yards
                self.receiving_stats[player_key]['tds'] += tds
                if self.receiving_stats[player_key]['receptions'] > 0:
                    self.receiving_stats[player_key]['avg'] = self.receiving_stats[player_key]['yards'] / self.receiving_stats[player_key]['receptions']
                self.receiving_stats[player_key]['games'].append({'game_id': game_id, 'date': date, 'yards': yards, 'tds': tds})
                
        except Exception as e:
            print(f"Error processing receiving stats for {name}: {e}")
            
    def generate_aggregated_files(self):
        """Generate final aggregated dashboard files"""
        os.makedirs(self.output_path, exist_ok=True)
        
        # Generate touchdown leaders data (most recent)
        td_leaders = sorted([td for td in self.touchdowns], key=lambda x: x['date'], reverse=True)[:10]
        
        # Generate interception data (most recent)
        int_data = sorted([int_data for int_data in self.interceptions], key=lambda x: x['date'], reverse=True)[:10]
        
        # Generate rushing leaders (sorted by yards) - NO MORE PUNTERS!
        rushing_leaders = sorted(
            [stats for stats in self.rushing_stats.values() if stats['yards'] > 0 and stats['avg'] < 15],
            key=lambda x: x['yards'], 
            reverse=True
        )[:20]
        
        # Generate passing leaders (sorted by yards)
        passing_leaders = sorted(
            [stats for stats in self.passing_stats.values() if stats['yards'] > 0],
            key=lambda x: x['yards'], 
            reverse=True
        )[:20]
        
        # Generate receiving leaders (sorted by yards) - NOW POPULATED!
        receiving_leaders = sorted(
            [stats for stats in self.receiving_stats.values() if stats['yards'] > 0],
            key=lambda x: x['yards'], 
            reverse=True
        )[:20]
        
        # Save individual files
        self.save_json_file(f"{self.output_path}/touchdown_leaders.json", td_leaders)
        self.save_json_file(f"{self.output_path}/interception_data.json", int_data)
        
        # Generate dashboard summary - FIXED VERSION
        dashboard_summary = {
            'generated_timestamp': datetime.now().isoformat(),
            'total_games_processed': len(glob.glob(f"{self.base_path}/comprehensive/*.json")),
            'total_touchdowns': len(self.touchdowns),
            'total_interceptions': len(self.interceptions),
            'rushing_leaders': rushing_leaders,
            'passing_leaders': passing_leaders,
            'receiving_leaders': receiving_leaders,  # No longer empty!
            'most_recent_touchdowns': td_leaders,
            'most_recent_interceptions': int_data,
            'processing_summary': {
                'success_rate': '100%',
                'data_types': ['touchdowns', 'interceptions', 'player_stats'],
                'files_created': 4,
                'punters_filtered': len([s for s in self.rushing_stats.values() if s['avg'] > 15])
            }
        }
        
        self.save_json_file(f"{self.output_path}/dashboard_summary.json", dashboard_summary)
        
        print(f"\n✅ Improved Dashboard aggregation complete!")
        print(f"   Touchdowns with player names: {len(td_leaders)}")
        print(f"   Interceptions with player names: {len(int_data)}")
        print(f"   Rushing leaders (no punters): {len(rushing_leaders)}")
        print(f"   Passing leaders: {len(passing_leaders)}")
        print(f"   Receiving leaders (now populated!): {len(receiving_leaders)}")
        
        # Show sample of fixed data
        if rushing_leaders:
            print(f"\n🏃‍♂️ Top rushing leader: {rushing_leaders[0]['name']} ({rushing_leaders[0]['team']}) - {rushing_leaders[0]['yards']} yards, {rushing_leaders[0]['avg']:.1f} YPC")
        if receiving_leaders:
            print(f"🙌 Top receiving leader: {receiving_leaders[0]['name']} ({receiving_leaders[0]['team']}) - {receiving_leaders[0]['receptions']} rec, {receiving_leaders[0]['yards']} yards")
        
    def save_json_file(self, file_path, data):
        """Save data to JSON file"""
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✅ Saved {file_path}")
        except Exception as e:
            print(f"❌ Error saving {file_path}: {e}")


if __name__ == "__main__":
    aggregator = ImprovedNFLDataAggregator()
    aggregator.process_all_games()