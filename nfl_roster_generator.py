#!/usr/bin/env python3
"""
NFL Roster Generator
Creates comprehensive NFL team rosters with position information
Similar to BaseballTracker's roster system but adapted for NFL
"""

import json
import requests
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

class NFLRosterGenerator:
    def __init__(self):
        self.base_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Claude-Code")
        self.data_dir = self.base_dir / "FootballData" / "data"
        self.rosters_dir = self.data_dir / "rosters"
        
        # Ensure directories exist
        self.rosters_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # NFL teams mapping
        self.nfl_teams = {
            'ARI': {'name': 'Arizona Cardinals', 'id': 'ari'},
            'ATL': {'name': 'Atlanta Falcons', 'id': 'atl'},
            'BAL': {'name': 'Baltimore Ravens', 'id': 'bal'},
            'BUF': {'name': 'Buffalo Bills', 'id': 'buf'},
            'CAR': {'name': 'Carolina Panthers', 'id': 'car'},
            'CHI': {'name': 'Chicago Bears', 'id': 'chi'},
            'CIN': {'name': 'Cincinnati Bengals', 'id': 'cin'},
            'CLE': {'name': 'Cleveland Browns', 'id': 'cle'},
            'DAL': {'name': 'Dallas Cowboys', 'id': 'dal'},
            'DEN': {'name': 'Denver Broncos', 'id': 'den'},
            'DET': {'name': 'Detroit Lions', 'id': 'det'},
            'GB': {'name': 'Green Bay Packers', 'id': 'gb'},
            'HOU': {'name': 'Houston Texans', 'id': 'hou'},
            'IND': {'name': 'Indianapolis Colts', 'id': 'ind'},
            'JAX': {'name': 'Jacksonville Jaguars', 'id': 'jax'},
            'KC': {'name': 'Kansas City Chiefs', 'id': 'kc'},
            'LV': {'name': 'Las Vegas Raiders', 'id': 'lv'},
            'LAC': {'name': 'Los Angeles Chargers', 'id': 'lac'},
            'LAR': {'name': 'Los Angeles Rams', 'id': 'lar'},
            'MIA': {'name': 'Miami Dolphins', 'id': 'mia'},
            'MIN': {'name': 'Minnesota Vikings', 'id': 'min'},
            'NE': {'name': 'New England Patriots', 'id': 'ne'},
            'NO': {'name': 'New Orleans Saints', 'id': 'no'},
            'NYG': {'name': 'New York Giants', 'id': 'nyg'},
            'NYJ': {'name': 'New York Jets', 'id': 'nyj'},
            'PHI': {'name': 'Philadelphia Eagles', 'id': 'phi'},
            'PIT': {'name': 'Pittsburgh Steelers', 'id': 'pit'},
            'SF': {'name': 'San Francisco 49ers', 'id': 'sf'},
            'SEA': {'name': 'Seattle Seahawks', 'id': 'sea'},
            'TB': {'name': 'Tampa Bay Buccaneers', 'id': 'tb'},
            'TEN': {'name': 'Tennessee Titans', 'id': 'ten'},
            'WSH': {'name': 'Washington Commanders', 'id': 'wsh'}
        }
        
        # Position mappings
        self.position_groups = {
            'QB': {'group': 'offense', 'type': 'skill'},
            'RB': {'group': 'offense', 'type': 'skill'},
            'FB': {'group': 'offense', 'type': 'skill'},
            'WR': {'group': 'offense', 'type': 'skill'},
            'TE': {'group': 'offense', 'type': 'skill'},
            'LT': {'group': 'offense', 'type': 'line'},
            'LG': {'group': 'offense', 'type': 'line'},
            'C': {'group': 'offense', 'type': 'line'},
            'RG': {'group': 'offense', 'type': 'line'},
            'RT': {'group': 'offense', 'type': 'line'},
            'OL': {'group': 'offense', 'type': 'line'},
            'DE': {'group': 'defense', 'type': 'line'},
            'DT': {'group': 'defense', 'type': 'line'},
            'NT': {'group': 'defense', 'type': 'line'},
            'LB': {'group': 'defense', 'type': 'linebacker'},
            'MLB': {'group': 'defense', 'type': 'linebacker'},
            'OLB': {'group': 'defense', 'type': 'linebacker'},
            'CB': {'group': 'defense', 'type': 'secondary'},
            'S': {'group': 'defense', 'type': 'secondary'},
            'FS': {'group': 'defense', 'type': 'secondary'},
            'SS': {'group': 'defense', 'type': 'secondary'},
            'K': {'group': 'special_teams', 'type': 'kicker'},
            'P': {'group': 'special_teams', 'type': 'punter'},
            'LS': {'group': 'special_teams', 'type': 'long_snapper'}
        }
    
    def get_team_roster_from_espn(self, team_abbr: str, team_info: Dict) -> List[Dict]:
        """Fetch team roster from ESPN"""
        try:
            # ESPN team roster endpoint
            espn_team_id = self.get_espn_team_id(team_abbr)
            if not espn_team_id:
                print(f"⚠️  No ESPN team ID found for {team_abbr}")
                return []
            
            url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{espn_team_id}/roster"
            
            print(f"Fetching roster for {team_abbr} ({team_info['name']})...")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            roster_data = response.json()
            return self.parse_espn_roster(roster_data, team_abbr, team_info)
            
        except Exception as e:
            print(f"Error fetching roster for {team_abbr}: {e}")
            return []
    
    def get_espn_team_id(self, team_abbr: str) -> Optional[str]:
        """Get ESPN team ID from team abbreviation"""
        # ESPN team ID mapping (these are ESPN's internal IDs)
        espn_team_ids = {
            'ARI': '22', 'ATL': '1', 'BAL': '33', 'BUF': '2', 'CAR': '29',
            'CHI': '3', 'CIN': '4', 'CLE': '5', 'DAL': '6', 'DEN': '7',
            'DET': '8', 'GB': '9', 'HOU': '34', 'IND': '11', 'JAX': '30',
            'KC': '12', 'LV': '13', 'LAC': '24', 'LAR': '14', 'MIA': '15',
            'MIN': '16', 'NE': '17', 'NO': '18', 'NYG': '19', 'NYJ': '20',
            'PHI': '21', 'PIT': '23', 'SF': '25', 'SEA': '26', 'TB': '27',
            'TEN': '10', 'WSH': '28'
        }
        return espn_team_ids.get(team_abbr)
    
    def parse_espn_roster(self, roster_data: Dict, team_abbr: str, team_info: Dict) -> List[Dict]:
        """Parse ESPN roster data into our format"""
        players = []
        
        if 'athletes' not in roster_data:
            print(f"⚠️  No athletes found in roster data for {team_abbr}")
            return players
        
        for athlete in roster_data['athletes']:
            try:
                player_info = athlete.get('athlete', {})
                
                # Extract basic info
                player_name = player_info.get('displayName', '')
                if not player_name:
                    continue
                
                player_id = str(player_info.get('id', ''))
                position = athlete.get('position', {}).get('abbreviation', 'UNK')
                jersey = athlete.get('jersey', '')
                
                # Extract physical stats
                height = player_info.get('height', 0)
                weight = player_info.get('weight', 0)
                age = player_info.get('age', 0)
                
                # Extract experience
                experience = player_info.get('experience', {}).get('years', 0)
                
                # Extract college
                college = ''
                college_info = player_info.get('college')
                if college_info:
                    college = college_info.get('name', '')
                
                # Determine position group
                pos_group_info = self.position_groups.get(position, {
                    'group': 'unknown', 'type': 'unknown'
                })
                
                player = {
                    'name': player_name,
                    'player_id': player_id,
                    'team': team_abbr,
                    'team_name': team_info['name'],
                    'position': position,
                    'position_group': pos_group_info['group'],
                    'position_type': pos_group_info['type'],
                    'jersey_number': jersey,
                    'height': height,
                    'weight': weight,
                    'age': age,
                    'experience': experience,
                    'college': college,
                    'active': True,
                    'last_updated': datetime.now().isoformat()
                }
                
                players.append(player)
                
            except Exception as e:
                print(f"Error parsing player data: {e}")
                continue
        
        print(f"  ✅ Parsed {len(players)} players")
        return players
    
    def generate_sample_roster(self, team_abbr: str, team_info: Dict) -> List[Dict]:
        """Generate sample roster data if API fails"""
        print(f"Generating sample roster for {team_abbr}...")
        
        sample_positions = [
            ('QB', 3), ('RB', 4), ('WR', 6), ('TE', 3),
            ('LT', 2), ('LG', 2), ('C', 2), ('RG', 2), ('RT', 2),
            ('DE', 4), ('DT', 3), ('LB', 6), ('CB', 5), ('S', 4),
            ('K', 1), ('P', 1), ('LS', 1)
        ]
        
        players = []
        player_id = 1
        
        for position, count in sample_positions:
            pos_group_info = self.position_groups.get(position, {
                'group': 'unknown', 'type': 'unknown'
            })
            
            for i in range(count):
                player = {
                    'name': f"{team_abbr} {position}{i+1}",
                    'player_id': f"{team_abbr}_{position}_{player_id:03d}",
                    'team': team_abbr,
                    'team_name': team_info['name'],
                    'position': position,
                    'position_group': pos_group_info['group'],
                    'position_type': pos_group_info['type'],
                    'jersey_number': str(player_id),
                    'height': 72,  # 6'0"
                    'weight': 200,
                    'age': 25,
                    'experience': 3,
                    'college': 'Sample University',
                    'active': True,
                    'last_updated': datetime.now().isoformat()
                }
                
                players.append(player)
                player_id += 1
        
        print(f"  ✅ Generated {len(players)} sample players")
        return players
    
    def save_team_roster(self, team_abbr: str, players: List[Dict]):
        """Save team roster to JSON file"""
        if not players:
            print(f"⚠️  No players to save for {team_abbr}")
            return
        
        roster_file = self.rosters_dir / f"{team_abbr.lower()}_roster_2025.json"
        
        roster_data = {
            'team': team_abbr,
            'team_name': self.nfl_teams[team_abbr]['name'],
            'season': 2025,
            'total_players': len(players),
            'last_updated': datetime.now().isoformat(),
            'position_counts': self.get_position_counts(players),
            'players': players
        }
        
        with open(roster_file, 'w') as f:
            json.dump(roster_data, f, indent=2)
        
        print(f"📄 Saved roster: {roster_file}")
    
    def get_position_counts(self, players: List[Dict]) -> Dict[str, int]:
        """Get count of players by position"""
        counts = {}
        for player in players:
            position = player['position']
            counts[position] = counts.get(position, 0) + 1
        return counts
    
    def generate_master_roster(self):
        """Generate master roster file with all teams"""
        print("🏈 Generating NFL Master Roster")
        print("=" * 50)
        
        all_players = []
        team_summary = {}
        
        for team_abbr, team_info in self.nfl_teams.items():
            try:
                # Try to get real roster from ESPN
                players = self.get_team_roster_from_espn(team_abbr, team_info)
                
                # Fall back to sample roster if needed
                if not players:
                    print(f"⚠️  Using sample roster for {team_abbr}")
                    players = self.generate_sample_roster(team_abbr, team_info)
                
                # Save individual team roster
                self.save_team_roster(team_abbr, players)
                
                # Add to master roster
                all_players.extend(players)
                team_summary[team_abbr] = {
                    'name': team_info['name'],
                    'player_count': len(players),
                    'positions': self.get_position_counts(players)
                }
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error processing {team_abbr}: {e}")
                continue
        
        # Save master roster
        master_file = self.rosters_dir / "nfl_master_roster_2025.json"
        master_data = {
            'season': 2025,
            'total_teams': len(self.nfl_teams),
            'total_players': len(all_players),
            'generated_date': datetime.now().isoformat(),
            'team_summary': team_summary,
            'all_players': all_players
        }
        
        with open(master_file, 'w') as f:
            json.dump(master_data, f, indent=2)
        
        print(f"📄 Saved master roster: {master_file}")
        
        # Generate position summary
        self.generate_position_summary(all_players)
        
        print(f"\n🎯 NFL Roster Generation Complete!")
        print(f"📊 Total Teams: {len(self.nfl_teams)}")
        print(f"👥 Total Players: {len(all_players)}")
    
    def generate_position_summary(self, all_players: List[Dict]):
        """Generate position-based summary"""
        position_summary = {}
        
        for player in all_players:
            position = player['position']
            group = player['position_group']
            
            if position not in position_summary:
                position_summary[position] = {
                    'position': position,
                    'group': group,
                    'total_players': 0,
                    'teams_with_position': set(),
                    'avg_experience': 0,
                    'experience_total': 0
                }
            
            pos_data = position_summary[position]
            pos_data['total_players'] += 1
            pos_data['teams_with_position'].add(player['team'])
            pos_data['experience_total'] += player.get('experience', 0)
        
        # Convert sets to counts and calculate averages
        for pos, data in position_summary.items():
            data['teams_with_position'] = len(data['teams_with_position'])
            if data['total_players'] > 0:
                data['avg_experience'] = round(data['experience_total'] / data['total_players'], 1)
            del data['experience_total']
        
        # Save position summary
        summary_file = self.rosters_dir / "position_summary_2025.json"
        summary_data = {
            'season': 2025,
            'generated_date': datetime.now().isoformat(),
            'total_positions': len(position_summary),
            'position_breakdown': position_summary
        }
        
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        print(f"📄 Saved position summary: {summary_file}")

def main():
    generator = NFLRosterGenerator()
    generator.generate_master_roster()

if __name__ == "__main__":
    main()