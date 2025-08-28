#!/usr/bin/env python3
"""
Fix Preseason Week Mapping - Consolidate ESPN's 4-week preseason structure into our 3-week structure

This script:
1. Maps ESPN Week 4 preseason games (Aug 21-Sept 3) to our Preseason Week 3
2. Ensures proper date formats and game data consistency
3. Creates updated game files for FootballData consumption

ESPN Structure -> Our Structure:
- ESPN Week 1 (Aug 7-13) -> Preseason Week 1
- ESPN Week 2 (Aug 14-20) -> Preseason Week 2  
- ESPN Weeks 3+4 (Aug 21-Sept 3) -> Preseason Week 3
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import glob

class PreseasonWeekMapper:
    def __init__(self):
        self.base_dir = Path('.')
        self.output_dir = Path('../FootballData/data')
        
        # Our 3-week preseason structure
        self.week_mappings = {
            'preseason_week_1': {
                'date_range': ('2025-08-07', '2025-08-13'),
                'espn_weeks': [1]
            },
            'preseason_week_2': {
                'date_range': ('2025-08-14', '2025-08-20'),
                'espn_weeks': [2]
            },
            'preseason_week_3': {
                'date_range': ('2025-08-21', '2025-09-03'),
                'espn_weeks': [3, 4]  # ESPN weeks 3 and 4 -> our week 3
            }
        }
    
    def load_daily_games(self):
        """Load all daily game files from August 2025"""
        daily_games = {}
        
        # Load all August game files
        august_files = glob.glob('august_*_2025.json')
        
        for file in august_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                    
                date = data.get('date')
                if date:
                    daily_games[date] = data.get('games', [])
                        
            except Exception as e:
                print(f"Error loading {file}: {e}")
        
        return daily_games
    
    def map_games_to_weeks(self, daily_games):
        """Map daily games to our 3-week preseason structure"""
        weekly_games = {
            'preseason_week_1': [],
            'preseason_week_2': [],
            'preseason_week_3': []
        }
        
        for date_str, games in daily_games.items():
            # Convert date string to datetime for comparison
            try:
                game_date = datetime.strptime(date_str, '%Y-%m-%d')
            except:
                print(f"Invalid date format: {date_str}")
                continue
            
            # Map to our week structure
            week_key = None
            for week, info in self.week_mappings.items():
                start_date = datetime.strptime(info['date_range'][0], '%Y-%m-%d')
                end_date = datetime.strptime(info['date_range'][1], '%Y-%m-%d')
                
                if start_date <= game_date <= end_date:
                    week_key = week
                    break
            
            if week_key:
                for game in games:
                    # Ensure game has proper format and dates
                    game_info = {
                        'game_id': game.get('game_id'),
                        'url': game.get('url'),
                        'date': date_str,  # Use consistent date format
                        'matchup': game.get('matchup'),
                        'away_team': game.get('away_team'),
                        'home_team': game.get('home_team'),
                        'season_type': 'preseason',
                        'our_week': int(week_key.split('_')[-1]),  # Extract week number
                        'espn_week': game.get('week', 0)
                    }
                    weekly_games[week_key].append(game_info)
        
        return weekly_games
    
    def create_week_files(self, weekly_games):
        """Create consolidated week files for FootballData"""
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        for week_key, games in weekly_games.items():
            if not games:
                print(f"No games found for {week_key}")
                continue
            
            week_num = int(week_key.split('_')[-1])
            
            # Create week summary
            week_data = {
                'week': week_num,
                'season_type': 'preseason',
                'date_range': self.week_mappings[week_key]['date_range'],
                'total_games': len(games),
                'games': games,
                'generated': datetime.now().isoformat()
            }
            
            # Write to FootballData directory
            output_file = self.output_dir / f'preseason_week_{week_num}_2025.json'
            
            try:
                with open(output_file, 'w') as f:
                    json.dump(week_data, f, indent=2)
                
                print(f"✅ Created {output_file} with {len(games)} games")
                
                # Print sample games for verification
                print(f"   Sample games in Week {week_num}:")
                for game in games[:3]:  # Show first 3 games
                    print(f"     {game['date']}: {game['matchup']}")
                if len(games) > 3:
                    print(f"     ... and {len(games) - 3} more games")
                print()
                
            except Exception as e:
                print(f"❌ Error creating {output_file}: {e}")
    
    def run(self):
        """Execute the preseason week mapping"""
        print("🏈 Fixing Preseason Week Mapping (ESPN 4-week -> Our 3-week)")
        print("=" * 60)
        
        # Load daily games
        print("📊 Loading daily game files...")
        daily_games = self.load_daily_games()
        
        total_games = sum(len(games) for games in daily_games.values())
        print(f"📅 Loaded {total_games} games from {len(daily_games)} days")
        
        # Map to weeks
        print("\n🗓️  Mapping games to 3-week structure...")
        weekly_games = self.map_games_to_weeks(daily_games)
        
        # Show mapping summary
        print("\n📋 Week mapping summary:")
        for week_key, games in weekly_games.items():
            week_num = int(week_key.split('_')[-1])
            date_range = self.week_mappings[week_key]['date_range']
            print(f"   Preseason Week {week_num} ({date_range[0]} to {date_range[1]}): {len(games)} games")
        
        # Create output files
        print(f"\n📂 Creating week files in {self.output_dir}...")
        self.create_week_files(weekly_games)
        
        print("✅ Preseason week mapping complete!")
        print(f"🎯 Files available in: {self.output_dir}")

def main():
    mapper = PreseasonWeekMapper()
    mapper.run()

if __name__ == "__main__":
    main()