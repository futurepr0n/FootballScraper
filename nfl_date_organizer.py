#!/usr/bin/env python3
"""
NFL Date-Based Schedule Organizer
Reorganizes ESPN NFL games by actual date instead of artificial week numbers
Similar to MLB system: august_15_2025.json, september_7_2025.json, etc.
"""

import json
import os
from datetime import datetime
from typing import Dict, List
from collections import defaultdict

class NFLDateOrganizer:
    def __init__(self):
        self.games_by_date = defaultdict(list)
        
        # ESPN season type mapping
        self.season_type_names = {
            1: 'preseason',
            2: 'regular_season', 
            3: 'playoffs'
        }
        
    def load_existing_games(self) -> List[Dict]:
        """Load all games from existing JSON summary files"""
        all_games = []
        
        # Get all summary JSON files
        json_files = [f for f in os.listdir('.') if f.endswith('_summary.json')]
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    if 'games' in data:
                        all_games.extend(data['games'])
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
        
        return all_games
    
    def organize_games_by_date(self, games: List[Dict]):
        """Organize games by actual date"""
        for game in games:
            try:
                # Parse ESPN date format: "2025-09-07T17:00Z"
                game_date = datetime.fromisoformat(game['date'].replace('Z', '+00:00'))
                date_key = game_date.strftime('%Y-%m-%d')
                
                # Add season type name for clarity
                game['season_type_name'] = self.season_type_names.get(game['season_type'], 'unknown')
                
                self.games_by_date[date_key].append(game)
                
            except Exception as e:
                print(f"Error processing game {game.get('game_id', 'unknown')}: {e}")
    
    def create_date_based_files(self):
        """Create date-based schedule files"""
        for date_key, games in sorted(self.games_by_date.items()):
            # Create filename: september_7_2025.json
            date_obj = datetime.strptime(date_key, '%Y-%m-%d')
            month_name = date_obj.strftime('%B').lower()
            day = date_obj.day
            year = date_obj.year
            
            filename = f"{month_name}_{day}_{year}.json"
            
            # Group games by season type for summary
            season_summary = defaultdict(int)
            for game in games:
                season_summary[game['season_type_name']] += 1
            
            # Create date file data structure
            date_data = {
                'date': date_key,
                'formatted_date': date_obj.strftime('%B %d, %Y'),
                'total_games': len(games),
                'season_breakdown': dict(season_summary),
                'games': games
            }
            
            # Write date-based JSON file
            with open(filename, 'w') as f:
                json.dump(date_data, f, indent=2)
            
            print(f"Created {filename} with {len(games)} games ({dict(season_summary)})")
    
    def cleanup_old_files(self):
        """Remove old week-based files"""
        old_patterns = ['preseason_week', 'regular_week', 'playoffs_week']
        
        files_to_remove = []
        for filename in os.listdir('.'):
            if any(pattern in filename for pattern in old_patterns):
                files_to_remove.append(filename)
        
        if files_to_remove:
            print(f"\nCleaning up {len(files_to_remove)} old week-based files:")
            for filename in files_to_remove:
                try:
                    os.remove(filename)
                    print(f"  Removed: {filename}")
                except Exception as e:
                    print(f"  Error removing {filename}: {e}")
    
    def generate_summary_report(self):
        """Generate summary of reorganized schedule"""
        total_games = sum(len(games) for games in self.games_by_date.values())
        total_dates = len(self.games_by_date)
        
        # Count by season type
        season_counts = defaultdict(int)
        for games in self.games_by_date.values():
            for game in games:
                season_counts[game['season_type_name']] += 1
        
        # Date range
        dates = sorted(self.games_by_date.keys())
        date_range = f"{dates[0]} to {dates[-1]}" if dates else "No games"
        
        print(f"\n🏈 NFL Schedule Reorganization Complete!")
        print(f"📅 Date range: {date_range}")
        print(f"📄 Total game dates: {total_dates}")
        print(f"🎯 Total games: {total_games}")
        print(f"📊 Season breakdown:")
        for season_type, count in sorted(season_counts.items()):
            print(f"   {season_type}: {count} games")

def main():
    print("🏈 NFL Date-Based Schedule Organizer")
    print("=" * 50)
    
    organizer = NFLDateOrganizer()
    
    # Load all games from existing JSON files
    print("📂 Loading games from existing files...")
    all_games = organizer.load_existing_games()
    print(f"✅ Loaded {len(all_games)} games total")
    
    # Remove duplicates based on game_id
    unique_games = {game['game_id']: game for game in all_games}.values()
    print(f"🔍 Found {len(unique_games)} unique games (removed {len(all_games) - len(unique_games)} duplicates)")
    
    # Organize by date
    print("📅 Organizing games by date...")
    organizer.organize_games_by_date(list(unique_games))
    
    # Create date-based files
    print("📝 Creating date-based schedule files...")
    organizer.create_date_based_files()
    
    # Generate summary
    organizer.generate_summary_report()
    
    # Ask before cleaning up old files
    cleanup = input("\n🗑️  Remove old week-based files? (y/n): ").lower().strip()
    if cleanup == 'y':
        organizer.cleanup_old_files()
    else:
        print("📦 Keeping old files for reference")

if __name__ == "__main__":
    main()