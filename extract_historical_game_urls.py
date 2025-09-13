#!/usr/bin/env python3
"""
Extract Historical Game URLs from CSV Files
Scans existing CSV files to extract game IDs with correct dates
Builds comprehensive list for validation-first scraping
"""

import os
import re
from pathlib import Path
from collections import defaultdict
import json

class HistoricalGameExtractor:
    def __init__(self):
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        self.games_by_season = defaultdict(set)
        self.game_urls = []
        
        print("🔍 Historical Game URL Extractor initialized")
        print(f"📂 Scanning directory: {self.csv_dir}")
    
    def extract_game_info_from_filename(self, filename):
        """Extract game ID and date from CSV filename"""
        # Pattern: nfl_TEAM_STAT_weekN_YYYYMMDD_GAMEID.csv
        pattern = r'nfl_\w+_\w+_week\d+_(\d{8})_(\d+)\.csv'
        match = re.match(pattern, filename)
        
        if match:
            date_str = match.group(1)  # YYYYMMDD
            game_id = match.group(2)   # Game ID
            
            # Extract year from date
            year = int(date_str[:4])
            
            # Only include historical data (not current/future years)
            if 2020 <= year <= 2024:  # Reasonable NFL data range
                return {
                    'game_id': game_id,
                    'date': date_str,
                    'year': year,
                    'url': f"https://www.espn.com/nfl/game/_/gameId/{game_id}"
                }
        
        return None
    
    def scan_csv_files(self):
        """Scan all CSV files and extract game information"""
        print(f"\n🔍 Scanning CSV files...")
        
        csv_files = list(self.csv_dir.glob("*.csv"))
        print(f"📊 Found {len(csv_files)} CSV files")
        
        unique_games = {}  # game_id -> game_info
        processed_count = 0
        
        for csv_file in csv_files:
            game_info = self.extract_game_info_from_filename(csv_file.name)
            
            if game_info:
                game_id = game_info['game_id']
                
                # Store unique games (avoid duplicates from multiple stat files)
                if game_id not in unique_games:
                    unique_games[game_id] = game_info
                    self.games_by_season[game_info['year']].add(game_id)
                
                processed_count += 1
                
                if processed_count % 1000 == 0:
                    print(f"   Processed {processed_count} files...")
        
        # Convert to list and sort
        self.game_urls = list(unique_games.values())
        self.game_urls.sort(key=lambda x: (x['year'], x['date'], x['game_id']))
        
        print(f"✅ Extracted {len(self.game_urls)} unique games")
        
        # Show breakdown by season
        print(f"\n📊 Games by season:")
        for year in sorted(self.games_by_season.keys()):
            count = len(self.games_by_season[year])
            print(f"   {year}: {count} games")
        
        return self.game_urls
    
    def save_game_urls(self, output_file="historical_game_urls.json"):
        """Save extracted game URLs to JSON file"""
        output_path = Path(output_file)
        
        # Prepare data for JSON
        games_data = {
            'metadata': {
                'total_games': len(self.game_urls),
                'seasons': {str(year): len(games) for year, games in self.games_by_season.items()},
                'date_range': {
                    'earliest': min(game['date'] for game in self.game_urls) if self.game_urls else None,
                    'latest': max(game['date'] for game in self.game_urls) if self.game_urls else None
                }
            },
            'games': self.game_urls
        }
        
        with open(output_path, 'w') as f:
            json.dump(games_data, f, indent=2)
        
        print(f"💾 Saved game URLs to: {output_path}")
        print(f"📈 Ready for validation-first scraping")
        
        return output_path
    
    def generate_priority_list(self):
        """Generate priority list focusing on popular matchups"""
        priority_matchups = [
            # NFC East rivalries
            ('PHI', 'NYG'), ('PHI', 'DAL'), ('PHI', 'WAS'),
            ('NYG', 'DAL'), ('NYG', 'WAS'), ('DAL', 'WAS'),
            # Other popular rivalries
            ('GB', 'CHI'), ('NE', 'NYJ'), ('PIT', 'BAL'),
            ('SF', 'SEA'), ('KC', 'DEN'), ('LAR', 'SF')
        ]
        
        print(f"\n🎯 Priority matchups for historical analysis:")
        for team1, team2 in priority_matchups:
            print(f"   {team1} vs {team2}")
        
        return priority_matchups
    
    def create_scraping_batches(self, batch_size=50):
        """Create batches for efficient scraping"""
        batches = []
        
        for i in range(0, len(self.game_urls), batch_size):
            batch = self.game_urls[i:i + batch_size]
            batches.append(batch)
        
        print(f"\n📦 Created {len(batches)} batches of {batch_size} games each")
        return batches

def main():
    """Extract historical game URLs from CSV files"""
    print("🏈 HISTORICAL GAME URL EXTRACTOR")
    print("=" * 60)
    
    extractor = HistoricalGameExtractor()
    
    # Extract game URLs
    game_urls = extractor.scan_csv_files()
    
    if not game_urls:
        print("❌ No historical games found in CSV files")
        return 1
    
    # Save to file
    output_file = extractor.save_game_urls()
    
    # Generate priority information
    extractor.generate_priority_list()
    
    # Create batches
    batches = extractor.create_scraping_batches()
    
    print(f"\n✅ EXTRACTION COMPLETE")
    print(f"📊 {len(game_urls)} historical games ready for scraping")
    print(f"📁 URLs saved to: {output_file}")
    print(f"🎯 Ready to run production scraper with validation-first approach")
    
    return 0

if __name__ == "__main__":
    exit(main())