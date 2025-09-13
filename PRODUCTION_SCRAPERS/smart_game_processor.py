#!/usr/bin/env python3
"""
Smart NFL Game Processor - Only processes completed games
Parses game files with timestamps and only scrapes games that have finished
"""

import sys
import re
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

def parse_game_file(file_path):
    """Parse the game file and extract games with their timestamps"""
    games = []
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for comment lines with timestamps
        if line.startswith('#') and ' - ' in line and 'Z' in line:
            # Extract timestamp from comment
            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z)', line)
            if timestamp_match:
                timestamp_str = timestamp_match.group(1)
                teams = line.split(' - ')[0].replace('#', '').strip()
                
                # Next line should be the URL
                if i + 1 < len(lines):
                    url_line = lines[i + 1].strip()
                    if url_line.startswith('https://www.espn.com/nfl/game/'):
                        game_id_match = re.search(r'gameId/(\d+)', url_line)
                        if game_id_match:
                            game_id = game_id_match.group(1)
                            games.append({
                                'teams': teams,
                                'timestamp': timestamp_str,
                                'url': url_line,
                                'game_id': game_id
                            })
        i += 1
    
    return games

def is_game_completed(timestamp_str, buffer_hours=3):
    """Check if a game has completed based on timestamp + buffer"""
    try:
        # Parse the timestamp (format: 2025-09-12T00:15Z)
        game_time = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%MZ')
        game_time = game_time.replace(tzinfo=timezone.utc)
        
        # Add buffer time (games typically last ~3 hours)
        completion_time = game_time + timedelta(hours=buffer_hours)
        
        # Get current time in UTC
        now = datetime.now(timezone.utc)
        
        return now >= completion_time
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_str}: {e}")
        return False

def create_temp_game_file(completed_games, base_filename):
    """Create a temporary file with only completed games"""
    temp_file = Path(base_filename).stem + "_completed.txt"
    
    with open(temp_file, 'w') as f:
        f.write("# NFL Completed Games - Auto-generated\n")
        f.write("# Format: One ESPN game URL per line\n")
        f.write("# Use: python process_nfl_game_file.py {}\n".format(temp_file))
        f.write("\n\n")
        
        for game in completed_games:
            f.write(f"# {game['teams']} - {game['timestamp']}\n")
            f.write(f"{game['url']}\n")
    
    return temp_file

def run_scraper_and_loader(temp_file, week_num):
    """Run the existing scraper and CSV loader"""
    try:
        print(f"🚀 Running scraper on {temp_file}...")
        
        # Run the game file processor
        result = subprocess.run([
            'python3', 'process_nfl_game_file.py', temp_file
        ], capture_output=True, text=True, timeout=600)
        
        if result.returncode != 0:
            print(f"❌ Scraper failed: {result.stderr}")
            return False
        
        print("✅ Scraping completed successfully")
        print(f"📄 Scraper output:\n{result.stdout}")
        
        # Run the CSV loader
        print(f"🗄️  Loading CSV data into database...")
        
        result = subprocess.run([
            'python3', 'simple_csv_loader.py', 
            '--csv-dir', '../FootballData/CSV_BACKUPS',
            '--season', '2025',
            '--week', str(week_num),
            '--season-type', 'regular'
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            print(f"❌ CSV loader failed: {result.stderr}")
            return False
        
        print("✅ Database loading completed successfully")
        return True
        
    except subprocess.TimeoutExpired:
        print("❌ Process timed out")
        return False
    except Exception as e:
        print(f"❌ Error running processes: {e}")
        return False

def main():
    if len(sys.argv) != 2:
        print("Usage: python smart_game_processor.py <week_file>")
        print("Example: python smart_game_processor.py regular_week2_2025.txt")
        sys.exit(1)
    
    game_file = sys.argv[1]
    
    # Extract week number from filename
    week_match = re.search(r'week(\d+)', game_file)
    if not week_match:
        print(f"❌ Cannot extract week number from {game_file}")
        sys.exit(1)
    
    week_num = int(week_match.group(1))
    
    print(f"🏈 Smart NFL Game Processor - Week {week_num}")
    print(f"📁 Processing file: {game_file}")
    print(f"⏰ Current time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)
    
    # Parse the game file
    try:
        games = parse_game_file(game_file)
        print(f"📋 Found {len(games)} games in file")
    except FileNotFoundError:
        print(f"❌ File not found: {game_file}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error parsing file: {e}")
        sys.exit(1)
    
    # Check which games are completed
    completed_games = []
    pending_games = []
    
    for game in games:
        if is_game_completed(game['timestamp']):
            completed_games.append(game)
        else:
            pending_games.append(game)
    
    # Report status
    print(f"✅ Completed games: {len(completed_games)}")
    for game in completed_games:
        print(f"   • {game['teams']} ({game['timestamp']})")
    
    print(f"⏳ Pending games: {len(pending_games)}")
    for game in pending_games:
        game_time = datetime.strptime(game['timestamp'], '%Y-%m-%dT%H:%MZ').replace(tzinfo=timezone.utc)
        completion_time = game_time + timedelta(hours=3)
        print(f"   • {game['teams']} (completes ~{completion_time.strftime('%Y-%m-%d %H:%M UTC')})")
    
    if not completed_games:
        print("🚫 No completed games to process. Exiting.")
        return
    
    # Create temporary file with completed games only
    temp_file = create_temp_game_file(completed_games, game_file)
    print(f"📝 Created temporary file: {temp_file}")
    
    try:
        # Run scraper and loader
        success = run_scraper_and_loader(temp_file, week_num)
        
        if success:
            print("🎉 Processing completed successfully!")
            print(f"🗑️  Cleaning up temporary file: {temp_file}")
            Path(temp_file).unlink()
        else:
            print(f"❌ Processing failed. Temporary file kept: {temp_file}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Process interrupted by user")
        print(f"🗑️  Cleaning up temporary file: {temp_file}")
        Path(temp_file).unlink()
        sys.exit(1)

if __name__ == "__main__":
    main()