#!/usr/bin/env python3
"""
Process NFL Game URLs from File
Processes individual NFL game URLs from a text file, similar to BaseballScraper approach.

Usage:
  python process_nfl_game_file.py preseason_week2_2025.txt
  python process_nfl_game_file.py --file week1_2024.txt --season 2024
"""

import argparse
import sys
from pathlib import Path
from enhanced_nfl_scraper import EnhancedNFLScraper
import logging
import re
from urllib.parse import urlparse
import json

logger = logging.getLogger(__name__)

def parse_game_urls_from_file(file_path: Path) -> list:
    """
    Parse ESPN game URLs from a text file
    
    Args:
        file_path: Path to text file containing game URLs
        
    Returns:
        List of game URL strings
    """
    game_urls = []
    
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            # Validate ESPN NFL game URL
            if 'espn.com/nfl/game/' in line:
                game_urls.append(line)
                logger.debug(f"Line {line_num}: Found game URL: {line}")
            else:
                logger.warning(f"Line {line_num}: Invalid URL format: {line}")
        
        logger.info(f"Loaded {len(game_urls)} game URLs from {file_path}")
        return game_urls
        
    except FileNotFoundError:
        logger.error(f"Game file not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading game file {file_path}: {e}")
        return []

def extract_game_info_from_url(game_url: str, default_season: int = 2025, default_week: int = 1) -> dict:
    """
    Extract game information from ESPN game URL
    
    Args:
        game_url: ESPN game URL
        default_season: Default season year
        default_week: Default week number
        
    Returns:
        Dictionary with game information
    """
    # Extract game ID from URL
    game_id_match = re.search(r'/gameId/(\d+)', game_url)
    game_id = game_id_match.group(1) if game_id_match else None
    
    if not game_id:
        logger.warning(f"Could not extract game ID from URL: {game_url}")
        return None
    
    # For now, use defaults - in a real implementation, you might parse season/week from filename or URL
    game_info = {
        'game_id': game_id,
        'game_url': game_url,
        'season': default_season,
        'season_type': 1,  # Assume preseason for manual files
        'week': default_week,
        'status': 'completed'
    }
    
    return game_info

def process_game_file(file_path: Path, season: int = None, week: int = None, season_type: int = 1) -> dict:
    """
    Process all games from a URL file
    
    Args:
        file_path: Path to game URL file
        season: Season year (extracted from filename if not provided)
        week: Week number (extracted from filename if not provided)
        season_type: Season type (1=Preseason, 2=Regular, 3=Playoffs)
        
    Returns:
        Processing summary dictionary
    """
    logger.info(f"Processing NFL games from file: {file_path}")
    
    # Extract season and week from filename if not provided
    filename = file_path.stem
    if season is None:
        season_match = re.search(r'(\d{4})', filename)
        season = int(season_match.group(1)) if season_match else 2025
    
    if week is None:
        week_match = re.search(r'week(\d+)', filename, re.I)
        week = int(week_match.group(1)) if week_match else 1
    
    # Detect season type from filename
    if 'preseason' in filename.lower():
        season_type = 1
    elif 'playoff' in filename.lower():
        season_type = 3
    else:
        season_type = 2  # Regular season
    
    logger.info(f"Detected: Season {season}, Week {week}, Type {season_type}")
    
    # Load game URLs
    game_urls = parse_game_urls_from_file(file_path)
    
    if not game_urls:
        return {
            'success': False,
            'message': 'No valid game URLs found in file',
            'file_path': str(file_path),
            'total_games': 0
        }
    
    # Initialize scraper
    scraper = EnhancedNFLScraper()
    
    # Track results
    processed_games = []
    failed_games = []
    created_files = []
    
    # Process each game URL
    for i, game_url in enumerate(game_urls, 1):
        logger.info(f"Processing game {i}/{len(game_urls)}: {game_url}")
        
        try:
            # Extract basic game info
            game_info = extract_game_info_from_url(game_url, season, week)
            if not game_info:
                failed_games.append({'url': game_url, 'error': 'Could not parse game URL'})
                continue
            
            game_info.update({
                'season': season,
                'season_type': season_type,
                'week': week
            })
            
            # Add placeholder team data (will be extracted from the actual page)
            game_info.update({
                'away_team': 'TBD',
                'home_team': 'TBD'
            })
            
            # Scrape game statistics
            all_teams_data = scraper.scrape_game_boxscore(game_info)
            
            if all_teams_data:
                # Save to CSV
                csv_files = scraper.save_statistics_to_csv(all_teams_data, game_info)
                created_files.extend(csv_files)
                processed_games.append(game_info)
                logger.info(f"✅ Successfully processed game {game_info['game_id']}")
            else:
                failed_games.append({
                    'url': game_url, 
                    'game_id': game_info.get('game_id'),
                    'error': 'No statistics found or game postponed'
                })
                logger.warning(f"❌ Failed to get statistics for game {game_info.get('game_id')}")
                
        except Exception as e:
            logger.error(f"❌ Error processing {game_url}: {e}")
            failed_games.append({'url': game_url, 'error': str(e)})
    
    # Generate summary
    summary = {
        'success': True,
        'file_path': str(file_path),
        'season': season,
        'season_type': season_type,
        'week': week,
        'total_games': len(game_urls),
        'processed': len(processed_games),
        'failed': len(failed_games),
        'created_files': created_files,
        'processed_games': processed_games,
        'failed_games': failed_games
    }
    
    # Save processing summary
    summary_file = file_path.parent / f"{file_path.stem}_summary.json"
    try:
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Summary saved to: {summary_file}")
    except Exception as e:
        logger.warning(f"Could not save summary: {e}")
    
    return summary

def main():
    """Main entry point for command line usage"""
    parser = argparse.ArgumentParser(description='Process NFL games from URL file')
    parser.add_argument('file', help='Path to file containing game URLs')
    parser.add_argument('--season', type=int, help='Override season year')
    parser.add_argument('--week', type=int, help='Override week number')
    parser.add_argument('--preseason', action='store_true', help='Mark as preseason games')
    parser.add_argument('--playoffs', action='store_true', help='Mark as playoff games')
    
    args = parser.parse_args()
    
    # Validate file path
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    
    # Determine season type
    season_type = 2  # Regular season default
    if args.preseason:
        season_type = 1
    elif args.playoffs:
        season_type = 3
    
    try:
        # Process the game file
        summary = process_game_file(
            file_path, 
            season=args.season, 
            week=args.week, 
            season_type=season_type
        )
        
        if summary['success']:
            print(f"\n✅ Successfully processed NFL games from {file_path.name}")
            print(f"   Season: {summary['season']}, Week: {summary['week']}, Type: {summary['season_type']}")
            print(f"   Games processed: {summary['processed']}/{summary['total_games']}")
            print(f"   Files created: {len(summary['created_files'])}")
            
            if summary['failed'] > 0:
                print(f"   ❌ Failed games: {summary['failed']}")
                
            print(f"\n📁 Output files:")
            for file_path in summary['created_files']:
                print(f"   • {file_path}")
                
        else:
            print(f"\n❌ Failed to process {file_path.name}: {summary.get('message', 'Unknown error')}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()