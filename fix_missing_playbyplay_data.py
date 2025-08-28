#!/usr/bin/env python3
"""
Fix Missing Play-by-Play Data
Specifically addresses Aug 22-23 games that have empty drives/plays arrays
"""

import json
import requests
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import sys
import os

# Import the existing scraper classes
from nfl_playbyplay_scraper import NFLPlayByPlayScraper
from config import PATHS

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlayByPlayFixer:
    """Fix missing play-by-play data for specific games"""
    
    def __init__(self):
        self.scraper = NFLPlayByPlayScraper()
        self.fixed_games = []
        self.failed_games = []
        
    def identify_empty_games(self, date_pattern: str = "2025-08-2") -> List[Dict]:
        """Identify games with empty drives/plays arrays"""
        play_by_play_dir = PATHS['data'] / 'preseason' / 'play_by_play'
        empty_games = []
        
        logger.info(f"Scanning for empty play-by-play files matching pattern: {date_pattern}")
        
        for file_path in play_by_play_dir.glob(f"{date_pattern}*_play_by_play.json"):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Check if drives and plays are empty
                game_info = data.get('game_info', {})
                drives = data.get('drives', [])
                plays = data.get('plays', [])
                
                if len(drives) == 0 and len(plays) == 0:
                    # Extract game ID from filename: YYYY-MM-DD_GAMEID_play_by_play.json
                    filename = file_path.name
                    parts = filename.split('_')
                    if len(parts) >= 3:
                        game_id = parts[1]  # The game ID is the second part
                        
                        empty_game = {
                            'file_path': file_path,
                            'game_id': game_id,
                            'date': parts[0],
                            'teams': game_info.get('teams', [])
                        }
                        
                        empty_games.append(empty_game)
                        logger.info(f"Found empty game: {filename} (Game ID: {game_id})")
                        
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue
        
        logger.info(f"Found {len(empty_games)} games with empty play-by-play data")
        return empty_games
    
    def fix_game_data(self, game_info: Dict) -> bool:
        """Fix a single game's play-by-play data"""
        game_id = game_info['game_id']
        file_path = game_info['file_path']
        
        logger.info(f"Fixing game {game_id}")
        
        try:
            # Fetch fresh play-by-play data from ESPN
            play_data = self.scraper.get_game_play_by_play(game_id)
            
            if not play_data:
                logger.error(f"Failed to fetch play-by-play data for game {game_id}")
                self.failed_games.append(game_info)
                return False
            
            # Extract structured data
            structured_data = self.scraper.extract_play_data(play_data)
            
            if not structured_data:
                logger.error(f"Failed to extract play data for game {game_id}")
                self.failed_games.append(game_info)
                return False
            
            # Check if we got actual data
            drives = structured_data.get('drives', [])
            plays = structured_data.get('plays', [])
            
            if len(drives) == 0 and len(plays) == 0:
                logger.warning(f"Game {game_id} still has no drives/plays - may not have started yet")
                # Don't treat this as a failure - game may not have started
                return True
            
            # Preserve existing game_info structure but update with fresh data
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
                
                # Merge the structured data with existing structure
                existing_data.update({
                    'drives': structured_data.get('drives', []),
                    'plays': structured_data.get('plays', []),
                    'scoring_plays': structured_data.get('scoring_plays', []),
                    'touchdowns': self._extract_touchdowns(structured_data),
                    'interceptions': self._extract_interceptions(structured_data),
                    'fumbles': self._extract_fumbles(structured_data)
                })
                
                # Update the game_info if we have better data
                if structured_data.get('game_info'):
                    existing_data['game_info'].update(structured_data['game_info'])
                
                # Add processing timestamp
                existing_data['last_updated'] = datetime.now().isoformat()
                
                # Write back to file
                with open(file_path, 'w') as f:
                    json.dump(existing_data, f, indent=2)
                
                logger.info(f"Successfully fixed game {game_id}: {len(drives)} drives, {len(plays)} plays")
                self.fixed_games.append(game_info)
                return True
                
            except Exception as e:
                logger.error(f"Error updating file {file_path}: {e}")
                self.failed_games.append(game_info)
                return False
            
        except Exception as e:
            logger.error(f"Error fixing game {game_id}: {e}")
            self.failed_games.append(game_info)
            return False
    
    def _extract_touchdowns(self, structured_data: Dict) -> List[Dict]:
        """Extract touchdown plays from structured data"""
        touchdowns = []
        
        scoring_plays = structured_data.get('scoring_plays', [])
        for play in scoring_plays:
            if 'touchdown' in play.get('text', '').lower():
                touchdowns.append(play)
        
        return touchdowns
    
    def _extract_interceptions(self, structured_data: Dict) -> List[Dict]:
        """Extract interception plays from structured data"""
        interceptions = []
        
        plays = structured_data.get('plays', [])
        for play in plays:
            if 'intercept' in play.get('text', '').lower():
                interceptions.append(play)
        
        return interceptions
    
    def _extract_fumbles(self, structured_data: Dict) -> List[Dict]:
        """Extract fumble plays from structured data"""
        fumbles = []
        
        plays = structured_data.get('plays', [])
        for play in plays:
            if 'fumble' in play.get('text', '').lower():
                fumbles.append(play)
        
        return fumbles
    
    def run_fix(self, date_pattern: str = "2025-08-2") -> Dict:
        """Main execution function"""
        logger.info("Starting Play-by-Play Data Fix")
        logger.info("=" * 50)
        
        # Identify empty games
        empty_games = self.identify_empty_games(date_pattern)
        
        if not empty_games:
            logger.info("No empty games found - all play-by-play data appears complete")
            return {
                'success': True,
                'total_games': 0,
                'fixed_games': 0,
                'failed_games': 0,
                'message': 'No games needed fixing'
            }
        
        # Fix each game
        for game_info in empty_games:
            try:
                self.fix_game_data(game_info)
                # Rate limiting
                time.sleep(2)
            except KeyboardInterrupt:
                logger.warning("Fix process interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error fixing game {game_info.get('game_id', 'unknown')}: {e}")
                self.failed_games.append(game_info)
        
        # Summary
        total_games = len(empty_games)
        fixed_count = len(self.fixed_games)
        failed_count = len(self.failed_games)
        
        logger.info("\n" + "=" * 50)
        logger.info("Play-by-Play Fix Summary")
        logger.info("=" * 50)
        logger.info(f"Total games processed: {total_games}")
        logger.info(f"Successfully fixed: {fixed_count}")
        logger.info(f"Failed to fix: {failed_count}")
        
        if self.fixed_games:
            logger.info("\nFixed games:")
            for game in self.fixed_games:
                logger.info(f"  ✅ {game['date']} - Game {game['game_id']}")
        
        if self.failed_games:
            logger.info("\nFailed games:")
            for game in self.failed_games:
                logger.info(f"  ❌ {game['date']} - Game {game['game_id']}")
        
        success = failed_count == 0
        return {
            'success': success,
            'total_games': total_games,
            'fixed_games': fixed_count,
            'failed_games': failed_count,
            'message': f"Fixed {fixed_count}/{total_games} games"
        }

def main():
    """Command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix missing NFL play-by-play data')
    parser.add_argument('--date-pattern', default='2025-08-2', 
                       help='Date pattern to match (default: 2025-08-2)')
    parser.add_argument('--specific-game', 
                       help='Fix specific game ID only')
    
    args = parser.parse_args()
    
    fixer = PlayByPlayFixer()
    
    if args.specific_game:
        # Fix specific game
        logger.info(f"Fixing specific game: {args.specific_game}")
        # Implementation for specific game would go here
        print("Specific game fixing not yet implemented")
        return
    
    # Run the general fix
    result = fixer.run_fix(args.date_pattern)
    
    if result['success']:
        print(f"\n✅ Play-by-play fix completed successfully!")
        print(f"Fixed {result['fixed_games']}/{result['total_games']} games")
        if result['total_games'] == 0:
            print("No games needed fixing.")
    else:
        print(f"\n❌ Play-by-play fix completed with errors")
        print(f"Fixed {result['fixed_games']}/{result['total_games']} games")
        print(f"Failed: {result['failed_games']} games")
        sys.exit(1)

if __name__ == "__main__":
    main()