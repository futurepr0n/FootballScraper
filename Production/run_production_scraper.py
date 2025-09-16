#!/usr/bin/env python3
"""
Production NFL Scraper Runner
Convenience script to run the complete production scraping and loading pipeline.

Usage:
  python run_production_scraper.py --url-file ../regular_week1_2025.txt --season 2025 --week 1
  python run_production_scraper.py --urls URL1 URL2 URL3 --season 2025 --week 2
"""

import sys
import logging
from pathlib import Path
from production_nfl_boxscore_scraper import ProductionNFLBoxscoreScraper
from production_csv_loader import ProductionCSVLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_complete_pipeline(url_data: list, season: int, week: int, load_to_db: bool = True):
    """Run complete scraping and loading pipeline"""
    
    logger.info(f"=== Starting Production NFL Pipeline ===")
    logger.info(f"Season: {season}, Week: {week}")
    logger.info(f"Games to process: {len(url_data)}")
    logger.info(f"Load to database: {load_to_db}")
    
    # Step 1: Scrape games to CSV
    logger.info("\n=== STEP 1: Scraping Games ===")
    scraper = ProductionNFLBoxscoreScraper()
    
    try:
        scrape_result = scraper.process_games_with_dates(url_data, season, week)
        
        if not scrape_result['success']:
            logger.error("Scraping failed")
            return False
        
        logger.info(f"✅ Scraping complete: {scrape_result['processed']}/{scrape_result['total_games']} games")
        logger.info(f"   Created {len(scrape_result['created_files'])} CSV files")
        
        if scrape_result['failed']:
            logger.warning(f"   ⚠️  {len(scrape_result['failed'])} games failed")
        
        # Step 2: Load to database (if requested)
        if load_to_db and scrape_result['created_files']:
            logger.info(f"\n=== STEP 2: Loading to Database ===")
            
            boxscore_dir = Path(__file__).parent.parent.parent / 'FootballData' / 'BOXSCORE_CSV'
            loader = ProductionCSVLoader()
            
            try:
                load_result = loader.load_boxscore_directory(str(boxscore_dir), season, week)
                
                if load_result['success']:
                    logger.info(f"✅ Database loading complete")
                    logger.info(f"   Files processed: {load_result['files_processed']}")
                    logger.info(f"   Stats loaded: {load_result['stats_loaded']}")
                    
                    if load_result['errors']:
                        logger.warning(f"   ⚠️  {len(load_result['errors'])} warnings/errors")
                        for error in load_result['errors'][:3]:
                            logger.warning(f"     • {error}")
                        if len(load_result['errors']) > 3:
                            logger.warning(f"     ... and {len(load_result['errors']) - 3} more")
                else:
                    logger.error(f"❌ Database loading failed: {load_result['message']}")
                    return False
                    
            except Exception as e:
                logger.error(f"❌ Database loading error: {e}")
                return False
            finally:
                loader.close()
        
        logger.info(f"\n=== Pipeline Complete ===")
        logger.info(f"✅ Successfully processed Season {season}, Week {week}")
        
        if scrape_result['created_files']:
            logger.info(f"CSV Files created in BOXSCORE_CSV:")
            for file_path in scrape_result['created_files'][:5]:  # Show first 5
                logger.info(f"  • {Path(file_path).name}")
            if len(scrape_result['created_files']) > 5:
                logger.info(f"  ... and {len(scrape_result['created_files']) - 5} more")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        return False

def load_urls_from_file(file_path: str) -> list:
    """Load game URLs from file with dates from comments"""
    import re
    
    try:
        with open(file_path, 'r') as f:
            url_data = []
            current_date = None
            
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                # Check if line is a comment with date info
                if line.startswith('#'):
                    # Extract date from comment like "# LAC @ KC - 2025-09-06" or "# DAL @ PHI - 2025-09-05T00:20Z"
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', line)
                    if date_match:
                        current_date = date_match.group(1).replace('-', '')  # Convert to YYYYMMDD format
                        logger.info(f"Found game date {current_date} for upcoming games")
                    continue
                
                # Check if line contains a game URL
                if 'espn.com/nfl/game/' in line:
                    url_data.append({
                        'url': line,
                        'date': current_date
                    })
                    
            return url_data
    except FileNotFoundError:
        logger.error(f"URL file not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading URL file: {e}")
        return []

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Production NFL Scraper Pipeline')
    
    # URL input options
    url_group = parser.add_mutually_exclusive_group(required=True)
    url_group.add_argument('--urls', nargs='+', help='Game URLs to scrape')
    url_group.add_argument('--url-file', help='File containing game URLs (one per line)')
    
    # Game info
    parser.add_argument('--season', type=int, default=2025, help='Season year')
    parser.add_argument('--week', type=int, required=True, help='Week number')
    
    # Options
    parser.add_argument('--scrape-only', action='store_true', 
                       help='Only scrape to CSV, do not load to database')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get URLs
    if args.url_file:
        url_data = load_urls_from_file(args.url_file)
        if not url_data:
            print(f"❌ No valid URLs found in file: {args.url_file}")
            sys.exit(1)
        print(f"Loaded {len(url_data)} URLs from {args.url_file}")
    else:
        # Convert simple URLs to URL data format
        url_data = [{'url': url, 'date': None} for url in args.urls]
    
    # Validate inputs
    if not url_data:
        print("❌ No URLs provided")
        sys.exit(1)
    
    if args.week < 1 or args.week > 22:
        print(f"❌ Invalid week: {args.week} (must be 1-22)")
        sys.exit(1)
    
    # Run pipeline
    load_to_db = not args.scrape_only
    success = run_complete_pipeline(url_data, args.season, args.week, load_to_db)
    
    if success:
        print(f"\n🎉 Production pipeline completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Production pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()