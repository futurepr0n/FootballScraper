"""
Centralized data path configuration for FootballScraper
This configuration enables all scraper scripts to use centralized data storage in FootballData
following the same pattern as BaseballScraper
"""

import os
from pathlib import Path

# Determine environment
IS_PRODUCTION = os.environ.get('NODE_ENV') == 'production' or os.path.exists('/app')

# Get the base directory of FootballScraper
SCRAPER_DIR = Path(__file__).parent

# Environment-aware data path resolution
def get_data_path():
    """Get the centralized data path from environment or defaults"""
    # First check for explicit environment variable
    if env_path := os.environ.get('FOOTBALL_DATA_PATH'):
        return Path(env_path).resolve()
    
    # Fallback to defaults based on environment
    if IS_PRODUCTION:
        return Path('/app/FootballData/data')
    else:
        # Development fallback - relative to FootballScraper
        return SCRAPER_DIR.parent / 'FootballData' / 'data'

# Base data path using environment-aware resolution
DATA_PATH = get_data_path()

# Ensure data path exists
DATA_PATH.mkdir(parents=True, exist_ok=True)

# Define specific data subdirectories
PATHS = {
    'data': DATA_PATH,
    'predictions': DATA_PATH / 'predictions',
    'stats': DATA_PATH / 'stats',
    'rolling_stats': DATA_PATH / 'rolling_stats',
    'team_stats': DATA_PATH / 'team_stats',
    'rosters': DATA_PATH / 'rosters.json',
    'odds': DATA_PATH / 'odds',
    'lineups': DATA_PATH / 'lineups',
    'hellraiser': DATA_PATH / 'hellraiser',
    'injuries': DATA_PATH / 'injuries',
    'handedness': DATA_PATH / 'handedness',
    'stadium': DATA_PATH / 'stadium',
    'multi_td_stats': DATA_PATH / 'multi_td_stats',
    'scanned': DATA_PATH.parent / 'SCANNED',  # Centralized processed schedule files
    'csv_backups': DATA_PATH.parent / 'CSV_BACKUPS',  # Centralized CSV backup files
}

# Legacy paths for backward compatibility (will be removed after migration)
LEGACY_PATHS = {
    'tracker_public': SCRAPER_DIR.parent / 'FootballTracker' / 'public' / 'data',
    'tracker_build': SCRAPER_DIR.parent / 'FootballTracker' / 'build' / 'data',
}

# NFL season configuration
NFL_SEASON_START = 9  # September
NFL_SEASON_END = 2    # February (next year)
NFL_REGULAR_SEASON_WEEKS = 18
NFL_PLAYOFF_WEEKS = 5

# ESPN NFL URL patterns
ESPN_NFL_BASE = "https://www.espn.com/nfl"
ESPN_NFL_SCORES = f"{ESPN_NFL_BASE}/scoreboard"
ESPN_NFL_SCHEDULE = f"{ESPN_NFL_BASE}/schedule"

# Utility functions
def get_data_path(*segments):
    """Get a path within the data directory"""
    return DATA_PATH.joinpath(*segments)

def get_game_data_path(year, week=None, day=None):
    """Get path for game data files"""
    if week and day:
        filename = f"week_{week:02d}_{day}_{year}.json"
        return DATA_PATH / str(year) / f"week_{week:02d}" / filename
    elif week:
        return DATA_PATH / str(year) / f"week_{week:02d}"
    else:
        return DATA_PATH / str(year)

def get_week_name(week_num):
    """Convert week number to name"""
    if 1 <= week_num <= NFL_REGULAR_SEASON_WEEKS:
        return f"week_{week_num:02d}"
    elif week_num == 19:
        return "wildcard"
    elif week_num == 20:
        return "divisional"
    elif week_num == 21:
        return "conference"
    elif week_num == 22:
        return "superbowl"
    else:
        return f"week_{week_num:02d}"

def ensure_dir(path):
    """Ensure a directory exists"""
    Path(path).mkdir(parents=True, exist_ok=True)
    return path

# For scripts that write to multiple locations (dev/prod sync)
def get_output_dirs(subpath=''):
    """
    Get both development and production output directories
    During migration, this returns the centralized path twice
    After migration, scripts can be updated to use single path
    """
    base_path = DATA_PATH / subpath if subpath else DATA_PATH
    return [str(base_path), str(base_path)]  # Same path for both

# Debug information
if __name__ == "__main__":
    print(f"FootballScraper Configuration:")
    print(f"  Environment: {'Production' if IS_PRODUCTION else 'Development'}")
    print(f"  Data Path: {DATA_PATH}")
    print(f"  Data Path Exists: {DATA_PATH.exists()}")
    print(f"  Key Paths:")
    for name, path in PATHS.items():
        print(f"    {name}: {path}")