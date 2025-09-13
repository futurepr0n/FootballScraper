# NFL Production Scrapers

This directory contains the finalized, working NFL scraping workflow for collecting and loading game statistics into the database.

## Core Workflow

The production scraping process consists of 3 main components:

### 1. Smart Game Processor (`smart_game_processor.py`)
**Purpose**: Intelligently processes only completed games from weekly schedule files

**Usage**:
```bash
python smart_game_processor.py regular_week2_2025.txt
```

**Features**:
- Parses game files with timestamps
- Checks if games are completed (game time + 3 hour buffer)
- Only processes finished games
- Creates temporary files for completed games
- Automatically runs the CSV scraper for valid games

### 2. NFL Game File Processor (`process_nfl_game_file.py`)
**Purpose**: Scrapes ESPN game URLs and generates CSV files with player statistics

**Usage**:
```bash
python process_nfl_game_file.py regular_week1_2025.txt
```

**Features**:
- Processes ESPN game URLs from schedule files
- Scrapes box scores for all statistical categories
- Generates CSV files organized by team and stat type
- Outputs to `../FootballData/CSV_BACKUPS/`

### 3. Simple CSV Loader (`simple_csv_loader.py`)
**Purpose**: Loads CSV files into PostgreSQL database with proper team assignments

**Usage**:
```bash
python simple_csv_loader.py --csv-dir ../FootballData/CSV_BACKUPS --season 2025 --week 1 --season-type regular
```

**Features**:
- Loads all CSV files for a specific week
- Creates/updates players, teams, and games tables
- Handles stat aggregation and conflict resolution
- Updates game team assignments after processing
- Maps all CSV columns to database fields (passing, rushing, receiving, etc.)

## Complete Workflow Example

### For Completed Games (Recommended):
```bash
# 1. Process only completed games automatically
python PRODUCTION_SCRAPERS/smart_game_processor.py regular_week2_2025.txt

# 2. Load generated CSV files into database
python PRODUCTION_SCRAPERS/simple_csv_loader.py --csv-dir ../FootballData/CSV_BACKUPS --season 2025 --week 2 --season-type regular
```

### For All Games (Manual):
```bash
# 1. Generate CSV files for all games in week
python PRODUCTION_SCRAPERS/process_nfl_game_file.py regular_week1_2025.txt

# 2. Load CSV files into database
python PRODUCTION_SCRAPERS/simple_csv_loader.py --csv-dir ../FootballData/CSV_BACKUPS --season 2025 --week 1 --season-type regular
```

## File Structure

```
PRODUCTION_SCRAPERS/
├── README.md                    # This documentation
├── smart_game_processor.py      # Smart completion-based scraper
├── process_nfl_game_file.py     # ESPN URL scraper
└── simple_csv_loader.py         # Database loader
```

## Input Files

Weekly schedule files should be located in the parent directory:
- `regular_week1_2025.txt`
- `regular_week2_2025.txt`
- etc.

Format:
```
# Regular Season Week 1 - 2025
https://www.espn.com/nfl/game/_/gameId/401772714 | 2025-09-06T01:20Z
https://www.espn.com/nfl/game/_/gameId/401772918 | 2025-09-09T01:00Z
```

## Database Schema

The loader populates these key tables:
- **teams**: Team information (abbreviation, name, conference, division)
- **players**: Player information (name, team_id, position, jersey_number)
- **games**: Game information (game_id, season, week, home/away teams, date)
- **player_game_stats**: Individual player statistics per game

## CSV Output Structure

CSV files are generated in this format:
```
nfl_{TEAM}_{CATEGORY}_week{WEEK}_{DATE}_{GAMEID}.csv
```

Examples:
- `nfl_KC_receiving_week1_20250912_401772714.csv`
- `nfl_BUF_passing_week1_20250912_401772918.csv`

## Database Configuration

Ensure these environment variables are set:
```bash
export DB_HOST=192.168.1.23
export DB_USER=postgres
export DB_PASSWORD=korn5676
export DB_NAME=football_tracker
```

## Prerequisites

1. Python virtual environment activated
2. Required packages: `psycopg2-binary`, `requests`, `beautifulsoup4`, `pandas`
3. PostgreSQL database accessible
4. ESPN URLs in proper format with timestamps

## Verification

After loading, verify data with:
```bash
# Check top receivers
PGPASSWORD=korn5676 psql -h 192.168.1.23 -U postgres -d football_tracker -c "
SELECT p.name, p.position, pgs.receptions, pgs.targets, pgs.receiving_yards, t.abbreviation 
FROM player_game_stats pgs 
JOIN players p ON pgs.player_id = p.id 
JOIN teams t ON p.team_id = t.id 
JOIN games g ON pgs.game_id = g.id 
WHERE g.season = 2025 AND g.week = 1 AND pgs.receptions > 0 
ORDER BY pgs.receptions DESC LIMIT 10;"

# Check API endpoints
curl "http://localhost:4201/api/nfl/players/top-receivers?limit=5"
```

## Troubleshooting

1. **Empty database**: Ensure CSV files exist in `../FootballData/CSV_BACKUPS/`
2. **Missing receptions/targets**: Verify CSV files contain 'rec' and 'tgts' columns
3. **Wrong game teams**: Check that CSV files have correct team abbreviations
4. **Duplicate data**: Clear existing data for the week before reloading

This production workflow ensures reliable, automated scraping and database loading for NFL statistics.