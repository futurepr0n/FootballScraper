# Production NFL Boxscore Scraper

Clean, production-ready NFL boxscore scraper with proper game date extraction and official team abbreviations.

## Key Features

✅ **Accurate Game Dates** - Extracts actual game dates from ESPN (not scraping date)  
✅ **Official NFL Team Abbreviations** - Proper NYG/NYJ and LAR/LAC differentiation  
✅ **Clean CSV Filenames** - No compound names or malformed abbreviations  
✅ **BOXSCORE_CSV Storage** - Separate directory for clean production data  
✅ **Transaction-based Database Loading** - Reliable data integrity  
✅ **Comprehensive Error Handling** - Production-ready reliability

## Directory Structure

```
Production/
├── production_nfl_boxscore_scraper.py    # Main scraper
├── production_csv_loader.py              # Database loader  
├── run_production_scraper.py             # Pipeline runner
└── README.md                             # This file

../FootballData/BOXSCORE_CSV/             # Clean CSV output
```

## Quick Start

### 1. Scrape and Load Week Data

```bash
cd Production/
python run_production_scraper.py --url-file ../regular_week1_2025.txt --season 2025 --week 1
```

### 2. Scrape Individual Games

```bash
python run_production_scraper.py --urls \
  "https://www.espn.com/nfl/game/_/gameId/401772718" \
  "https://www.espn.com/nfl/game/_/gameId/401772719" \
  --season 2025 --week 1
```

### 3. Scrape Only (No Database Loading)

```bash
python run_production_scraper.py --url-file ../regular_week2_2025.txt --season 2025 --week 2 --scrape-only
```

## CSV Output Format

### Filename Convention
```
nfl_{TEAM}_{STAT_CATEGORY}_week{WEEK}_{GAME_DATE}_{GAME_ID}.csv
```

### Examples
```
nfl_NYG_passing_week1_20250908_401772918.csv      # Actual game date
nfl_NYJ_rushing_week1_20250908_401772825.csv      # Clear team differentiation  
nfl_LAR_receiving_week2_20250915_401773020.csv    # Clean stat categories
nfl_NO_kicking_week1_20250908_401772718.csv       # No compound names
```

### Team Abbreviations (Official NFL)

**Same-City Team Differentiation:**
- **NYG** = New York Giants 
- **NYJ** = New York Jets
- **LAR** = Los Angeles Rams
- **LAC** = Los Angeles Chargers

**All 32 Teams:**
```
AFC East: BUF, MIA, NE, NYJ
AFC North: BAL, CIN, CLE, PIT  
AFC South: HOU, IND, JAX, TEN
AFC West: DEN, KC, LAC, LV

NFC East: DAL, NYG, PHI, WSH
NFC North: CHI, DET, GB, MIN
NFC South: ATL, CAR, NO, TB
NFC West: ARI, LAR, SF, SEA
```

## Individual Component Usage

### Production Scraper Only

```bash
python production_nfl_boxscore_scraper.py --url-file ../regular_week1_2025.txt --season 2025 --week 1
```

**Features:**
- Extracts actual game dates from ESPN pages
- Uses official NFL team abbreviations
- Handles compound team names correctly
- Clean stat category naming
- Saves to BOXSCORE_CSV directory

### Database Loader Only  

```bash
python production_csv_loader.py --boxscore-dir ../../FootballData/BOXSCORE_CSV --season 2025 --week 1
```

**Features:**
- Validates team abbreviations against official NFL teams
- Transaction-based loading for data integrity
- Comprehensive error handling and reporting
- Supports loading specific weeks or entire directories

## Game Date Extraction

The scraper uses multiple methods to extract accurate game dates:

1. **Game Info Sections** - ESPN game metadata
2. **Breadcrumb Navigation** - Page navigation elements  
3. **Meta Tags** - HTML meta properties
4. **URL Pattern Analysis** - Date parameters in URLs

**Fallback:** Uses "UNKNOWN_DATE" instead of current date if extraction fails

## Error Handling

### Scraping Errors
- Network timeouts and connection issues
- ESPN page structure changes
- Missing game data

### Loading Errors  
- Invalid team abbreviations
- Database connection issues
- Malformed CSV data
- Transaction rollback on errors

### Logging
- Comprehensive logging to console and files
- Separate log files: `production_scraper.log`, `production_loader.log`
- Configurable log levels (INFO, DEBUG)

## Database Schema

Loads data into existing PostgreSQL schema:

```sql
-- Teams with official abbreviations
INSERT INTO teams (abbreviation, name, conference, division)

-- Players with proper position mapping  
INSERT INTO players (name, team_id, position, jersey_number)

-- Games with season/week organization
INSERT INTO games (game_id, season, week, season_type, date)

-- Player game statistics with conflict resolution
INSERT INTO player_game_stats (player_id, game_id, team_id, ...)
ON CONFLICT (player_id, game_id) DO UPDATE SET ...
```

## Configuration

### Environment Variables
```bash
DB_HOST=192.168.1.23          # Database host
DB_NAME=football_tracker      # Database name  
DB_USER=postgres              # Database user
DB_PASSWORD=korn5676          # Database password
DB_PORT=5432                  # Database port
```

### Default Paths
```python
BOXSCORE_CSV = "../../../FootballData/BOXSCORE_CSV/"
URL_FILES = "../regular_week{N}_2025.txt"
```

## Comparison: Old vs New System

| Feature | Old Enhanced Scraper | New Production Scraper |
|---------|---------------------|----------------------|
| **Game Dates** | Current date (20250913) | Actual game date (20250908) |
| **Team Names** | "NE_orleans", "DAL_vegas" | "NO", "LV" |
| **NY Teams** | Poor differentiation | Clear NYG vs NYJ |
| **LA Teams** | Ambiguous mapping | Clear LAR vs LAC |
| **CSV Location** | CSV_BACKUPS (mixed) | BOXSCORE_CSV (clean) |
| **Stat Categories** | "kick returns", "bay kicking" | "kick_returns", "kicking" |
| **Error Handling** | Basic logging | Transaction-based |
| **Validation** | Minimal | Official NFL teams |

## Troubleshooting

### Common Issues

**1. Invalid team abbreviation warnings:**
```
WARNING: Invalid team abbreviation in filename: XYZ
```
**Solution:** Check team name parsing logic for new ESPN formats

**2. Game date extraction fails:**
```
WARNING: Could not extract game date from URL
```
**Result:** Filename uses "UNKNOWN_DATE" - manual correction may be needed

**3. Database connection errors:**
```
ERROR: Database connection failed: connection refused
```  
**Solution:** Check database server and credentials

**4. Empty CSV files:**
```
INFO: Saved 0 records for TEAM category
```
**Solution:** ESPN may not have stats for that game/category yet

### Performance Tips

- **Batch Processing:** Use URL files for multiple games
- **Week-specific Loading:** Use `--week N` for targeted database loading  
- **Scrape-only Mode:** Use `--scrape-only` to generate CSVs without database load
- **Verbose Logging:** Use `--verbose` for debugging

## Development Notes

This production system was designed to address specific issues with the original scraper:

1. **Filename Issues:** Malformed team abbreviations and incorrect dates
2. **Team Differentiation:** Poor handling of same-city teams (NY, LA)
3. **Data Quality:** Mixed clean and malformed data in same directory
4. **Production Reliability:** Better error handling and validation

The system maintains backward compatibility with existing database schema while providing clean, reliable data generation.