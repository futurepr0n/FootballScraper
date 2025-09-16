# NFL Roster Scraping and Loading Guide

## Overview
This guide documents the complete process for scraping NFL roster data from ESPN and loading it into the PostgreSQL database. The system scrapes all 32 NFL teams' rosters, including player names, positions, jersey numbers, physical stats, and more.

## Key Scripts

### 1. `simple_roster_scraper.py`
**Purpose**: Main scraper for all 32 NFL teams  
**Location**: `/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper/`  
**Output**: CSV and JSON files in `../FootballData/rosters/`

### 2. `scrape_washington.py`
**Purpose**: Specialized scraper for Washington Commanders (if main scraper fails for WAS)  
**Location**: `/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper/`  
**Output**: CSV file and direct database import

### 3. `import_clean_rosters.py`
**Purpose**: Import scraped CSV data into PostgreSQL database  
**Location**: `/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper/`  
**Input**: CSV files from `../FootballData/rosters/`

## Prerequisites

### Python Environment
```bash
cd /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper
source venv/bin/activate
```

### Required Python Packages
- requests
- beautifulsoup4
- psycopg2-binary
- csv (standard library)
- json (standard library)
- pathlib (standard library)

### Database Configuration
- **Host**: 192.168.1.23
- **Database**: football_tracker
- **User**: postgres
- **Password**: korn5676
- **Table**: nfl_rosters

## Running the Scrapers

### Full Roster Scrape (All 32 Teams)
```bash
cd /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper
source venv/bin/activate
python simple_roster_scraper.py
```

**What it does:**
1. Tests with Buffalo Bills first to verify scraping logic
2. Scrapes all 32 NFL teams from ESPN roster pages
3. Parses player data including:
   - Name and jersey number (combined field is split)
   - Position
   - Age
   - Height
   - Weight (strips 'lbs' suffix)
   - Experience (handles 'R' for rookies)
   - College
   - Roster section (Offense/Defense/Special Teams/Practice Squad)
4. Saves individual team CSV files
5. Creates master CSV and JSON files with timestamp

**Output files:**
- Individual team files: `nfl_roster_[TEAM]_[TIMESTAMP].csv`
- Master file: `nfl_rosters_all_[TIMESTAMP].csv`
- JSON version: `nfl_rosters_all_[TIMESTAMP].json`

### Washington Commanders Fix (If Needed)
```bash
cd /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper
source venv/bin/activate
python scrape_washington.py
```

**When to use**: If the main scraper returns 0 players for Washington (WAS)

**What it does:**
1. Specifically targets Washington's roster page
2. Saves to CSV
3. Automatically imports to database
4. Updates players table positions

## Importing to Database

### Standard Import Process
```bash
cd /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper
source venv/bin/activate
python import_clean_rosters.py
```

**Note**: Edit line 24 to point to your latest CSV file:
```python
roster_file = Path("../FootballData/rosters/nfl_rosters_all_YYYYMMDD_HHMMSS.csv")
```

**What it does:**
1. Clears existing nfl_rosters table
2. Reads CSV file
3. Parses and validates data:
   - Converts age to integer
   - Removes 'lbs' from weight
   - Handles 'R' for rookie experience
   - Limits position field to 10 characters
4. Inserts into nfl_rosters table with UPSERT logic
5. Updates players table positions from roster data
6. Displays summary statistics

## Database Schema

### nfl_rosters Table
```sql
CREATE TABLE nfl_rosters (
    id SERIAL PRIMARY KEY,
    team VARCHAR(5),
    name VARCHAR(100),
    jersey VARCHAR(3),
    position VARCHAR(10),
    age INTEGER,
    height VARCHAR(10),
    weight INTEGER,
    experience INTEGER,
    college VARCHAR(100),
    image_url TEXT,
    roster_section VARCHAR(20),
    scraped_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, team)
);
```

## Verification Queries

### Check Total Players and Teams
```sql
SELECT 
    COUNT(*) as total_players,
    COUNT(DISTINCT team) as teams,
    SUM(CASE WHEN position = 'QB' THEN 1 ELSE 0 END) as quarterbacks
FROM nfl_rosters;
```

### Players Per Team
```sql
SELECT team, COUNT(*) as player_count
FROM nfl_rosters
GROUP BY team
ORDER BY team;
```

### Sample QBs
```sql
SELECT team, name, jersey, position
FROM nfl_rosters
WHERE position = 'QB'
ORDER BY team, name
LIMIT 20;
```

### Check Specific Players
```sql
SELECT p.name, p.position AS players_position, 
       r.position AS roster_position, r.team, r.jersey
FROM players p
LEFT JOIN nfl_rosters r ON p.name = r.name
WHERE p.name IN ('Jayden Daniels', 'Baker Mayfield', 'Josh Allen')
ORDER BY p.name;
```

## Troubleshooting

### Issue: Washington Returns 0 Players
**Solution**: Run the specialized Washington scraper
```bash
python scrape_washington.py
```

### Issue: CSV Data Malformed
**Symptoms**: Names and jersey numbers combined incorrectly
**Solution**: The scraper uses regex to split: `^(.+?)(\d+)$`

### Issue: Players Missing Positions
**Check**: Verify roster data has positions
```sql
SELECT COUNT(*) FROM nfl_rosters WHERE position IS NULL OR position = '';
```

### Issue: Duplicate Players
**Solution**: The import uses UPSERT with `ON CONFLICT (name, team)` to handle duplicates

## Expected Results

After successful scraping and import:
- **Total players**: ~2400-2500 (varies by roster cuts)
- **Teams**: 32
- **Players per team**: 68-83 (typical NFL roster size)
- **Quarterbacks**: ~95-105

## Data Quality Notes

1. **Jersey Numbers**: Some players may not have jersey numbers (practice squad)
2. **Experience**: 'R' indicates rookie (converted to 0)
3. **Weight**: Stored as integer without 'lbs'
4. **Height**: Stored as string (e.g., "6' 5\"")
5. **Position Limits**: Truncated to 10 characters for database constraint

## Maintenance

### Weekly Updates During Season
Run the full scrape weekly during the NFL season to capture roster changes:
```bash
# Sunday night or Monday morning
cd /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper
source venv/bin/activate
python simple_roster_scraper.py
python import_clean_rosters.py  # Update the CSV filename
```

### Backup Before Import
```sql
-- Create backup table
CREATE TABLE nfl_rosters_backup AS SELECT * FROM nfl_rosters;

-- After import, if issues:
TRUNCATE nfl_rosters;
INSERT INTO nfl_rosters SELECT * FROM nfl_rosters_backup;
```

## Integration with Other Systems

The roster data integrates with:
- **players table**: Position updates via JOIN on name and team
- **load-nfl-week-csvs.js**: Now checks roster for positions instead of inferring from stats
- **NFL Analytics**: Provides accurate position data for analysis

## File Locations

- **Scripts**: `/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper/`
- **Output Data**: `/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/rosters/`
- **Virtual Environment**: `/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper/venv/`

## Contact & Support

For issues with:
- **ESPN changes**: Update CSS selectors in scraper
- **Database access**: Check PostgreSQL connection at 192.168.1.23
- **Missing teams**: Check ESPN URL patterns (especially Washington/WAS vs WSH)