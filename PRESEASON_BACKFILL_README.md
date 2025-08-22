# NFL Preseason Data Backfill System

A comprehensive data collection and processing system for NFL preseason games, designed to create organized datasets for dashboard components and analytics.

## 🎯 System Overview

This system collects, processes, and organizes comprehensive NFL preseason data including:
- **Box Scores**: Team and player statistics
- **Play-by-Play Data**: Detailed drive and play information
- **Player Performance**: Individual player stats across categories
- **Team Summaries**: Win/loss records and scoring averages
- **Leaderboards**: Touchdowns, interceptions, position rankings

## 📁 File Structure

```
FootballScraper/
├── nfl_preseason_backfill.py      # Main backfill system
├── nfl_preseason_aggregator.py    # Data aggregation & leaderboards
├── nfl_playbyplay_scraper.py      # Play-by-play extraction (existing)
├── test_preseason_system.py       # System validation & testing
├── run_preseason_backfill.sh      # Automated batch processing script
└── august_*_2025.json             # Schedule files with game IDs

FootballData/
├── data/preseason/
│   ├── box_scores/                # Game box score files
│   ├── play_by_play/              # Detailed play data
│   ├── game_summaries/            # Basic game info
│   ├── player_stats/              # Player performance files
│   ├── team_stats/                # Team-level statistics
│   ├── aggregated/                # Processed leaderboards & summaries
│   └── *_complete.json            # Comprehensive game files
├── logs/                          # Processing logs
└── preseason_backfill_progress.json  # Progress tracking
```

## 🚀 Quick Start

### 1. Test the System
```bash
cd FootballScraper
python3 test_preseason_system.py
```
This validates API connectivity and tests with a single game.

### 2. Run Full Backfill
```bash
./run_preseason_backfill.sh
```
This automated script will:
- Check requirements
- Show progress estimates
- Run complete data collection
- Generate aggregated summaries
- Provide dashboard-ready files

### 3. Manual Execution
```bash
# Data collection only
python3 nfl_preseason_backfill.py

# Aggregation only (after backfill)
python3 nfl_preseason_aggregator.py
```

## 📊 Data Output

### Primary Dashboard Files
Located in `FootballData/data/preseason/aggregated/`:

- **`dashboard_summary.json`** - Main dashboard data with top performers
- **`touchdown_leaders.json`** - Complete TD leaderboard  
- **`interception_data.json`** - INT statistics (thrown & caught)
- **`position_leaders.json`** - Leaders by position (QB, RB, WR, TE)
- **`team_summaries.json`** - Team records and scoring averages

### Individual Game Files
Located in `FootballData/data/preseason/`:

- **`YYYY-MM-DD_weekN_GAMEID_complete.json`** - All data for one game
- **`box_scores/YYYY-MM-DD_weekN_GAMEID_boxscore.json`** - Box score only
- **`play_by_play/YYYY-MM-DD_weekN_GAMEID_plays.json`** - Plays only

## 🎮 Dashboard Integration Examples

### Touchdown Leaders Component
```javascript
const touchdownData = await fetch('/data/preseason/aggregated/touchdown_leaders.json');
const leaders = await touchdownData.json();

// Display top 10 TD scorers with breakdown
leaders.slice(0, 10).map(player => (
  <div key={player.player_id}>
    <h3>{player.name} ({player.position}) - {player.team}</h3>
    <div>Total TDs: {player.total_touchdowns}</div>
    <div>
      Passing: {player.touchdown_breakdown.passing || 0},
      Rushing: {player.touchdown_breakdown.rushing || 0},
      Receiving: {player.touchdown_breakdown.receiving || 0}
    </div>
  </div>
));
```

### Team Standings Component  
```javascript
const teamData = await fetch('/data/preseason/aggregated/team_summaries.json');
const teams = await teamData.json();

// Sort by win percentage
const standings = Object.values(teams)
  .sort((a, b) => b.win_percentage - a.win_percentage);
```

### Position Leaders Component
```javascript
const positionData = await fetch('/data/preseason/aggregated/position_leaders.json');
const leaders = await positionData.json();

// Display top QBs
const topQBs = leaders.QB.slice(0, 5).map(qb => (
  <div key={qb.player_id}>
    {qb.name}: {qb.passing_yards} yds, {qb.passing_tds} TDs
  </div>
));
```

## ⚡ System Features

### Rate Limiting & Respectful Scraping
- 1.5 second delay between ESPN API requests
- Retry logic with exponential backoff
- Request timeout handling
- User-Agent rotation

### Progress Tracking & Resumption
- Automatic progress saving after each game
- Resume capability from interruption
- Failed game tracking with error details
- Processing time estimates

### Comprehensive Error Handling
- Network timeout management
- Invalid response handling
- File system error recovery
- Graceful degradation for missing data

### Data Quality Features
- JSON validation for all files
- Statistical consistency checks
- Missing data identification
- Duplicate game detection

## 📈 Processing Estimates

**For Full August 2025 Preseason (estimated ~60 games):**
- Processing Time: ~3-4 minutes
- Data Size: ~30-50MB total
- API Requests: ~180 (3 per game with retries)
- Generated Files: ~300+ individual files

## 🔧 Configuration & Customization

### Modify Rate Limiting
In `nfl_preseason_backfill.py`:
```python
self.request_delay = 1.5  # Seconds between requests
self.max_retries = 3      # Retry attempts
self.retry_delay = 5      # Delay between retries
```

### Add New Statistical Categories
In `nfl_preseason_aggregator.py`:
```python
self.stat_categories = {
    'passing': ['yards', 'touchdowns', 'interceptions', ...],
    'rushing': ['yards', 'touchdowns', 'attempts', ...],
    # Add new categories here
}
```

### Customize Output Structure
Modify the aggregation functions to change leaderboard criteria or add new summary types.

## 🚨 Troubleshooting

### Common Issues

**"No schedule files found"**
- Ensure `august_*_2025.json` files exist in FootballScraper directory
- Run `nfl_schedule_generator.py` if needed

**"ESPN API connection failed"**
- Check internet connectivity
- Verify ESPN endpoints are accessible
- May be temporary - retry later

**"Progress file corrupted"**
- Delete `FootballData/preseason_backfill_progress.json`
- Restart process (will begin fresh)

**"Incomplete aggregation"**  
- Ensure backfill completed successfully first
- Check that `*_complete.json` files exist
- Review aggregation logs for specific errors

### Log Files
All processing logs are saved in:
- `logs/preseason_backfill_YYYYMMDD_HHMMSS.log`
- `logs/preseason_aggregation_YYYYMMDD_HHMMSS.log`
- `FootballData/logs/nfl_preseason_backfill.log`

## 🎯 Data Usage Examples

### Find All Games for a Team
```python
import json
from pathlib import Path

def find_team_games(team_abbr):
    games = []
    for file in Path("../FootballData/data/preseason").glob("*_complete.json"):
        with open(file) as f:
            data = json.load(f)
        
        teams = data['game_summary']['teams']
        if any(t['abbreviation'] == team_abbr for t in teams):
            games.append(data)
    
    return games

# Get all Chiefs preseason games
chiefs_games = find_team_games('KC')
```

### Extract Player Statistics
```python
def get_player_stats(player_name):
    stats = {}
    for file in Path("../FootballData/data/preseason/player_stats").glob("*.json"):
        with open(file) as f:
            data = json.load(f)
        
        for team, players in data.items():
            for player_id, player_data in players.items():
                if player_name.lower() in player_data['name'].lower():
                    stats[file.stem] = player_data
    
    return stats
```

## 🔄 System Updates

### Adding New Data Points
1. Modify `extract_*` methods in `NFLPreseasonBackfill`
2. Update aggregation logic in `NFLPreseasonAggregator`  
3. Test with single game before full run
4. Update dashboard integration code

### Extending to Regular Season
The system architecture supports regular season games:
1. Modify schedule file detection patterns
2. Adjust season type filtering
3. Update file naming conventions
4. Scale rate limiting for larger datasets

## 📋 System Requirements

- Python 3.7+
- `requests` library
- `pathlib` (standard library)
- 50MB+ free disk space
- Internet connection for ESPN API

## 🎉 Success Metrics

After successful completion, you should have:
- ✅ Complete box scores for all preseason games
- ✅ Detailed play-by-play data with situational analysis
- ✅ Player leaderboards across all statistical categories
- ✅ Team standings and performance summaries
- ✅ Dashboard-ready JSON files for immediate integration
- ✅ Comprehensive logging for process validation

---

**Ready to build amazing football dashboards with comprehensive preseason data! 🏈**