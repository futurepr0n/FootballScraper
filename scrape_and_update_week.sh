#!/bin/bash
#
# Scrape NFL boxscores and update game scores in database
# Usage: ./scrape_and_update_week.sh [season] [week]
#

# Default values
SEASON=${1:-2025}
WEEK=${2:-1}

echo "========================================="
echo "NFL Data Pipeline - Season $SEASON Week $WEEK"
echo "========================================="

# Step 1: Run the boxscore scraper
echo ""
echo "[1/2] Scraping boxscore data from ESPN..."
echo "-----------------------------------------"

# Check if we have a URLs file for this week
URL_FILE="week_${WEEK}_urls.txt"
if [ -f "$URL_FILE" ]; then
    echo "Using URL file: $URL_FILE"
    python3 production_nfl_boxscore_scraper.py --url-file "$URL_FILE" --season "$SEASON" --week "$WEEK"
else
    echo "No URL file found for week $WEEK"
    echo "Please create $URL_FILE with game URLs or pass URLs directly"
    echo "Example: python3 production_nfl_boxscore_scraper.py --urls URL1 URL2 --season $SEASON --week $WEEK"
    exit 1
fi

# Check if scraping was successful
if [ $? -ne 0 ]; then
    echo "❌ Scraping failed!"
    exit 1
fi

# Step 2: Update game scores in database
echo ""
echo "[2/2] Updating game scores in database..."
echo "-----------------------------------------"

python3 update_game_scores.py --season "$SEASON" --week "$WEEK"

# Check if update was successful
if [ $? -ne 0 ]; then
    echo "❌ Score update failed!"
    exit 1
fi

# Step 3: Fix any data issues (dates, duplicate teams)
echo ""
echo "[3/3] Fixing game data issues..."
echo "-----------------------------------------"

python3 fix_game_data.py

# Check if fix was successful
if [ $? -ne 0 ]; then
    echo "⚠️ Data fix encountered issues, but continuing..."
fi

echo ""
echo "========================================="
echo "✅ Pipeline complete for Week $WEEK!"
echo "========================================="

# Optional: Display updated scores
echo ""
echo "Updated scores:"
PGPASSWORD=korn5676 psql -h 192.168.1.23 -U postgres -d football_tracker -t -c "
SELECT
    at.abbreviation || ' ' || g.away_score || ' @ ' ||
    ht.abbreviation || ' ' || g.home_score as game_result
FROM games g
JOIN teams ht ON g.home_team_id = ht.id
JOIN teams at ON g.away_team_id = at.id
WHERE g.season = $SEASON AND g.week = $WEEK
ORDER BY g.game_id;"