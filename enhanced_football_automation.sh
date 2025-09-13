#!/bin/bash
# Enhanced Football Automation Script
# Processes all 465 games using the proven simple scraper approach
# Runs in batches to avoid memory issues and provide progress updates

echo "🏈 Enhanced Football Automation - Processing All 465 Games"
echo "==========================================================="

# Change to FootballScraper directory
cd /Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper

# Activate virtual environment
source venv/bin/activate

# Function to get remaining game count
get_remaining_games() {
    PGPASSWORD=korn5676 psql -h 192.168.1.23 -U postgres -d football_tracker -t -c "
        SELECT COUNT(*) FROM games g 
        WHERE NOT EXISTS (SELECT 1 FROM plays WHERE game_id = g.id)
    " | tr -d ' '
}

# Function to get total plays count
get_total_plays() {
    PGPASSWORD=korn5676 psql -h 192.168.1.23 -U postgres -d football_tracker -t -c "
        SELECT COUNT(*) FROM plays
    " | tr -d ' '
}

# Initial status
TOTAL_GAMES=465
REMAINING=$(get_remaining_games)
PROCESSED=$((TOTAL_GAMES - REMAINING))
TOTAL_PLAYS=$(get_total_plays)

echo "📊 Initial Status:"
echo "   Total games: $TOTAL_GAMES"
echo "   Already processed: $PROCESSED" 
echo "   Remaining to process: $REMAINING"
echo "   Total plays extracted: $TOTAL_PLAYS"
echo ""

if [ "$REMAINING" -eq 0 ]; then
    echo "🎉 All games already processed!"
    exit 0
fi

echo "🚀 Starting batch processing..."
echo "⏱️  Processing one game every ~10 seconds"
echo ""

# Process games in batches
BATCH_SIZE=20
GAMES_PROCESSED_THIS_RUN=0
START_TIME=$(date +%s)

while [ "$REMAINING" -gt 0 ] && [ "$GAMES_PROCESSED_THIS_RUN" -lt "$BATCH_SIZE" ]; do
    echo "🎯 Processing game $(($GAMES_PROCESSED_THIS_RUN + 1))/$BATCH_SIZE in this batch..."
    
    # Run the simple scraper
    python3 simple_playbyplay_scraper.py
    
    # Check if it was successful
    if [ $? -eq 0 ]; then
        GAMES_PROCESSED_THIS_RUN=$((GAMES_PROCESSED_THIS_RUN + 1))
        
        # Update counts
        REMAINING=$(get_remaining_games)
        PROCESSED=$((TOTAL_GAMES - REMAINING))
        TOTAL_PLAYS=$(get_total_plays)
        
        echo "   📈 Progress: $PROCESSED/$TOTAL_GAMES games processed ($TOTAL_PLAYS total plays)"
        echo ""
        
        # Brief pause between games
        sleep 2
    else
        echo "   ❌ Game processing failed, continuing..."
        break
    fi
done

# Final status for this batch
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
REMAINING=$(get_remaining_games)
PROCESSED=$((TOTAL_GAMES - REMAINING))
TOTAL_PLAYS=$(get_total_plays)

echo "🏁 Batch Complete!"
echo "=================="
echo "⏱️  Batch time: ${ELAPSED}s"
echo "✅ Games processed this run: $GAMES_PROCESSED_THIS_RUN"
echo "📊 Overall progress: $PROCESSED/$TOTAL_GAMES games ($((PROCESSED * 100 / TOTAL_GAMES))%)"
echo "💾 Total plays in database: $TOTAL_PLAYS"

if [ "$REMAINING" -gt 0 ]; then
    echo "🔄 $REMAINING games remaining - run script again to continue"
    echo "💡 Estimated time remaining: ~$((REMAINING * 10 / 60)) minutes"
else
    echo "🎉 ALL GAMES COMPLETED!"
    echo "🗃️  Database now contains play-by-play data for all $TOTAL_GAMES games"
fi