#!/bin/bash

# NFL Preseason Data Backfill Script
# Comprehensive data collection and processing for August 2025 preseason games

set -e  # Exit on any error

echo "🏈 NFL Preseason Data Backfill System"
echo "====================================="
echo ""

# Function to check if required files exist
check_requirements() {
    echo "📋 Checking requirements..."
    
    # Check for schedule files
    schedule_count=$(find . -name "august_*_2025.json" | wc -l)
    echo "   • Found $schedule_count August schedule files"
    
    if [ $schedule_count -eq 0 ]; then
        echo "❌ No August schedule files found"
        echo "   Run nfl_schedule_generator.py first to create schedule files"
        exit 1
    fi
    
    # Check for required Python files
    required_files=(
        "nfl_preseason_backfill.py"
        "nfl_preseason_aggregator.py"
        "nfl_playbyplay_scraper.py"
    )
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            echo "❌ Missing required file: $file"
            exit 1
        fi
        echo "   ✅ Found $file"
    done
    
    # Check Python environment
    if ! python3 -c "import requests, json, pathlib" 2>/dev/null; then
        echo "❌ Missing required Python packages"
        echo "   Install with: pip3 install requests"
        exit 1
    fi
    echo "   ✅ Python environment OK"
    
    # Create logs directory
    mkdir -p logs
    echo "   ✅ Logs directory ready"
    
    # Check FootballData directory
    if [ ! -d "../FootballData" ]; then
        echo "⚠️  Creating FootballData directory structure..."
        mkdir -p ../FootballData/data/preseason
        mkdir -p ../FootballData/logs
    fi
    echo "   ✅ FootballData directory ready"
    
    echo ""
}

# Function to show current progress
show_progress() {
    echo "📊 Current Progress:"
    
    if [ -f "../FootballData/preseason_backfill_progress.json" ]; then
        completed=$(python3 -c "
import json
try:
    with open('../FootballData/preseason_backfill_progress.json', 'r') as f:
        progress = json.load(f)
    print(f'   • Completed games: {len(progress.get(\"completed_games\", {}))}')
    print(f'   • Failed games: {len(progress.get(\"failed_games\", {}))}')
    print(f'   • Total processed: {progress.get(\"total_processed\", 0)}')
    if progress.get('last_updated'):
        print(f'   • Last updated: {progress[\"last_updated\"][:19]}')
except:
    print('   • No previous progress found')
")
        echo "$completed"
    else
        echo "   • No previous progress found"
    fi
    echo ""
}

# Function to estimate time and data
show_estimates() {
    game_count=$(find . -name "august_*_2025.json" -exec grep -l "preseason" {} \; | \
                 xargs -I {} python3 -c "
import json, sys
total = 0
for file in sys.argv[1:]:
    try:
        with open(file, 'r') as f:
            data = json.load(f)
        total += len([g for g in data.get('games', []) if g.get('season_type_name') == 'preseason'])
    except:
        pass
print(total)
" {})
    
    echo "📈 Processing Estimates:"
    echo "   • Total preseason games: $game_count"
    echo "   • Estimated time: $(($game_count * 2 / 60)) minutes (2 sec/game + delays)"
    echo "   • Estimated data size: $(($game_count * 500))KB raw data"
    echo "   • Rate limit: 1.5 seconds between requests"
    echo ""
}

# Function to run the backfill
run_backfill() {
    echo "🚀 Starting NFL Preseason Data Backfill..."
    echo ""
    
    # Log file for this run
    log_file="logs/preseason_backfill_$(date +%Y%m%d_%H%M%S).log"
    
    echo "📝 Logging to: $log_file"
    echo ""
    
    # Run the backfill with progress output
    if python3 nfl_preseason_backfill.py 2>&1 | tee "$log_file"; then
        echo ""
        echo "✅ Backfill phase completed successfully!"
        return 0
    else
        echo ""
        echo "❌ Backfill phase failed - check $log_file for details"
        return 1
    fi
}

# Function to run aggregation
run_aggregation() {
    echo ""
    echo "📊 Starting Data Aggregation..."
    echo ""
    
    # Log file for aggregation
    log_file="logs/preseason_aggregation_$(date +%Y%m%d_%H%M%S).log"
    
    echo "📝 Logging to: $log_file"
    echo ""
    
    # Run the aggregator
    if python3 nfl_preseason_aggregator.py 2>&1 | tee "$log_file"; then
        echo ""
        echo "✅ Aggregation phase completed successfully!"
        return 0
    else
        echo ""
        echo "❌ Aggregation phase failed - check $log_file for details"
        return 1
    fi
}

# Function to show final summary
show_summary() {
    echo ""
    echo "🎉 NFL Preseason Backfill Complete!"
    echo "==================================="
    
    # Count final files
    if [ -d "../FootballData/data/preseason" ]; then
        complete_files=$(find ../FootballData/data/preseason -name "*_complete.json" | wc -l)
        box_score_files=$(find ../FootballData/data/preseason -name "*_boxscore.json" | wc -l)
        pbp_files=$(find ../FootballData/data/preseason -name "*_plays.json" | wc -l)
        
        echo ""
        echo "📁 Generated Files:"
        echo "   • Complete game files: $complete_files"
        echo "   • Box score files: $box_score_files"
        echo "   • Play-by-play files: $pbp_files"
        
        if [ -d "../FootballData/data/preseason/aggregated" ]; then
            agg_files=$(find ../FootballData/data/preseason/aggregated -name "*.json" | wc -l)
            echo "   • Aggregated files: $agg_files"
        fi
        
        echo ""
        echo "💾 Data Location: ../FootballData/data/preseason/"
        echo ""
        
        # Show key aggregated files
        if [ -f "../FootballData/data/preseason/aggregated/dashboard_summary.json" ]; then
            echo "🎯 Dashboard-Ready Files:"
            echo "   • dashboard_summary.json - Main dashboard data"
            echo "   • touchdown_leaders.json - TD leaderboard"
            echo "   • interception_data.json - INT statistics"
            echo "   • position_leaders.json - Leaders by position"
            echo "   • team_summaries.json - Team records & stats"
            echo ""
        fi
        
        echo "🚀 Next Steps:"
        echo "   • Data is ready for dashboard components"
        echo "   • Use dashboard_summary.json for quick integration"
        echo "   • Check individual position files for detailed stats"
        echo "   • Review logs/ directory for processing details"
        
    else
        echo "⚠️  Data directory not found - check for errors"
    fi
    
    echo ""
}

# Main execution
main() {
    # Start timing
    start_time=$(date +%s)
    
    # Check requirements
    check_requirements
    
    # Show current state
    show_progress
    show_estimates
    
    # Interactive confirmation
    echo "🤔 Ready to proceed?"
    echo "   This will collect comprehensive data for ALL August 2025 preseason games"
    echo "   including box scores, play-by-play, and statistical summaries."
    echo ""
    read -p "Continue? (y/N): " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Operation cancelled"
        exit 0
    fi
    
    echo ""
    echo "⏳ Starting comprehensive backfill process..."
    echo ""
    
    # Phase 1: Data Collection
    if ! run_backfill; then
        echo "💔 Backfill failed - stopping here"
        echo "   Check logs for error details"
        echo "   You can resume later - progress has been saved"
        exit 1
    fi
    
    # Phase 2: Data Aggregation
    echo ""
    echo "🔄 Moving to aggregation phase..."
    sleep 2
    
    if ! run_aggregation; then
        echo "💔 Aggregation failed"
        echo "   Raw data collection was successful"
        echo "   You can retry aggregation later"
        exit 1
    fi
    
    # Calculate total time
    end_time=$(date +%s)
    total_time=$(($end_time - $start_time))
    minutes=$(($total_time / 60))
    seconds=$(($total_time % 60))
    
    echo ""
    echo "⏱️  Total processing time: ${minutes}m ${seconds}s"
    
    # Show final summary
    show_summary
}

# Handle interruption gracefully
trap 'echo ""; echo "⏹️  Process interrupted"; echo "Progress has been saved and can be resumed"; exit 130' INT

# Run main function
main "$@"