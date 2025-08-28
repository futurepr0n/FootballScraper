#!/bin/bash
#
# Enhanced Football Analytics Automation
# NFL game-day-aware processing: Run the day after NFL games when data is complete
#
# This script ensures proper NFL data processing sequence:
# 1. FootballScraper creates local CSV files for specified week
# 2. FootballTracker processes local CSV files → JSON
# 3. CSV files archived to centralized FootballData backups
# 4. Local CSV files cleaned up
#
# Usage:
#   ./enhanced_football_automation.sh                      # Process current NFL week
#   ./enhanced_football_automation.sh --week 3 --preseason # Process preseason week 3
#   ./enhanced_football_automation.sh --week 5             # Process regular season week 5
#   ./enhanced_football_automation.sh --test               # Test run (no cleanup)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Helper functions
log_info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_success() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_nfl() { echo -e "${PURPLE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} 🏈 $1"; }

# Configuration
IS_PRODUCTION=${NODE_ENV:-development}
TEST_MODE=false
TARGET_WEEK=""
TARGET_SEASON="2025"
IS_PRESEASON=false
FAST_MODE=${FAST_SCRAPE:-false}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --week=*)
            TARGET_WEEK="${1#*=}"
            shift
            ;;
        --week)
            TARGET_WEEK="$2"
            shift 2
            ;;
        --season=*)
            TARGET_SEASON="${1#*=}"
            shift
            ;;
        --season)
            TARGET_SEASON="$2"
            shift 2
            ;;
        --preseason)
            IS_PRESEASON=true
            shift
            ;;
        --test)
            TEST_MODE=true
            shift
            ;;
        --help)
            echo "Enhanced Football Analytics Automation"
            echo "Usage: $0 [--week N] [--season YYYY] [--preseason] [--test] [--help]"
            echo ""
            echo "Options:"
            echo "  --week N        Process specific NFL week (1-18 regular, 1-3 preseason)"
            echo "  --season YYYY   Process specific season (default: 2025)"
            echo "  --preseason     Process preseason week instead of regular season"
            echo "  --test          Test run - no cleanup or production changes"
            echo "  --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                        # Process current NFL week"
            echo "  $0 --week 3 --preseason  # Process preseason week 3"
            echo "  $0 --week 10             # Process regular season week 10"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# NFL Week Detection Function
detect_current_nfl_week() {
    local current_date=$(date +%Y-%m-%d)
    local current_month=$(date +%m)
    local current_day=$(date +%d)
    
    # NFL Season Calendar (approximate)
    # Preseason: August 10-30
    # Regular Season Week 1: Early September
    # Regular Season Week 18: Early January
    # Playoffs: January - February
    
    if [[ "$current_month" == "08" ]]; then
        # August - Preseason
        if [[ "$current_day" -ge 10 ]]; then
            IS_PRESEASON=true
            if [[ "$current_day" -le 17 ]]; then
                TARGET_WEEK="1"
            elif [[ "$current_day" -le 24 ]]; then
                TARGET_WEEK="2"
            else
                TARGET_WEEK="3"
            fi
        fi
    elif [[ "$current_month" == "09" ]]; then
        # September - Regular Season starts
        if [[ "$current_day" -le 15 ]]; then
            TARGET_WEEK="1"
        elif [[ "$current_day" -le 22 ]]; then
            TARGET_WEEK="2"
        else
            TARGET_WEEK="3"
        fi
    elif [[ "$current_month" == "10" ]]; then
        # October
        TARGET_WEEK=$(( (current_day - 1) / 7 + 4 ))
    elif [[ "$current_month" == "11" ]]; then
        # November
        TARGET_WEEK=$(( (current_day - 1) / 7 + 8 ))
    elif [[ "$current_month" == "12" ]]; then
        # December
        TARGET_WEEK=$(( (current_day - 1) / 7 + 12 ))
    elif [[ "$current_month" == "01" ]]; then
        # January - Regular season end + Playoffs
        if [[ "$current_day" -le 7 ]]; then
            TARGET_WEEK="18"
        else
            TARGET_WEEK="playoffs"
        fi
    else
        # Default to week 1 if uncertain
        TARGET_WEEK="1"
    fi
    
    log_nfl "Auto-detected NFL week: $TARGET_WEEK (preseason: $IS_PRESEASON)"
}

# If no week specified, auto-detect
if [[ -z "$TARGET_WEEK" ]]; then
    detect_current_nfl_week
fi

# Environment detection
if [[ "$IS_PRODUCTION" == "production" ]] || [[ -d "/app" ]]; then
    BASE_DIR="/app"
    log_info "🌐 Production environment detected"
else
    BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
    log_info "💻 Development environment detected"
fi

SCRAPER_DIR="$BASE_DIR/FootballScraper"
TRACKER_DIR="$BASE_DIR/FootballTracker"
FOOTBALL_DATA_DIR="$BASE_DIR/FootballData"
LOGS_DIR="$BASE_DIR/logs"

# Ensure directories exist
mkdir -p "$LOGS_DIR"

log_nfl "Enhanced Football Analytics Automation"
log_info "========================================"
log_info "Started at: $(date)"
log_info ""
log_info "📋 Environment Configuration:"
log_info "   Base Directory: $BASE_DIR"
log_info "   Scraper: $SCRAPER_DIR"
log_info "   Tracker: $TRACKER_DIR"
log_info "   Football Data: $FOOTBALL_DATA_DIR"
log_info "   Logs: $LOGS_DIR"
log_info "   Test Mode: $TEST_MODE"
log_info "   Target Week: $TARGET_WEEK"
log_info "   Target Season: $TARGET_SEASON"
log_info "   Preseason: $IS_PRESEASON"
log_info ""

# Verify directories
if [[ ! -d "$SCRAPER_DIR" ]]; then
    log_error "FootballScraper directory not found: $SCRAPER_DIR"
    exit 1
fi

if [[ ! -d "$TRACKER_DIR" ]]; then
    log_error "FootballTracker directory not found: $TRACKER_DIR"
    exit 1
fi

if [[ ! -d "$FOOTBALL_DATA_DIR" ]]; then
    log_error "FootballData directory not found: $FOOTBALL_DATA_DIR"
    exit 1
fi

# Function to run command with proper error handling and logging
run_step() {
    local step_name="$1"
    local work_dir="$2"
    local command="$3"
    local log_file="$4"
    
    log_info "🔄 $step_name"
    log_info "   📁 Working directory: $work_dir"
    log_info "   🔧 Command: $command"
    
    if [[ "$TEST_MODE" == true ]]; then
        log_warning "   🧪 TEST MODE: Would execute: $command"
        log_success "   ✅ $step_name completed (test mode)"
        return 0
    fi
    
    cd "$work_dir"
    
    if [[ -n "$log_file" ]]; then
        if eval "$command" >> "$log_file" 2>&1; then
            log_success "   ✅ $step_name completed successfully"
            return 0
        else
            local exit_code=$?
            log_error "   ❌ $step_name failed with exit code $exit_code"
            log_error "   📄 Check log file: $log_file"
            return $exit_code
        fi
    else
        if eval "$command"; then
            log_success "   ✅ $step_name completed successfully"
            return 0
        else
            local exit_code=$?
            log_error "   ❌ $step_name failed with exit code $exit_code"
            return $exit_code
        fi
    fi
}

# Function to count CSV files in directory
count_csv_files() {
    local dir="$1"
    ls "$dir"/*.csv 2>/dev/null | wc -l
}

# Function to archive and cleanup CSV files for NFL
archive_and_cleanup_nfl_csv() {
    local scraper_dir="$1"
    local week_identifier="$2"
    
    cd "$scraper_dir"
    
    log_info "📦 Archiving NFL CSV files to centralized backup"
    
    # Use Python to archive CSV files to FootballData/CSV_BACKUPS/
    if python3 -c "
import os
import shutil
from pathlib import Path
import glob

scraper_dir = Path('$scraper_dir')
backup_dir = Path('$FOOTBALL_DATA_DIR/CSV_BACKUPS')
backup_dir.mkdir(exist_ok=True)

csv_files = list(scraper_dir.glob('*.csv'))
if csv_files:
    for csv_file in csv_files:
        dest_file = backup_dir / csv_file.name
        shutil.copy2(csv_file, dest_file)
        print(f'Archived: {csv_file.name} -> {dest_file}')
    print(f'✅ Archived {len(csv_files)} CSV files to centralized backup')
else:
    print('ℹ️ No CSV files found to archive')
"; then
        log_success "✅ NFL CSV files archived to centralized backup"
        
        if [[ "$TEST_MODE" != true ]]; then
            log_info "🧹 Cleaning up local CSV files"
            local csv_count=$(count_csv_files ".")
            if [[ $csv_count -gt 0 ]]; then
                rm -f *.csv
                log_success "✅ Cleaned up $csv_count local CSV files"
            else
                log_info "ℹ️ No CSV files to clean up"
            fi
        else
            log_warning "🧪 TEST MODE: Skipped CSV cleanup"
        fi
        return 0
    else
        log_error "❌ CSV archival failed - keeping local files for manual review"
        return 1
    fi
}

# Main execution
main() {
    local overall_start_time=$(date +%s)
    
    log_nfl "🏈 STEP 1: NFL Week Detection & Validation"
    log_info "=========================================="
    
    # Validate week parameters
    if [[ "$IS_PRESEASON" == true ]]; then
        if [[ "$TARGET_WEEK" -lt 1 || "$TARGET_WEEK" -gt 3 ]]; then
            log_error "Invalid preseason week: $TARGET_WEEK (must be 1-3)"
            exit 1
        fi
        log_nfl "🏈 Processing Preseason Week $TARGET_WEEK"
    else
        if [[ "$TARGET_WEEK" != "playoffs" ]] && [[ "$TARGET_WEEK" -lt 1 || "$TARGET_WEEK" -gt 18 ]]; then
            log_error "Invalid regular season week: $TARGET_WEEK (must be 1-18 or 'playoffs')"
            exit 1
        fi
        log_nfl "🏈 Processing Regular Season Week $TARGET_WEEK"
    fi
    
    log_nfl "🏈 STEP 2: NFL Data Scraping"
    log_info "============================"
    
    # Prepare scraper command with venv activation
    local scraper_command=""
    local week_args=""
    
    # Build week arguments
    if [[ "$IS_PRESEASON" == true ]]; then
        week_args="--preseason --week $TARGET_WEEK --season $TARGET_SEASON"
    else
        week_args="--week $TARGET_WEEK --season $TARGET_SEASON"
    fi
    
    # Add fast flag if enabled
    local fast_flag=""
    if [[ "$FAST_MODE" == "true" ]]; then
        fast_flag="--fast"
        log_info "   ⚡ Fast mode enabled for development"
    fi
    
    # Check if venv exists and set up proper command
    cd "$SCRAPER_DIR"
    
    if [[ -d "venv" ]]; then
        log_info "   🐍 Using virtual environment"
        scraper_command="source venv/bin/activate && python enhanced_nfl_scraper.py $week_args $fast_flag"
    else
        log_warning "   ⚠️ No venv found, using system Python"
        scraper_command="python3 enhanced_nfl_scraper.py $week_args $fast_flag"
    fi
    
    # Count existing CSV files before scraping
    local pre_scrape_csv_count=$(count_csv_files "$SCRAPER_DIR")
    log_info "   📄 CSV files before scraping: $pre_scrape_csv_count"
    
    # Run the enhanced NFL scraper
    if ! run_step "Running enhanced NFL scrape" "$SCRAPER_DIR" "$scraper_command" "$LOGS_DIR/nfl_scraper.log"; then
        log_error "❌ NFL scraping failed - aborting automation"
        exit 1
    fi
    
    # Run play-by-play scraper for detailed game data
    log_nfl "🏈 Running NFL play-by-play scraper for detailed game data"
    cd "$SCRAPER_DIR"
    
    local playbyplay_command=""
    if [[ -d "venv" ]]; then
        playbyplay_command="source venv/bin/activate && python nfl_playbyplay_scraper.py $week_args"
    else
        playbyplay_command="python3 nfl_playbyplay_scraper.py $week_args"
    fi
    
    if ! run_step "Running NFL play-by-play scraper" "$SCRAPER_DIR" "$playbyplay_command" "$LOGS_DIR/nfl_playbyplay.log"; then
        log_warning "⚠️ NFL play-by-play scrape failed, continuing anyway"
    fi
    
    # Count CSV files after scraping
    local post_scrape_csv_count=$(count_csv_files "$SCRAPER_DIR")
    local new_csv_count=$((post_scrape_csv_count - pre_scrape_csv_count))
    log_info "   📄 New CSV files created: $new_csv_count"
    
    if [[ $new_csv_count -eq 0 ]]; then
        log_warning "⚠️ No new CSV files created - check scraper results"
        log_info "📋 This may be due to no games scheduled for week $TARGET_WEEK"
    fi
    
    log_nfl "🏈 STEP 3: CSV Archival (Pre-Processing)"
    log_info "======================================="
    
    # Archive CSV files to centralized backup BEFORE processing
    if [[ $new_csv_count -gt 0 ]]; then
        local week_identifier="week_${TARGET_WEEK}_${TARGET_SEASON}"
        if [[ "$IS_PRESEASON" == true ]]; then
            week_identifier="preseason_${week_identifier}"
        fi
        
        log_info "   📅 Week identifier: $week_identifier"
        
        if ! archive_and_cleanup_nfl_csv "$SCRAPER_DIR" "$week_identifier"; then
            log_error "❌ CSV archival failed - processing will fail"
            exit 1
        fi
    else
        log_info "ℹ️ No new CSV files to archive"
    fi
    
    log_nfl "🏈 STEP 4: NFL Data Processing"
    log_info "=============================="
    
    # Process the NFL data (converts CSV → JSON from centralized backup)
    cd "$SCRAPER_DIR"
    local processing_command=""
    
    if [[ -d "venv" ]]; then
        processing_command="source venv/bin/activate && python nfl_data_processor.py --week $TARGET_WEEK --season $TARGET_SEASON"
        if [[ "$IS_PRESEASON" == true ]]; then
            processing_command="$processing_command --preseason"
        fi
    else
        processing_command="python3 nfl_data_processor.py --week $TARGET_WEEK --season $TARGET_SEASON"
        if [[ "$IS_PRESEASON" == true ]]; then
            processing_command="$processing_command --preseason"
        fi
    fi
    
    if ! run_step "Processing NFL statistics" "$SCRAPER_DIR" "$processing_command" "$LOGS_DIR/nfl_process.log"; then
        log_error "❌ NFL data processing failed"
        log_warning "⚠️ CSV files remain in FootballScraper and CSV_BACKUPS for manual review"
        exit 1
    fi
    
    log_nfl "🏈 STEP 5: Data Integrity and Validation"
    log_info "========================================"
    
    # Run data validation specific to NFL
    cd "$SCRAPER_DIR"
    local validation_command=""
    
    if [[ -d "venv" ]]; then
        validation_command="source venv/bin/activate && python nfl_data_aggregator.py --week $TARGET_WEEK --season $TARGET_SEASON"
    else
        validation_command="python3 nfl_data_aggregator.py --week $TARGET_WEEK --season $TARGET_SEASON"
    fi
    
    run_step "Running NFL data validation" "$SCRAPER_DIR" "$validation_command" "$LOGS_DIR/nfl_validation.log"
    
    log_nfl "🏈 STEP 6: Generate TD Predictions & Analysis"
    log_info "============================================="
    
    # Generate NFL-specific analysis files
    cd "$SCRAPER_DIR"
    
    # Generate rolling stats for NFL
    if [[ -d "venv" ]]; then
        run_step "Generating NFL rolling stats" "$SCRAPER_DIR" "source venv/bin/activate && python nfl_rolling_stats_generator.py --week $TARGET_WEEK" "$LOGS_DIR/nfl_rolling_stats.log"
        
        # Generate roster data
        run_step "Generating NFL roster data" "$SCRAPER_DIR" "source venv/bin/activate && python nfl_roster_generator.py" "$LOGS_DIR/nfl_rosters.log"
        
        # Generate dashboard aggregation
        run_step "Generating NFL dashboard aggregation" "$SCRAPER_DIR" "source venv/bin/activate && python nfl_dashboard_aggregator.py --week $TARGET_WEEK" "$LOGS_DIR/nfl_dashboard.log"
    else
        run_step "Generating NFL rolling stats" "$SCRAPER_DIR" "python3 nfl_rolling_stats_generator.py --week $TARGET_WEEK" "$LOGS_DIR/nfl_rolling_stats.log"
        run_step "Generating NFL roster data" "$SCRAPER_DIR" "python3 nfl_roster_generator.py" "$LOGS_DIR/nfl_rosters.log"
        run_step "Generating NFL dashboard aggregation" "$SCRAPER_DIR" "python3 nfl_dashboard_aggregator.py --week $TARGET_WEEK" "$LOGS_DIR/nfl_dashboard.log"
    fi
    
    log_nfl "🏈 STEP 7: FootballAPI Integration"
    log_info "=================================="
    
    # Check if FootballAPI is running and restart if needed
    log_info "🔄 Checking FootballAPI status"
    if pgrep -f "FootballAPI.*enhanced_main.py" > /dev/null 2>&1; then
        log_info "🔄 Restarting FootballAPI for data refresh"
        pkill -f "FootballAPI.*enhanced_main.py" || true
        sleep 3
    fi
    
    # Start FootballAPI if not running (background process)
    cd "$BASE_DIR/FootballAPI"
    if [[ -d "venv" ]]; then
        nohup ./venv/bin/python enhanced_main.py > "$LOGS_DIR/football_api.log" 2>&1 &
    else
        nohup python3 enhanced_main.py > "$LOGS_DIR/football_api.log" 2>&1 &
    fi
    
    sleep 5  # Give API time to start
    
    # Verify API is responding
    if curl -s http://localhost:9000/health > /dev/null 2>&1; then
        log_success "✅ FootballAPI started successfully on port 9000"
    else
        log_warning "⚠️ FootballAPI may not be responding properly"
    fi
    
    log_nfl "🏈 STEP 8: FootballTracker Build & Deploy"
    log_info "========================================"
    
    # Kill existing FootballTracker server processes
    log_info "🔄 Stopping existing FootballTracker server processes"
    if pgrep -f "serve.*4000" > /dev/null 2>&1; then
        if run_step "Killing existing FootballTracker server" "$TRACKER_DIR" "pkill -f 'serve.*4000'" ""; then
            sleep 3
            log_success "⏰ Waited 3 seconds for clean shutdown"
        fi
    else
        log_info "ℹ️ No existing FootballTracker server processes found"
    fi
    
    # Build production FootballTracker and start server
    log_info "🚀 Building FootballTracker production application"
    if run_step "Building FootballTracker production" "$TRACKER_DIR" "npm run build-production" "$LOGS_DIR/football_production_build.log"; then
        log_success "✅ FootballTracker production build completed successfully"
        
        # Start server in background on port 4000
        log_info "🌐 Starting FootballTracker server on port 4000"
        cd "$TRACKER_DIR"
        nohup serve -s build -l 4000 > "$LOGS_DIR/football_server.log" 2>&1 &
        sleep 3
        
        # Check if server started successfully
        if pgrep -f "serve.*4000" > /dev/null 2>&1; then
            server_pid=$(pgrep -f "serve.*4000")
            log_success "✅ FootballTracker server started successfully (PID: $server_pid)"
            log_info "🌐 Server accessible at http://localhost:4000"
        else
            log_warning "⚠️ FootballTracker server may not have started properly"
        fi
    else
        log_error "❌ FootballTracker production build failed"
        
        # Fallback to regular build
        log_warning "🔄 Attempting fallback build process"
        if run_step "Building FootballTracker (fallback)" "$TRACKER_DIR" "npm run build" "$LOGS_DIR/football_fallback_build.log"; then
            log_info "🌐 Starting FootballTracker server manually"
            nohup serve -s build -l 4000 > "$LOGS_DIR/football_server.log" 2>&1 &
            sleep 2
            if pgrep -f "serve.*4000" > /dev/null 2>&1; then
                server_pid=$(pgrep -f "serve.*4000")
                log_success "✅ FootballTracker fallback server started (PID: $server_pid)"
            fi
        fi
    fi
    
    log_nfl "🏈 STEP 9: Final Cleanup"
    log_info "======================"
    
    # Clean up local CSV files from FootballScraper (already archived)
    if [[ $new_csv_count -gt 0 && "$TEST_MODE" != true ]]; then
        cd "$SCRAPER_DIR"
        local csv_count=$(count_csv_files ".")
        if [[ $csv_count -gt 0 ]]; then
            log_info "🧹 Cleaning up local CSV files (already archived to centralized backup)"
            rm -f *.csv
            log_success "✅ Cleaned up local CSV files"
        else
            log_info "ℹ️ No local CSV files to clean up"
        fi
    else
        if [[ "$TEST_MODE" == true ]]; then
            log_warning "🧪 TEST MODE: Skipped CSV cleanup - files remain in FootballScraper"
        else
            log_info "ℹ️ No new CSV files generated - no cleanup needed"
        fi
    fi
    
    # Calculate execution time
    local overall_end_time=$(date +%s)
    local execution_time=$((overall_end_time - overall_start_time))
    local minutes=$((execution_time / 60))
    local seconds=$((execution_time % 60))
    
    log_info ""
    log_success "🏈 Enhanced Football Automation Complete!"
    log_info "========================================"
    log_info "📊 Summary:"
    log_info "   ⏱️ Execution time: ${minutes}m ${seconds}s"
    log_info "   📄 New CSV files processed: $new_csv_count"
    log_info "   🏈 Week processed: $TARGET_WEEK ($([[ "$IS_PRESEASON" == true ]] && echo "preseason" || echo "regular season"))"
    log_info "   📁 Logs directory: $LOGS_DIR"
    log_info "   🧪 Test mode: $TEST_MODE"
    log_info ""
    
    # Check if key files were generated
    log_info "📄 Checking generated files:"
    
    FOOTBALL_DATA_PATH="$FOOTBALL_DATA_DIR/data"
    
    # Check for weekly data files
    local week_file=""
    if [[ "$IS_PRESEASON" == true ]]; then
        week_file="$FOOTBALL_DATA_PATH/preseason/week_${TARGET_WEEK}_${TARGET_SEASON}.json"
    else
        week_file="$FOOTBALL_DATA_PATH/weekly/week_${TARGET_WEEK}_${TARGET_SEASON}.json"
    fi
    
    # Key prediction files to check
    prediction_files=(
        "$week_file"
        "$FOOTBALL_DATA_PATH/stats/player_stats.json"
        "$FOOTBALL_DATA_PATH/stats/team_stats.json"
        "$FOOTBALL_DATA_PATH/rolling_stats/rolling_stats_latest.json"
        "$FOOTBALL_DATA_PATH/predictions/td_predictions_latest.json"
    )
    
    for file in "${prediction_files[@]}"; do
        if [[ -f "$file" ]]; then
            # Check file age
            if [[ "$OSTYPE" == "darwin"* ]]; then
                file_age=$(stat -f %m "$file" 2>/dev/null)
            else
                file_age=$(stat -c %Y "$file" 2>/dev/null)
            fi
            
            if [[ -n "$file_age" ]]; then
                current_time=$(date +%s)
                age_minutes=$(( (current_time - file_age) / 60 ))
                
                if [[ $age_minutes -lt 60 ]]; then
                    log_success "   ✅ $(basename "$file") (updated ${age_minutes}m ago)"
                else
                    log_warning "   ⚠️ $(basename "$file") (updated ${age_minutes}m ago - may be stale)"
                fi
            else
                log_success "   ✅ $(basename "$file") (exists)"
            fi
        else
            log_error "   ❌ $(basename "$file") - not found"
        fi
    done
    
    # Check if servers are running
    if pgrep -f "FootballAPI.*enhanced_main.py" > /dev/null 2>&1; then
        api_pid=$(pgrep -f "FootballAPI.*enhanced_main.py")
        log_success "   ✅ FootballAPI running (PID: $api_pid) on port 9000"
    else
        log_error "   ❌ FootballAPI not running"
    fi
    
    if pgrep -f "serve.*4000" > /dev/null 2>&1; then
        server_pid=$(pgrep -f "serve.*4000")
        log_success "   ✅ FootballTracker server running (PID: $server_pid) on port 4000"
    else
        log_error "   ❌ FootballTracker server not running"
    fi
    
    log_info ""
    log_info "📋 Next Steps:"
    log_info "   1. Check FootballTracker frontend for updated data: http://localhost:4000"
    log_info "   2. Verify FootballAPI health: curl http://localhost:9000/health"
    log_info "   3. Review logs if any warnings occurred: $LOGS_DIR"
    log_info "   4. Verify week $TARGET_WEEK data in FootballData/data/"
    
    return 0
}

# Execute main function
main "$@"