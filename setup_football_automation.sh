#!/bin/bash
# setup_football_automation.sh - Complete FootballScraper Environment Setup
# Creates virtual environment, installs dependencies, and prepares automation

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Logging functions
log_info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_success() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"; }
log_nfl() { echo -e "${PURPLE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} 🏈 $1"; }

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FOOTBALL_DATA_DIR="$BASE_DIR/FootballData"
VENV_DIR="$SCRIPT_DIR/venv"

log_nfl "Football Automation Environment Setup"
log_info "================================================"
log_info "Base Directory: $BASE_DIR"
log_info "FootballScraper: $SCRIPT_DIR"
log_info "FootballData: $FOOTBALL_DATA_DIR"
log_info ""

# Step 1: Create FootballData directory structure
log_nfl "🏈 STEP 1: Creating FootballData Directory Structure"
log_info "=================================================="

if [ ! -d "$FOOTBALL_DATA_DIR" ]; then
    log_info "📁 Creating FootballData directory"
    mkdir -p "$FOOTBALL_DATA_DIR"
fi

# Create subdirectories
FOOTBALL_DIRS=(
    "data"
    "data/2025"
    "data/2025/week_01"
    "data/2025/week_02"
    "data/2025/week_03"
    "data/2025/week_04"
    "data/predictions"
    "data/stats"
    "data/rolling_stats"
    "data/team_stats"
    "data/odds"
    "data/lineups"
    "data/hellraiser"
    "data/injuries"
    "data/handedness"
    "data/stadium"
    "data/multi_td_stats"
    "SCANNED"
    "CSV_BACKUPS"
)

for dir in "${FOOTBALL_DIRS[@]}"; do
    target_dir="$FOOTBALL_DATA_DIR/$dir"
    if [ ! -d "$target_dir" ]; then
        log_info "📁 Creating: $target_dir"
        mkdir -p "$target_dir"
    fi
done

log_success "✅ FootballData directory structure created"

# Step 2: Create Python virtual environment
log_nfl "🏈 STEP 2: Setting Up Python Virtual Environment"
log_info "=============================================="

if [ ! -d "$VENV_DIR" ]; then
    log_info "🐍 Creating virtual environment"
    python3 -m venv "$VENV_DIR"
    log_success "✅ Virtual environment created"
else
    log_info "🐍 Virtual environment already exists"
fi

# Activate virtual environment and install packages
log_info "📦 Installing Python dependencies"
source "$VENV_DIR/bin/activate"

# Core dependencies
pip install --upgrade pip
pip install requests beautifulsoup4 pandas numpy
pip install selenium webdriver-manager  # For advanced scraping if needed
pip install fastapi uvicorn  # For FootballAPI integration
pip install python-dateutil pytz

log_success "✅ Python dependencies installed"

# Step 3: Create required configuration files
log_nfl "🏈 STEP 3: Creating Configuration Files"
log_info "======================================"

# Create logs directory
if [ ! -d "$SCRIPT_DIR/logs" ]; then
    mkdir -p "$SCRIPT_DIR/logs"
    log_info "📁 Created logs directory"
fi

# Create .env file for environment configuration
ENV_FILE="$SCRIPT_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    log_info "📄 Creating .env configuration file"
    cat > "$ENV_FILE" << EOF
# FootballScraper Environment Configuration
NODE_ENV=development
FOOTBALL_DATA_PATH=$FOOTBALL_DATA_DIR/data
FOOTBALL_API_PORT=9000
FOOTBALL_TRACKER_PORT=4000
ESPN_DELAY_SECONDS=2
SCRAPE_TIMEOUT_SECONDS=30
EOF
    log_success "✅ Created .env file"
else
    log_info "📄 .env file already exists"
fi

# Create requirements.txt for future use
REQUIREMENTS_FILE="$SCRIPT_DIR/requirements.txt"
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    log_info "📄 Creating requirements.txt"
    cat > "$REQUIREMENTS_FILE" << EOF
requests>=2.25.1
beautifulsoup4>=4.10.0
pandas>=1.3.0
numpy>=1.21.0
selenium>=4.0.0
webdriver-manager>=3.8.0
fastapi>=0.70.0
uvicorn>=0.15.0
python-dateutil>=2.8.2
pytz>=2021.3
lxml>=4.6.0
EOF
    log_success "✅ Created requirements.txt"
fi

# Step 4: Make scripts executable
log_nfl "🏈 STEP 4: Setting Script Permissions"
log_info "==================================="

SCRIPTS=(
    "enhanced_football_automation.sh"
    "setup_football_automation.sh"
)

for script in "${SCRIPTS[@]}"; do
    if [ -f "$SCRIPT_DIR/$script" ]; then
        chmod +x "$SCRIPT_DIR/$script"
        log_info "🔧 Made $script executable"
    fi
done

log_success "✅ Script permissions set"

# Step 5: Test environment
log_nfl "🏈 STEP 5: Testing Environment"
log_info "============================="

# Test Python environment
log_info "🐍 Testing Python environment"
if python3 -c "import requests, bs4, pandas; print('✅ Core packages available')" 2>/dev/null; then
    log_success "✅ Python environment working"
else
    log_error "❌ Python environment test failed"
    exit 1
fi

# Test configuration
log_info "📋 Testing configuration"
if python3 -c "from config import PATHS, DATA_PATH; print(f'✅ Config loaded: {DATA_PATH}')" 2>/dev/null; then
    log_success "✅ Configuration working"
else
    log_error "❌ Configuration test failed"
    exit 1
fi

# Test ESPN access
log_info "🌐 Testing ESPN access"
if python3 -c "import requests; r=requests.get('https://www.espn.com/nfl/schedule', headers={'User-Agent': 'Mozilla/5.0'}, timeout=5); print(f'✅ ESPN accessible: {r.status_code}')" 2>/dev/null; then
    log_success "✅ ESPN access working"
else
    log_warning "⚠️ ESPN access test failed (may be temporary)"
fi

# Step 6: Create sample data
log_nfl "🏈 STEP 6: Creating Sample Data Structure"
log_info "======================================="

# Create sample week data file
SAMPLE_WEEK_FILE="$FOOTBALL_DATA_DIR/data/2025/week_03/week_03_2025.json"
mkdir -p "$(dirname "$SAMPLE_WEEK_FILE")"

if [ ! -f "$SAMPLE_WEEK_FILE" ]; then
    log_info "📄 Creating sample week data structure"
    cat > "$SAMPLE_WEEK_FILE" << EOF
{
  "week": 3,
  "season": 2025,
  "season_type": 1,
  "season_type_name": "preseason",
  "games": [],
  "generated_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "total_games": 0,
  "completed_games": 0,
  "status": "ready_for_scraping"
}
EOF
    log_success "✅ Created sample data structure"
fi

# Summary
log_nfl "🏈 FOOTBALL AUTOMATION SETUP COMPLETE!"
log_info "====================================="
log_info "📊 Setup Summary:"
log_info "   Environment: $([ "$NODE_ENV" = "production" ] && echo "Production" || echo "Development")"
log_info "   FootballData: $FOOTBALL_DATA_DIR"
log_info "   Virtual Environment: $VENV_DIR"
log_info "   Configuration: $ENV_FILE"
log_info "   Logs: $SCRIPT_DIR/logs"
log_info ""
log_info "🚀 Ready to use:"
log_info "   ./enhanced_football_automation.sh --week 3 --preseason    # Process preseason week 3"
log_info "   ./enhanced_football_automation.sh --help                  # Show all options"
log_info ""
log_info "🔧 Manual testing:"
log_info "   source venv/bin/activate && python enhanced_nfl_scraper.py --help"
log_info "   source venv/bin/activate && python fix_nfl_scraper.py     # Test quick scraper"
log_info ""
log_success "✅ FootballScraper environment is ready for automation!"

deactivate 2>/dev/null || true  # Deactivate venv if active