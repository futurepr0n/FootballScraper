#!/bin/bash
"""
Simple Play-by-Play Scraper using pure curl and shell tools
Tests ESPN API endpoints and HTML scraping for play-by-play data
"""

set -e

# Configuration
GAME_ID="401547406"
GAME_URL="https://www.espn.com/nfl/game/_/gameId/${GAME_ID}"
PBP_URL="https://www.espn.com/nfl/playbyplay/_/gameId/${GAME_ID}"
CSV_DIR="/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS"

# Create output directory
mkdir -p "$CSV_DIR"

echo "🏈 SIMPLE PLAY-BY-PLAY SCRAPER TEST"
echo "===================================="
echo "Game ID: $GAME_ID"
echo "Testing multiple ESPN endpoints..."
echo

# Test ESPN API endpoints
echo "🔗 Testing ESPN API endpoints..."

API_ENDPOINTS=(
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=${GAME_ID}"
    "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/${GAME_ID}/competitions/${GAME_ID}"
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/playbyplay?event=${GAME_ID}"
    "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/${GAME_ID}"
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
)

for api_url in "${API_ENDPOINTS[@]}"; do
    echo "Testing: $api_url"
    
    # Try to fetch JSON data
    if response=$(curl -s --max-time 10 -H "Accept: application/json" "$api_url" 2>/dev/null); then
        if echo "$response" | grep -q "plays\|drives\|playByPlay"; then
            echo "✅ Found potential play data!"
            echo "$response" | head -c 1000
            echo "..."
            echo
            
            # Save response for analysis
            echo "$response" > "${CSV_DIR}/api_response_${GAME_ID}_$(basename ${api_url}).json"
            echo "💾 Saved response to: api_response_${GAME_ID}_$(basename ${api_url}).json"
            echo
        else
            echo "❌ No play data found"
            echo
        fi
    else
        echo "❌ Request failed"
        echo
    fi
done

echo "🌐 Testing HTML play-by-play page..."
echo "URL: $PBP_URL"

# Fetch HTML with browser-like headers
if html_content=$(curl -s --max-time 30 \
    -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36" \
    -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" \
    -H "Accept-Language: en-US,en;q=0.9" \
    -H "Accept-Encoding: gzip, deflate, br" \
    -H "Cache-Control: no-cache" \
    -H "Connection: keep-alive" \
    --compressed \
    "$PBP_URL" 2>/dev/null); then
    
    echo "✅ Downloaded HTML content: $(echo "$html_content" | wc -c) characters"
    
    # Save HTML for analysis
    echo "$html_content" > "${CSV_DIR}/playbyplay_${GAME_ID}.html"
    echo "💾 Saved HTML to: playbyplay_${GAME_ID}.html"
    
    # Look for embedded JSON
    echo
    echo "🔍 Searching for embedded JSON data..."
    
    if echo "$html_content" | grep -o 'window\.espn\.gamepackage.*=.*{.*}' | head -1; then
        echo "✅ Found window.espn.gamepackage data!"
        echo "$html_content" | grep -o 'window\.espn\.gamepackage.*=.*{.*}' | head -1 | sed 's/.*=//' > "${CSV_DIR}/gamepackage_${GAME_ID}.json"
        echo "💾 Extracted to: gamepackage_${GAME_ID}.json"
    fi
    
    if echo "$html_content" | grep -o '"drives":\s*\[.*\]' | head -1; then
        echo "✅ Found drives JSON data!"
        echo "$html_content" | grep -o '"drives":\s*\[.*\]' | head -1 > "${CSV_DIR}/drives_${GAME_ID}.json"
        echo "💾 Extracted to: drives_${GAME_ID}.json"
    fi
    
    if echo "$html_content" | grep -o '"playByPlay":\s*{.*}' | head -1; then
        echo "✅ Found playByPlay JSON data!"
        echo "$html_content" | grep -o '"playByPlay":\s*{.*}' | head -1 > "${CSV_DIR}/playbyplay_${GAME_ID}.json"
        echo "💾 Extracted to: playbyplay_${GAME_ID}.json"
    fi
    
    # Look for play elements in HTML
    echo
    echo "🔍 Searching for HTML play elements..."
    
    if echo "$html_content" | grep -c 'play.*description\|PlayByPlay\|Playbyplay'; then
        echo "✅ Found $(echo "$html_content" | grep -c 'play.*description\|PlayByPlay\|Playbyplay') potential play elements"
    else
        echo "❌ No play elements found in HTML"
    fi
    
    # Check for JavaScript dependencies
    if echo "$html_content" | grep -q "window.__espnfitt__\|__INITIAL_STATE__\|React\|Vue"; then
        echo "⚠️  Page appears to be JavaScript-heavy"
        echo "   May require browser automation for complete data"
    fi
    
else
    echo "❌ Failed to download HTML content"
fi

echo
echo "📊 SUMMARY"
echo "=========="
echo "✅ Created test files in: $CSV_DIR"
echo "🔍 Check the generated JSON and HTML files for play data"
echo "📋 Next steps:"
echo "   1. Examine JSON files for play-by-play structure"
echo "   2. If no JSON data, consider browser automation (Selenium/Puppeteer)"
echo "   3. Build parser for successful data format"

# List created files
echo
echo "📁 Generated files:"
ls -la "${CSV_DIR}"/*${GAME_ID}* 2>/dev/null || echo "No files generated"

echo
echo "🏁 Test complete!"