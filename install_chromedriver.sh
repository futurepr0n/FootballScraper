#!/bin/bash
# ChromeWebDriver Installation Script
# Run this script to install ChromeWebDriver for the accordion expansion scraper

echo "🏈 Installing ChromeWebDriver for ESPN Play-by-Play Scraping"
echo "============================================================"

# Check if Chrome is installed
if ! command -v /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome &> /dev/null; then
    echo "❌ Google Chrome not found in Applications folder"
    echo "   Please install Google Chrome first from: https://www.google.com/chrome/"
    exit 1
fi

# Get Chrome version
CHROME_VERSION=$(/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --version | cut -d' ' -f3 | cut -d'.' -f1-3)
echo "✅ Found Google Chrome version: $CHROME_VERSION"

# Install webdriver-manager if not already installed
echo "📦 Installing Python webdriver-manager..."
source venv/bin/activate
pip install webdriver-manager selenium

echo "✅ ChromeWebDriver setup complete!"
echo ""
echo "🎯 Next Steps:"
echo "1. Run: python3 enhanced_playbyplay_scraper.py"
echo "2. The script will automatically download the correct ChromeDriver"
echo "3. It will use your accordion expansion approach to extract plays"
echo ""
echo "💡 If you get permission errors, run:"
echo "   chmod +x enhanced_playbyplay_scraper.py"