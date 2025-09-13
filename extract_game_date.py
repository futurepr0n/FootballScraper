#!/usr/bin/env python3
"""
Extract actual game dates from ESPN URLs for proper CSV file naming
"""

import re
import subprocess

def extract_game_date_from_espn_url(game_url):
    """Extract the actual game date from ESPN game page title"""
    try:
        # Fetch the page title which contains the date
        cmd = ['curl', '-s', game_url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            print(f"Failed to fetch {game_url}")
            return None
        
        html_content = result.stdout
        
        # Look for the title tag with date pattern
        # Format: "Commanders 20-16 Cardinals (Sep 10, 2023) Final Score - ESPN"
        title_match = re.search(r'<title[^>]*>.*?\(([^)]+)\).*?</title>', html_content)
        
        if title_match:
            date_str = title_match.group(1).strip()
            print(f"Found date string: '{date_str}'")
            
            # Parse dates like "Sep 10, 2023"
            date_pattern = r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})'
            date_match = re.match(date_pattern, date_str)
            
            if date_match:
                month_str = date_match.group(1)
                day = date_match.group(2).zfill(2)  # Zero-pad day
                year = date_match.group(3)
                
                # Convert month abbreviation to number
                months = {
                    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                }
                
                month_num = months.get(month_str, '01')
                
                # Format as YYYYMMDD
                formatted_date = f"{year}{month_num}{day}"
                print(f"Formatted date: {formatted_date}")
                return formatted_date
        
        # Also try meta description as fallback
        desc_match = re.search(r'from ([^"]+) on ESPN', html_content)
        if desc_match:
            date_str = desc_match.group(1).strip()
            print(f"Found fallback date string: '{date_str}'")
            
            # Parse "September 10, 2023" format
            full_date_pattern = r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})'
            date_match = re.match(full_date_pattern, date_str)
            
            if date_match:
                month_str = date_match.group(1)
                day = date_match.group(2).zfill(2)
                year = date_match.group(3)
                
                # Convert full month name to number
                full_months = {
                    'January': '01', 'February': '02', 'March': '03', 'April': '04',
                    'May': '05', 'June': '06', 'July': '07', 'August': '08',
                    'September': '09', 'October': '10', 'November': '11', 'December': '12'
                }
                
                month_num = full_months.get(month_str, '01')
                formatted_date = f"{year}{month_num}{day}"
                print(f"Fallback formatted date: {formatted_date}")
                return formatted_date
        
        print("Could not extract date from page")
        return None
        
    except Exception as e:
        print(f"Error extracting date from {game_url}: {e}")
        return None

def main():
    # Test with the problematic game
    test_url = "https://www.espn.com/nfl/game/_/gameId/401547406"
    print(f"Testing date extraction for: {test_url}")
    
    game_date = extract_game_date_from_espn_url(test_url)
    
    if game_date:
        print(f"✅ Successfully extracted game date: {game_date}")
        print(f"Example filename: nfl_WAS_passing_week1_{game_date}_401547406.csv")
    else:
        print("❌ Failed to extract game date")

if __name__ == "__main__":
    main()