#!/usr/bin/env python3
"""
Debug NFL statistics extraction
Test the actual table parsing for NFL boxscores
"""

import requests
from bs4 import BeautifulSoup
import re

def debug_nfl_stats_extraction(url):
    """Debug NFL boxscore statistics extraction"""
    print(f"Debugging: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n=== STATISTICS EXTRACTION DEBUG ===")
        
        # Look for statistical sections
        stats_sections = soup.find_all(['section', 'div'], 
                                     class_=re.compile(r'(stats|boxscore|statistics)', re.I))
        
        print(f"Found {len(stats_sections)} statistical sections")
        
        for i, section in enumerate(stats_sections[:5]):
            print(f"\n--- Section {i+1}: {section.name} ---")
            classes = section.get('class', [])
            print(f"Classes: {classes}")
            
            # Look for tables within this section
            tables = section.find_all('table')
            print(f"Tables in section: {len(tables)}")
            
            for j, table in enumerate(tables[:2]):  # Show first 2 tables
                print(f"\n  Table {j+1}:")
                table_classes = table.get('class', [])
                print(f"    Classes: {table_classes}")
                
                # Get headers
                headers = table.find_all(['th', 'td'])
                if headers:
                    header_text = ' | '.join(h.get_text(strip=True) for h in headers[:8])  # First 8 headers
                    print(f"    Headers: {header_text}")
                
                # Get sample data
                rows = table.find_all('tr')
                print(f"    Rows: {len(rows)}")
                
                if len(rows) > 1:
                    # Look at first data row
                    data_row = rows[1]
                    cells = data_row.find_all(['td', 'th'])
                    if cells:
                        sample_data = ' | '.join(cell.get_text(strip=True) for cell in cells[:6])  # First 6 cells
                        print(f"    Sample data: {sample_data}")
                
                # Check if this looks like NFL stats
                all_text = table.get_text().lower()
                nfl_keywords = ['pass', 'rush', 'rec', 'yds', 'td', 'att', 'comp']
                found_keywords = [kw for kw in nfl_keywords if kw in all_text]
                if found_keywords:
                    print(f"    NFL keywords found: {found_keywords}")
                    print("    *** THIS TABLE LOOKS LIKE NFL STATS ***")
        
        print("\n=== END DEBUG ===")
        
    except Exception as e:
        print(f"Error debugging page: {e}")

if __name__ == "__main__":
    # Debug the Ravens vs Colts boxscore
    test_url = "https://www.espn.com/nfl/boxscore/_/gameId/401773001"
    debug_nfl_stats_extraction(test_url)