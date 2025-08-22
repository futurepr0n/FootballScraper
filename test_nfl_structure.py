#!/usr/bin/env python3
"""
Test NFL page structure analysis
Examine the structure of ESPN NFL boxscore pages
"""

import requests
from bs4 import BeautifulSoup

def analyze_nfl_page_structure(url):
    """Analyze ESPN NFL page structure"""
    print(f"Analyzing: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n=== PAGE STRUCTURE ANALYSIS ===")
        
        # Check for various table-like structures
        print(f"\n1. Regular <table> tags: {len(soup.find_all('table'))}")
        for i, table in enumerate(soup.find_all('table')[:5]):  # Show first 5
            classes = table.get('class', [])
            print(f"   Table {i+1}: classes={classes}")
            
            # Check headers to understand content
            headers = table.find_all(['th', 'td'])
            if headers:
                header_text = ' '.join(h.get_text(strip=True).lower() for h in headers[:5])
                print(f"      Sample headers: {header_text}")
            
            # Check if this looks like a stats table
            rows = table.find_all('tr')
            print(f"      Rows: {len(rows)}")
            if len(rows) > 1:
                first_data_row = rows[1].find_all(['td', 'th'])
                if first_data_row:
                    sample_data = ' | '.join(cell.get_text(strip=True) for cell in first_data_row[:3])
                    print(f"      Sample data: {sample_data}")
        
        # Check for sections that might contain stats
        stats_sections = soup.find_all(['section', 'div'], class_=lambda x: x and 'stats' in str(x).lower())
        print(f"\n2. Sections with 'stats' in class: {len(stats_sections)}")
        for i, section in enumerate(stats_sections[:3]):
            classes = section.get('class', [])
            print(f"   Stats section {i+1}: {section.name}, classes={classes}")
            
            # Look for tables within these sections
            tables_in_section = section.find_all('table')
            print(f"      Tables in section: {len(tables_in_section)}")
            
            # Look for any structured data
            if not tables_in_section:
                # Look for other structured elements
                structured = section.find_all(['ul', 'ol', 'dl', 'div'], class_=True)
                print(f"      Other structured elements: {len(structured[:5])}")
                for j, elem in enumerate(structured[:2]):
                    elem_classes = elem.get('class', [])
                    print(f"         Element {j+1}: {elem.name}, classes={elem_classes}")
                    sample_text = elem.get_text(strip=True)[:100] + "..." if len(elem.get_text(strip=True)) > 100 else elem.get_text(strip=True)
                    print(f"         Sample text: {sample_text}")
        
        print(f"\n3. Sections with 'boxscore' in class: {len(soup.find_all(['section', 'div'], class_=lambda x: x and 'boxscore' in str(x).lower()))}")
        
        print(f"\n4. Elements with 'statistics' in class: {len(soup.find_all(['section', 'div'], class_=lambda x: x and 'statistics' in str(x).lower()))}")
        
        # Look for any structure that might contain player stats
        print(f"\n5. Looking for player-related elements...")
        player_elements = soup.find_all(['div', 'section', 'table'], class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ['player', 'roster', 'team-stats', 'game-stats']
        ))
        print(f"   Player-related elements: {len(player_elements)}")
        for i, elem in enumerate(player_elements[:3]):
            classes = elem.get('class', [])
            print(f"   Element {i+1}: {elem.name}, classes={classes}")
        
        # Check for common ESPN structural elements
        print(f"\n6. ESPN structural analysis...")
        espn_sections = soup.find_all(['section', 'div'], class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ['card', 'widget', 'content', 'game']
        ))
        print(f"   ESPN sections found: {len(espn_sections)}")
        
        # Look for specific NFL stat categories
        print(f"\n7. NFL-specific content...")
        nfl_keywords = ['passing', 'rushing', 'receiving', 'defense', 'kicking']
        for keyword in nfl_keywords:
            elements = soup.find_all(text=lambda text: text and keyword.lower() in text.lower())
            print(f"   '{keyword}' mentions: {len(elements)}")
        
        # Check page title and basic info
        title = soup.find('title')
        print(f"\n8. Page title: {title.get_text() if title else 'Not found'}")
        
        # Look for game status indicators
        status_elements = soup.find_all(['div', 'span'], class_=lambda x: x and 'status' in str(x).lower())
        print(f"\n9. Status elements: {len(status_elements)}")
        for i, elem in enumerate(status_elements[:3]):
            print(f"   Status {i+1}: {elem.get_text(strip=True)}")
        
        print("\n=== END ANALYSIS ===")
        
    except Exception as e:
        print(f"Error analyzing page: {e}")

if __name__ == "__main__":
    # Test with the Ravens vs Colts game - both regular and boxscore URLs
    game_url = "https://www.espn.com/nfl/game/_/gameId/401773001"
    boxscore_url = "https://www.espn.com/nfl/boxscore/_/gameId/401773001"
    
    print("=== ANALYZING REGULAR GAME PAGE ===")
    analyze_nfl_page_structure(game_url)
    
    print("\n" + "="*60)
    print("=== ANALYZING BOXSCORE PAGE ===")
    analyze_nfl_page_structure(boxscore_url)