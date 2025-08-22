#!/usr/bin/env python3
"""
Analyze complete NFL boxscore structure to find all team sections and stat categories
"""

import requests
from bs4 import BeautifulSoup
import re

def analyze_complete_nfl_structure(url):
    """Analyze complete NFL boxscore structure"""
    print(f"Analyzing complete structure: {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n=== COMPLETE NFL STRUCTURE ANALYSIS ===")
        
        # Look for TeamTitle__Name elements (like baseball structure)
        team_titles = soup.find_all('div', class_='TeamTitle__Name')
        print(f"\n1. TeamTitle__Name elements: {len(team_titles)}")
        
        teams_and_categories = {}
        
        for i, title in enumerate(team_titles):
            title_text = title.get_text(strip=True)
            print(f"   Title {i+1}: '{title_text}'")
            
            # Extract team name and category
            if ' ' in title_text:
                parts = title_text.split()
                team = parts[0]  # First word should be team name
                category = ' '.join(parts[1:])  # Rest is category
                
                if team not in teams_and_categories:
                    teams_and_categories[team] = []
                teams_and_categories[team].append(category)
        
        print(f"\n2. Teams and their stat categories:")
        for team, categories in teams_and_categories.items():
            print(f"   {team}: {categories}")
        
        # Look for all possible stat categories by analyzing section headers
        print(f"\n3. All section headers analysis:")
        
        # Look for sections that might contain team stats
        all_sections = soup.find_all(['section', 'div'], class_=re.compile(r'(team|boxscore)', re.I))
        
        stat_keywords = set()
        for section in all_sections[:20]:  # Limit to first 20 for readability
            section_text = section.get_text().lower()
            
            # Look for common NFL stat keywords
            nfl_keywords = [
                'passing', 'rushing', 'receiving', 'defense', 'kicking', 'punting',
                'kick returns', 'punt returns', 'fumbles', 'interceptions',
                'tackles', 'sacks', 'touchdowns', 'field goals', 'extra points'
            ]
            
            for keyword in nfl_keywords:
                if keyword in section_text:
                    stat_keywords.add(keyword)
        
        print(f"   Found stat keywords: {sorted(stat_keywords)}")
        
        # Look for the main boxscore container structure
        print(f"\n4. Main boxscore container analysis:")
        main_container = soup.find('div', class_='Boxscore')
        if main_container:
            print(f"   Found main Boxscore container")
            
            # Look for team-specific containers within it
            team_containers = main_container.find_all('div', class_='Boxscore__Team')
            print(f"   Team containers: {len(team_containers)}")
            
            for i, team_container in enumerate(team_containers[:2]):
                print(f"\n   Team Container {i+1}:")
                
                # Look for team identification
                team_links = team_container.find_all('a', href=re.compile(r'/nfl/team/'))
                for link in team_links:
                    team_name = link.get_text(strip=True)
                    href = link.get('href', '')
                    print(f"     Team link: {team_name} ({href})")
                
                # Look for categories in this team container
                categories = team_container.find_all('div', class_='Boxscore__Category')
                print(f"     Categories in this team: {len(categories)}")
                
                for j, cat in enumerate(categories[:5]):  # First 5 categories
                    # Look for category title
                    title_elem = cat.find('div', class_='TeamTitle__Name')
                    if title_elem:
                        cat_title = title_elem.get_text(strip=True)
                        print(f"       Category {j+1}: {cat_title}")
        
        print("\n=== END COMPLETE ANALYSIS ===")
        
    except Exception as e:
        print(f"Error analyzing page: {e}")

if __name__ == "__main__":
    # Analyze the Ravens vs Colts boxscore
    test_url = "https://www.espn.com/nfl/boxscore/_/gameId/401773001"
    analyze_complete_nfl_structure(test_url)