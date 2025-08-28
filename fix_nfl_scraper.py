#!/usr/bin/env python3
"""
Quick fix for NFL scraper parsing issues
"""

import requests
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin

def quick_scrape_nfl_week(season=2025, season_type=1, week=3):
    """Quick scrape function using working method from test"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    url = f"https://www.espn.com/nfl/schedule/_/week/{week}/year/{season}/seasontype/{season_type}"
    print(f"🏈 Fetching NFL Week {week} from: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find game links
        game_links = soup.find_all('a', href=re.compile(r'/nfl/game/'))
        print(f"🔗 Found {len(game_links)} game links")
        
        games = []
        
        for link in game_links:
            try:
                href = link.get('href', '')
                text = link.get_text().strip()
                
                # Extract game ID from URL
                game_id_match = re.search(r'/game/_/gameId/(\d+)', href)
                if not game_id_match:
                    continue
                    
                game_id = game_id_match.group(1)
                
                # Build full URL
                game_url = urljoin("https://www.espn.com", href)
                
                # Extract teams from text or URL
                teams = []
                
                # Try to extract from URL path
                url_parts = href.split('/')
                if len(url_parts) > 4:
                    team_part = url_parts[-1]  # Last part often has team names
                    if '-' in team_part:
                        team_names = team_part.split('-')
                        if len(team_names) >= 2:
                            # Map to abbreviations (simplified)
                            team_map = {
                                'steelers': 'PIT', 'panthers': 'CAR',
                                'patriots': 'NE', 'giants': 'NYG',
                                'eagles': 'PHI', 'jets': 'NYJ',
                                'falcons': 'ATL', 'cowboys': 'DAL',
                                'vikings': 'MIN', 'titans': 'TEN',
                                'bears': 'CHI', 'chiefs': 'KC',
                                'broncos': 'DEN', 'rams': 'LAR',
                                'dolphins': 'MIA', 'lions': 'DET',
                                'seahawks': 'SEA'
                            }
                            
                            away_team = team_map.get(team_names[0], team_names[0][:3].upper())
                            home_team = team_map.get(team_names[1], team_names[1][:3].upper())
                            
                            game_info = {
                                'game_id': game_id,
                                'game_url': game_url,
                                'away_team': away_team,
                                'home_team': home_team,
                                'season': season,
                                'season_type': season_type,
                                'week': week,
                                'status': 'completed',
                                'text': text
                            }
                            
                            games.append(game_info)
                            print(f"   ✅ {away_team} @ {home_team} (ID: {game_id})")
                
            except Exception as e:
                print(f"   ❌ Error parsing link: {e}")
                continue
        
        print(f"🎯 Total games parsed: {len(games)}")
        
        # Save to JSON for testing
        if games:
            output_file = f"nfl_week_{week}_{season}_quick.json"
            with open(output_file, 'w') as f:
                json.dump(games, f, indent=2)
            print(f"💾 Saved to: {output_file}")
        
        return games
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

if __name__ == "__main__":
    games = quick_scrape_nfl_week()
    
    if games:
        print("\n🏈 Sample games found:")
        for game in games[:3]:
            print(f"   {game['away_team']} @ {game['home_team']} - {game['game_url']}")
    else:
        print("\n❌ No games found")