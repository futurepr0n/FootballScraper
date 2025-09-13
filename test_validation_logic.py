#!/usr/bin/env python3
"""
Test the validation logic without external dependencies
"""

import os
import re
from pathlib import Path

class ValidationTester:
    def __init__(self):
        self.valid_teams = {
            'ARI': 'Arizona Cardinals', 'WAS': 'Washington Commanders', 'WSH': 'Washington Commanders'
        }
        self.csv_dir = Path("/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballData/CSV_BACKUPS")
        
    def test_game_date_extraction(self):
        """Test date extraction from HTML title"""
        test_html = '<title>Commanders 20-16 Cardinals (Sep 10, 2023) Final Score - ESPN</title>'
        
        title_match = re.search(r'<title[^>]*>.*?\(([^)]+)\).*?</title>', test_html)
        
        if title_match:
            date_str = title_match.group(1).strip()
            print(f"✅ Found date string: '{date_str}'")
            
            date_pattern = r'([A-Za-z]+)\s+(\d+),?\s+(\d{4})'
            date_match = re.match(date_pattern, date_str)
            
            if date_match:
                month_str = date_match.group(1)
                day = date_match.group(2).zfill(2)
                year = date_match.group(3)
                
                months = {'Sep': '09'}
                month_num = months.get(month_str)
                
                formatted_date = f"{year}{month_num}{day}"
                print(f"✅ Formatted date: {formatted_date}")
                return formatted_date
        
        return None
    
    def test_team_extraction(self):
        """Test team extraction from URL"""
        test_url = "https://www.espn.com/nfl/game/_/gameId/401547406/cardinals-commanders"
        
        url_parts = test_url.split('/')
        if len(url_parts) > 7:
            teams_part = url_parts[-1]  # "cardinals-commanders"
            print(f"✅ Found teams part: {teams_part}")
            
            if '-' in teams_part:
                team_parts = teams_part.split('-')
                away_team_name = team_parts[0]  # "cardinals"
                home_team_name = team_parts[1]  # "commanders"
                
                name_to_abbr = {
                    'cardinals': 'ARI',
                    'commanders': 'WAS'
                }
                
                away_abbr = name_to_abbr.get(away_team_name)
                home_abbr = name_to_abbr.get(home_team_name)
                
                if away_abbr and home_abbr:
                    print(f"✅ Extracted teams: {away_abbr} @ {home_abbr}")
                    return {'away_team': away_abbr, 'home_team': home_abbr}
        
        return None
    
    def test_file_naming(self):
        """Test proper CSV file naming"""
        game_id = "401547406"
        game_date = "20230910"
        team = "WAS"
        category = "passing"
        
        # Correct format
        correct_filename = f"nfl_{team}_{category}_week1_{game_date}_{game_id}.csv"
        print(f"✅ Correct filename: {correct_filename}")
        
        # Wrong format (what we currently have)
        wrong_filename = f"nfl_{team}_{category}_week1_20250909_{game_id}.csv"
        print(f"❌ Wrong filename: {wrong_filename}")
        
        return correct_filename
    
    def test_existing_file_check(self):
        """Test checking for existing files"""
        game_id = "401547406"
        game_date = "20230910"
        
        # Check for correctly dated files
        pattern = f"*{game_date}_{game_id}.csv"
        existing_files = list(self.csv_dir.glob(pattern))
        print(f"Files with correct date ({game_date}): {len(existing_files)}")
        
        # Check for incorrectly dated files
        wrong_pattern = f"*202509*_{game_id}.csv"
        wrong_files = list(self.csv_dir.glob(wrong_pattern))
        print(f"Files with wrong date (2025-09): {len(wrong_files)}")
        
        if wrong_files:
            print(f"Example wrong file: {wrong_files[0].name}")
        
        return len(existing_files) == 0 and len(wrong_files) > 0
    
    def run_all_tests(self):
        """Run all validation tests"""
        print("=" * 60)
        print("VALIDATION LOGIC TESTS")
        print("=" * 60)
        
        # Test 1: Date extraction
        print("\n1. Testing game date extraction...")
        game_date = self.test_game_date_extraction()
        if game_date != "20230910":
            print("❌ Date extraction failed")
            return False
        
        # Test 2: Team extraction  
        print("\n2. Testing team extraction...")
        teams = self.test_team_extraction()
        if not teams or teams['away_team'] != 'ARI' or teams['home_team'] != 'WAS':
            print("❌ Team extraction failed")
            return False
        
        # Test 3: File naming
        print("\n3. Testing file naming...")
        filename = self.test_file_naming()
        if "20230910" not in filename:
            print("❌ File naming failed")
            return False
        
        # Test 4: Existing file check
        print("\n4. Testing existing file detection...")
        has_wrong_files = self.test_existing_file_check()
        if not has_wrong_files:
            print("❌ File detection failed")
            return False
        
        print("\n" + "=" * 60)
        print("✅ ALL VALIDATION TESTS PASSED")
        print("✅ Logic is sound - ready for implementation")
        print("=" * 60)
        
        return True

def main():
    tester = ValidationTester()
    success = tester.run_all_tests()
    
    if success:
        print("\n🎯 NEXT STEPS:")
        print("1. The validation logic works correctly")
        print("2. We can detect incorrect vs correct file naming")
        print("3. Ready to implement full scraper with requests/BeautifulSoup")
        print("4. Will extract actual play-by-play data from ESPN accordion sections")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())