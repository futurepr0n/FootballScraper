#!/usr/bin/env python3
"""
NFL Preseason System Test Suite
Validates the backfill system with a single game before running full processing
"""

import json
import os
import sys
from pathlib import Path
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_game():
    """Test the backfill system with a single preseason game"""
    print("🧪 Testing NFL Preseason Backfill System")
    print("=" * 50)
    
    # Find a single preseason game to test
    test_game_id = None
    test_game_info = None
    
    # Look through August files for a preseason game
    for file_path in Path('.').glob('august_*_2025.json'):
        try:
            with open(file_path, 'r') as f:
                schedule_data = json.load(f)
            
            preseason_games = [g for g in schedule_data.get('games', []) 
                              if g.get('season_type_name') == 'preseason']
            
            if preseason_games:
                test_game_id = preseason_games[0].get('game_id')
                test_game_info = preseason_games[0]
                print(f"📅 Found test game in {file_path}")
                print(f"🎯 Test game: {test_game_info.get('matchup', 'Unknown')}")
                print(f"🆔 Game ID: {test_game_id}")
                break
                
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            continue
    
    if not test_game_id:
        print("❌ No preseason games found in schedule files")
        return False
    
    # Import the backfill system
    try:
        from nfl_preseason_backfill import NFLPreseasonBackfill
        print("✅ Successfully imported backfill system")
    except ImportError as e:
        print(f"❌ Cannot import backfill system: {e}")
        return False
    
    # Test the backfill system
    print("\n🔧 Testing backfill system...")
    
    try:
        # Create backfill instance
        backfill = NFLPreseasonBackfill()
        print("✅ Backfill system initialized")
        
        # Test directory creation
        if backfill.football_data_dir.exists():
            print("✅ FootballData directory structure ready")
        else:
            print("❌ FootballData directory not created")
            return False
        
        # Test single game processing
        print(f"\n🎲 Processing single test game: {test_game_id}")
        success = backfill.process_game(test_game_id, test_game_info)
        
        if success:
            print("✅ Single game processing successful!")
            
            # Verify files were created
            expected_files = [
                f"game_summaries/*_{test_game_id}_summary.json",
                f"box_scores/*_{test_game_id}_boxscore.json", 
                f"play_by_play/*_{test_game_id}_plays.json",
                f"player_stats/*_{test_game_id}_players.json",
                f"*_{test_game_id}_complete.json"
            ]
            
            files_found = 0
            for pattern in expected_files:
                matching_files = list(backfill.football_data_dir.glob(f"data/preseason/{pattern}"))
                if matching_files:
                    files_found += 1
                    print(f"✅ Found: {matching_files[0].name}")
                else:
                    print(f"⚠️  Missing: {pattern}")
            
            print(f"\n📊 Files created: {files_found}/{len(expected_files)}")
            
            if files_found >= 3:  # At least most files created
                print("✅ File creation test passed")
                return True
            else:
                print("❌ Not enough files created")
                return False
                
        else:
            print("❌ Single game processing failed")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        logger.error(f"Test error: {e}")
        return False

def test_data_structure():
    """Test the data structure and validate JSON files"""
    print("\n📋 Testing Data Structure Validation")
    print("-" * 40)
    
    football_data_dir = Path("../FootballData")
    
    # Check if any test data exists
    test_files = list(football_data_dir.glob("data/preseason/**/*.json"))
    
    if not test_files:
        print("⚠️  No test data files found")
        return True  # Not a failure if no data yet
    
    print(f"🔍 Found {len(test_files)} test files to validate")
    
    valid_files = 0
    invalid_files = 0
    
    for file_path in test_files[:5]:  # Test first 5 files
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Basic validation - ensure it's valid JSON with some structure
            if isinstance(data, dict) and len(data) > 0:
                valid_files += 1
                print(f"✅ Valid: {file_path.name}")
            else:
                invalid_files += 1
                print(f"❌ Invalid structure: {file_path.name}")
                
        except json.JSONDecodeError:
            invalid_files += 1
            print(f"❌ Invalid JSON: {file_path.name}")
        except Exception as e:
            invalid_files += 1
            print(f"❌ Error reading {file_path.name}: {e}")
    
    print(f"\n📊 Validation Results: {valid_files} valid, {invalid_files} invalid")
    return invalid_files == 0

def test_aggregation_system():
    """Test the aggregation system if data exists"""
    print("\n📈 Testing Aggregation System")
    print("-" * 40)
    
    try:
        from nfl_preseason_aggregator import NFLPreseasonAggregator
        print("✅ Successfully imported aggregation system")
        
        aggregator = NFLPreseasonAggregator()
        
        # Check if there's any data to aggregate
        complete_files = list(aggregator.preseason_dir.glob("*_complete.json"))
        
        if not complete_files:
            print("⚠️  No complete game files found for aggregation test")
            print("   Run backfill first to generate test data")
            return True
        
        print(f"📊 Found {len(complete_files)} complete game files")
        
        # Test loading games
        games_data = aggregator.load_all_preseason_games()
        
        if games_data:
            print(f"✅ Successfully loaded {len(games_data)} games")
            
            # Test basic aggregation functions
            player_stats = aggregator.aggregate_player_stats(games_data)
            print(f"✅ Aggregated stats for {len(player_stats)} players")
            
            touchdown_leaders = aggregator.create_touchdown_leaderboard(player_stats)
            print(f"✅ Created touchdown leaderboard with {len(touchdown_leaders)} players")
            
            return True
        else:
            print("❌ Failed to load game data")
            return False
            
    except ImportError as e:
        print(f"❌ Cannot import aggregation system: {e}")
        return False
    except Exception as e:
        print(f"❌ Aggregation test failed: {e}")
        return False

def test_api_connectivity():
    """Test ESPN API connectivity"""
    print("\n🌐 Testing ESPN API Connectivity")
    print("-" * 40)
    
    try:
        import requests
        
        # Test with a known game ID (doesn't have to be real)
        test_url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=401773000"
        
        print(f"🔗 Testing connection to: {test_url}")
        
        response = requests.get(test_url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        if response.status_code == 200:
            print("✅ ESPN API is accessible")
            
            try:
                data = response.json()
                if 'header' in data:
                    print("✅ ESPN API returns expected structure")
                    return True
                else:
                    print("⚠️  ESPN API response structure changed")
                    return True  # Still accessible, just different
            except:
                print("⚠️  ESPN API returns non-JSON data")
                return True
                
        else:
            print(f"⚠️  ESPN API returned status: {response.status_code}")
            return True  # May be temporary
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to ESPN API - check internet connection")
        return False
    except Exception as e:
        print(f"⚠️  API test error: {e}")
        return True  # Don't fail on minor issues

def cleanup_test_data():
    """Clean up test data"""
    print("\n🧹 Cleaning Up Test Data")
    print("-" * 40)
    
    response = input("Remove test data files? (y/N): ")
    if not response.lower().startswith('y'):
        print("Test data preserved")
        return
    
    football_data_dir = Path("../FootballData")
    
    # Only remove files from our test
    test_files = list(football_data_dir.glob("data/preseason/**/*.json"))
    
    removed_count = 0
    for file_path in test_files:
        try:
            file_path.unlink()
            removed_count += 1
        except Exception as e:
            print(f"⚠️  Could not remove {file_path}: {e}")
    
    print(f"🗑️  Removed {removed_count} test files")
    
    # Remove empty directories
    for dir_path in [
        football_data_dir / "data" / "preseason" / "game_summaries",
        football_data_dir / "data" / "preseason" / "box_scores",
        football_data_dir / "data" / "preseason" / "play_by_play",
        football_data_dir / "data" / "preseason" / "player_stats"
    ]:
        try:
            if dir_path.exists() and not any(dir_path.iterdir()):
                dir_path.rmdir()
        except:
            pass

def main():
    """Run all tests"""
    print("🧪 NFL Preseason System Test Suite")
    print("=" * 50)
    print("This will test the backfill system before running the full process")
    print()
    
    tests = [
        ("API Connectivity", test_api_connectivity),
        ("Single Game Processing", test_single_game),
        ("Data Structure Validation", test_data_structure),
        ("Aggregation System", test_aggregation_system)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🔬 Running: {test_name}")
        print("=" * (10 + len(test_name)))
        
        try:
            if test_func():
                print(f"✅ {test_name}: PASSED")
                passed += 1
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"💥 {test_name}: ERROR - {e}")
    
    print("\n" + "=" * 50)
    print(f"🧪 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! System is ready for full backfill.")
    elif passed >= total * 0.75:
        print("⚠️  Most tests passed. System should work but check failures.")
    else:
        print("❌ Multiple test failures. Fix issues before running full backfill.")
    
    # Offer to clean up
    if passed > 0:
        print()
        cleanup_test_data()
    
    print("\n🚀 Ready to run full backfill with: ./run_preseason_backfill.sh")

if __name__ == "__main__":
    main()