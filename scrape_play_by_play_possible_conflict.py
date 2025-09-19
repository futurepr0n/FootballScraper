import argparse
import pandas as pd
import re
import time
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def scrape_play_by_play(game_id: str):
    """
    Scrapes the play-by-play data for a given NFL game from ESPN.

    Args:
        game_id (str): The ESPN game ID for the game to scrape.
    """
    url = f"https://www.espn.com/nfl/playbyplay/_/gameId/{game_id}"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            print(f"Navigating to {url}...")
            page.goto(url, wait_until="networkidle", timeout=30000)
            
            main_container_selector = '[data-testid="prism-tab-list-panel"]'
            print(f"Waiting for main container: {main_container_selector}")
            page.wait_for_selector(main_container_selector, timeout=15000)
            
            accordion_buttons = page.locator('[data-testid="prism-Accordion"] button')
            header_count = accordion_buttons.count()
            print(f"Found {header_count} accordion headers to click.")

            for i in range(header_count):
                try:
                    accordion_buttons.nth(i).click()
                    time.sleep(0.2)
                except Exception as e:
                    print(f"Could not click accordion header {i}: {e}")

            print("All accordions expanded. Scraping play-by-play data...")
            
            play_by_play_container = page.locator(main_container_selector)
            all_plays_html = play_by_play_container.inner_html()

            soup = BeautifulSoup(all_plays_html, 'lxml')
            
            plays_data = []
            
            # Each drive is within a 'prism-Accordion'
            drives = soup.find_all('section', {'data-testid': 'prism-Accordion'})

            for drive in drives:
                # The plays are within 'prism-LayoutCard' sections
                plays = drive.find_all('section', {'data-testid': 'prism-LayoutCard'})
                for play in plays:
                    play_info = play.find('div', class_='zkpVE')
                    if not play_info:
                        continue

                    playcall_tag = play_info.find('div', class_='Bneh')
                    time_quarter_tag = play_info.find('div', class_='FWLyZ')
                    
                    playcall = playcall_tag.text.strip() if playcall_tag else 'N/A'
                    time_quarter_text = time_quarter_tag.text.strip() if time_quarter_tag else ''
                    
                    time_match = re.search(r'(\d{1,2}:\d{2})', time_quarter_text)
                    quarter_match = re.search(r'(\d+)(?:st|nd|rd|th)', time_quarter_text)
                    
                    time_val = time_match.group(1) if time_match else 'N/A'
                    quarter_val = quarter_match.group(1) if quarter_match else 'N/A'

                    play_description_tag = play.find('div', class_='kSGlO')
                    play_description = ' '.join(p.text.strip() for p in play_description_tag.find_all('div')) if play_description_tag else 'N/A'

                    plays_data.append({
                        "Playcall": playcall,
                        "Time": time_val,
                        "Quarter": quarter_val,
                        "Play": play_description
                    })

            output_df = pd.DataFrame(plays_data)
            output_filename = f"play_by_play_{game_id}.csv"
            output_path = f"/Users/futurepr0n/Development/Capping.Pro/Revamp/FootballScraper/{output_filename}"
            output_df.to_csv(output_path, index=False)
            
            print(f"Successfully scraped play-by-play data to {output_path}")

        except Exception as e:
            print(f"An error occurred: {e}")
            page.screenshot(path='error_screenshot.png')
            print("A screenshot has been saved as error_screenshot.png")
        finally:
            browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape NFL play-by-play data from ESPN.")
    parser.add_argument("game_id", type=str, help="The ESPN game ID.")
    args = parser.parse_args()
    
    scrape_play_by_play(args.game_id)
