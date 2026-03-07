"""
FotMob Lineup Scraper with Screenshot & OCR Analysis
Takes screenshots of the lineup section and uses OCR + image analysis to extract player data

Installation:
    pip install playwright beautifulsoup4 pytesseract pillow opencv-python

On macOS, also install Tesseract:
    brew install tesseract

On Ubuntu:
    sudo apt-get install tesseract-ocr

Usage:
    # Jupyter - just call it directly (auto-detects async)
    scraper = FotMobLineupScreenshotScraper()
    data = scraper.scrape_with_screenshots(url)
    
    # CLI / Script
    data = scraper.scrape_with_screenshots(url)
    
    # Both work the same way!
"""

import json
import re
import sys
import asyncio
import os
import threading
import concurrent.futures
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from pathlib import Path


class FotMobLineupScreenshotScraper:
    def __init__(self, headless=True, screenshot_dir="fotmob_screenshots"):
        self.headless = headless
        self.screenshot_dir = screenshot_dir
        
        # Create screenshot directory
        Path(self.screenshot_dir).mkdir(exist_ok=True)
        
        # Try to import optional dependencies
        self.has_ocr = self._check_ocr_available()
        self.has_cv2 = self._check_cv2_available()

    def _check_ocr_available(self) -> bool:
        """Check if pytesseract is available"""
        try:
            import pytesseract
            return True
        except ImportError:
            print("⚠️  pytesseract not installed. OCR features disabled.")
            print("   Install with: pip install pytesseract")
            return False

    def _check_cv2_available(self) -> bool:
        """Check if OpenCV is available"""
        try:
            import cv2
            return True
        except ImportError:
            print("⚠️  opencv-python not installed. Image analysis features disabled.")
            print("   Install with: pip install opencv-python")
            return False

    def scrape_with_screenshots(self, url: str) -> Dict:
        """
        UNIVERSAL scrape with screenshots
        Works in Jupyter notebooks, CLI, and regular scripts
        Auto-detects the environment and uses the appropriate method
        
        Args:
            url: FotMob match URL
        
        Returns:
            Dictionary containing match and lineup data with screenshots
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in Jupyter or async context - run in separate thread
                print("🔄 Running in async context (Jupyter), using thread wrapper...")
                return self._scrape_in_thread(url)
            else:
                # We're in a regular script with no running loop
                return loop.run_until_complete(self.scrape_from_url_with_screenshots(url))
        except RuntimeError:
            # No event loop exists at all
            return asyncio.run(self.scrape_from_url_with_screenshots(url))

    def _scrape_in_thread(self, url: str) -> Dict:
        """Run async scraping in a separate thread (for Jupyter)"""
        result = None
        exception = None
        
        def run_in_thread():
            nonlocal result, exception
            try:
                # Create new event loop for this thread
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    result = new_loop.run_until_complete(self.scrape_from_url_with_screenshots(url))
                finally:
                    new_loop.close()
            except Exception as e:
                exception = e
        
        thread = threading.Thread(target=run_in_thread, daemon=False)
        thread.start()
        thread.join(timeout=120)  # 2 minute timeout
        
        if exception:
            raise exception
        if result is None:
            raise RuntimeError("Scraping timed out after 2 minutes")
        
        return result

    async def scrape_from_url_with_screenshots(self, url: str) -> Dict:
        """
        ASYNCHRONOUS scrape with screenshots
        Internal method used by scrape_with_screenshots()
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            print("ERROR: playwright not installed. Install with: pip install playwright")
            sys.exit(1)
        
        print(f"📍 Fetching: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            page = await browser.new_page()
            await page.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
            
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                print("✅ Page loaded")
                
                # Wait for lineup content
                try:
                    await page.wait_for_selector("[class*='lineup'], [class*='player']", timeout=10000)
                except:
                    pass
                
                # Get HTML content
                html_content = await page.content()
                
                # Take screenshots of lineup sections
                print("📸 Capturing screenshots...")
                screenshot_paths = await self._take_lineup_screenshots(page)
                
            finally:
                await browser.close()
        
        # Parse HTML
        data = self.parse_html(html_content, url)
        
        # Add screenshot data
        if screenshot_paths:
            data['screenshots'] = screenshot_paths
            
            # Try OCR extraction
            if self.has_ocr:
                print("🔍 Extracting text from screenshots with OCR...")
                ocr_data = self._extract_from_screenshots(screenshot_paths)
                if ocr_data:
                    data['ocr_extracted'] = ocr_data
            
            # Try image analysis
            if self.has_cv2:
                print("📊 Analyzing screenshot images...")
                image_analysis = self._analyze_screenshots(screenshot_paths)
                if image_analysis:
                    data['image_analysis'] = image_analysis
        
        return data

    async def _take_lineup_screenshots(self, page) -> List[str]:
        """Take screenshots of all lineup sections"""
        screenshot_paths = []
        
        try:
            # Try to find lineup containers
            lineup_selectors = [
                "[class*='lineup']",
                "[class*='Lineup']",
                "[class*='player-card']",
                "[data-testid*='lineup']",
            ]
            
            for selector in lineup_selectors:
                try:
                    elements = await page.locator(selector).count()
                    if elements > 0:
                        print(f"  Found {elements} lineup elements with selector: {selector}")
                        
                        # Take screenshot of each element
                        for i in range(min(elements, 5)):  # Limit to 5 screenshots
                            try:
                                element = page.locator(selector).nth(i)
                                filename = f"{self.screenshot_dir}/lineup_{len(screenshot_paths)}.png"
                                await element.screenshot(path=filename)
                                screenshot_paths.append(filename)
                                print(f"  ✓ Saved {filename}")
                            except:
                                pass
                except:
                    pass
            
            # If no specific elements found, take full page screenshot
            if not screenshot_paths:
                print("  No specific lineup elements found, taking full page screenshot...")
                filename = f"{self.screenshot_dir}/full_page.png"
                await page.screenshot(path=filename)
                screenshot_paths.append(filename)
                print(f"  ✓ Saved {filename}")
        
        except Exception as e:
            print(f"  ⚠️  Error taking screenshots: {e}")
        
        return screenshot_paths

    def _extract_from_screenshots(self, screenshot_paths: List[str]) -> Dict:
        """Extract text from screenshots using OCR (Tesseract)"""
        if not self.has_ocr:
            return None
        
        try:
            import pytesseract
            from PIL import Image
            
            extracted_data = {}
            
            for i, screenshot_path in enumerate(screenshot_paths):
                try:
                    # Load image
                    image = Image.open(screenshot_path)
                    
                    # Run OCR
                    text = pytesseract.image_to_string(image)
                    
                    # Extract structured data
                    extracted_data[f'screenshot_{i}'] = {
                        'path': screenshot_path,
                        'raw_text': text,
                        'players': self._parse_player_names_from_text(text),
                        'teams': self._parse_team_names_from_text(text),
                    }
                    
                    print(f"  ✓ Extracted text from {screenshot_path}")
                
                except Exception as e:
                    print(f"  ⚠️  Error processing {screenshot_path}: {e}")
            
            return extracted_data if extracted_data else None
        
        except ImportError:
            print("⚠️  pytesseract not installed")
            return None

    def _analyze_screenshots(self, screenshot_paths: List[str]) -> Dict:
        """Analyze screenshots using OpenCV for image features"""
        if not self.has_cv2:
            return None
        
        try:
            import cv2
            import numpy as np
            
            analysis_data = {}
            
            for i, screenshot_path in enumerate(screenshot_paths):
                try:
                    # Read image
                    image = cv2.imread(screenshot_path)
                    if image is None:
                        continue
                    
                    # Convert to grayscale
                    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                    
                    # Detect edges
                    edges = cv2.Canny(gray, 100, 200)
                    
                    # Detect contours (potential player cards)
                    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    
                    # Analyze colors
                    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
                    
                    analysis_data[f'screenshot_{i}'] = {
                        'path': screenshot_path,
                        'dimensions': image.shape,
                        'contours_found': len(contours),
                        'potential_card_count': min(len(contours), 15),
                        'has_sufficient_content': len(contours) > 5,
                        'color_distribution': self._analyze_color_distribution(hsv),
                    }
                    
                    print(f"  ✓ Analyzed {screenshot_path}: {len(contours)} objects detected")
                
                except Exception as e:
                    print(f"  ⚠️  Error analyzing {screenshot_path}: {e}")
            
            return analysis_data if analysis_data else None
        
        except ImportError:
            print("⚠️  opencv-python not installed")
            return None

    def _analyze_color_distribution(self, hsv_image) -> Dict:
        """Analyze color distribution in HSV image"""
        import numpy as np
        
        pixels = hsv_image.reshape(-1, 3)
        h, s, v = pixels[:, 0], pixels[:, 1], pixels[:, 2]
        
        return {
            'mean_hue': float(np.mean(h)),
            'mean_saturation': float(np.mean(s)),
            'mean_value': float(np.mean(v)),
            'std_hue': float(np.std(h)),
            'std_saturation': float(np.std(s)),
            'std_value': float(np.std(v)),
        }

    def _parse_player_names_from_text(self, text: str) -> List[str]:
        """Parse potential player names from OCR text"""
        players = []
        
        lines = text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            
            if (len(line) > 3 and 
                len(line) < 50 and 
                line[0].isalpha() and
                not re.match(r'^\d+', line)):
                
                line = re.sub(r'[_\-\|]+', '', line)
                
                if line and line not in players:
                    players.append(line)
        
        return players[:30]

    def _parse_team_names_from_text(self, text: str) -> List[str]:
        """Parse potential team names from OCR text"""
        teams = []
        
        pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*\(\s*\d+-\d+-\d+\s*\)'
        matches = re.findall(pattern, text)
        
        teams.extend(matches)
        
        lines = text.split('\n')
        for line in lines:
            if (3 < len(line) < 20 and 
                line.isupper() and 
                line.count(' ') <= 2):
                teams.append(line.strip())
        
        return list(set(teams))

    def scrape_from_html_file(self, filepath: str) -> Dict:
        """Parse HTML from a local file"""
        print(f"📄 Reading HTML file: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        return self.parse_html(html_content)

    def parse_html(self, html_content: str, url: str = None) -> Dict:
        """Parse HTML content and extract lineup data"""
        print("🔍 Parsing HTML...")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        match_info = self._extract_match_info(soup)
        teams = self._extract_team_lineups(soup)
        
        result = {
            'source_url': url,
            'match': match_info,
            'teams': teams,
            'screenshots': [],
            'ocr_extracted': None,
            'image_analysis': None,
        }
        
        return result

    def _extract_match_info(self, soup: BeautifulSoup) -> Dict:
        """Extract match metadata"""
        text = soup.get_text()
        
        match_info = {
            'home_team': None,
            'away_team': None,
            'result': None,
            'competition': None,
            'date': None,
            'stadium': None,
            'status': None
        }
        
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            match = re.search(r'(.+?)\s+vs\s+(.+?)\s*(?:-|–|—|\(|$)', title_text)
            if match:
                match_info['home_team'] = match.group(1).strip()
                match_info['away_team'] = match.group(2).strip()
        
        score_match = re.search(r'(\d+)\s*(?:-|–|—)\s*(\d+)\s+(?:Full time|Live|HT|FT)', text)
        if score_match:
            match_info['result'] = f"{score_match.group(1)}-{score_match.group(2)}"
        
        if 'Full time' in text or 'FT' in text:
            match_info['status'] = 'Full time'
        elif 'HT' in text or 'Half time' in text:
            match_info['status'] = 'Half time'
        elif 'Live' in text or 'LIVE' in text:
            match_info['status'] = 'Live'
        
        competitions = [
            'Champions League', 'Europa League', 'Conference League',
            'Premier League', 'LaLiga', 'Serie A', 'Bundesliga', 'Ligue 1',
            'FA Cup', 'EFL Cup', 'Coppa Italia', 'DFB-Pokal'
        ]
        for comp in competitions:
            if comp in text:
                match_info['competition'] = comp
                break
        
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
        if date_match:
            match_info['date'] = date_match.group(1)
        
        return match_info

    def _extract_team_lineups(self, soup: BeautifulSoup) -> Dict:
        """Extract team lineups"""
        teams = self._extract_from_player_cards(soup)
        
        if not teams or len(teams) < 2:
            teams = self._extract_from_text_patterns(soup)
        
        return teams

    def _extract_from_player_cards(self, soup: BeautifulSoup) -> Dict:
        """Extract teams from player card structure"""
        teams = {}
        text = soup.get_text()
        
        player_links = soup.find_all('a', href=re.compile(r'/players/\d+/'))
        
        if not player_links:
            return {}
        
        formations = re.findall(r'(\d+-\d+-\d+)', text)
        
        processed_players = []
        for link in player_links:
            player_info = self._extract_player_info(link)
            if player_info and player_info['name'] not in [p['name'] for p in processed_players]:
                processed_players.append(player_info)
        
        if len(processed_players) >= 20:
            split_point = len(processed_players) // 2
            
            teams = {
                'team_1': {
                    'name': 'Team 1',
                    'formation': formations[0] if formations else None,
                    'starting_xi': processed_players[:11],
                    'substitutes': processed_players[11:split_point],
                    'bench': processed_players[split_point:]
                },
                'team_2': {
                    'name': 'Team 2',
                    'formation': formations[1] if len(formations) > 1 else None,
                    'starting_xi': processed_players[split_point:split_point+11],
                    'substitutes': processed_players[split_point+11:split_point+15],
                    'bench': processed_players[split_point+15:]
                }
            }
        
        return teams

    def _extract_player_info(self, player_link) -> Optional[Dict]:
        """Extract player information from a player link"""
        try:
            player_name = player_link.get_text(strip=True)
            
            if not player_name or len(player_name) < 2:
                return None
            
            player_info = {
                'name': player_name,
                'number': None,
                'position': None,
                'sub_time': None
            }
            
            parent = player_link.find_parent(['div', 'span', 'p'])
            if parent:
                parent_text = parent.get_text()
                
                number_match = re.search(r'\b(\d{1,2})\b', parent_text)
                if number_match:
                    player_info['number'] = int(number_match.group(1))
                
                for pos in ['Keeper', 'Goalkeeper', 'Defender', 'Midfielder', 'Forward', 'Attacker']:
                    if pos.lower() in parent_text.lower():
                        player_info['position'] = pos
                        break
                
                time_match = re.search(r"(\d+)'", parent_text)
                if time_match:
                    player_info['sub_time'] = time_match.group(1) + "'"
            
            return player_info
        except:
            return None

    def _extract_from_text_patterns(self, soup: BeautifulSoup) -> Dict:
        """Fallback: Extract teams from text patterns"""
        text = soup.get_text()
        teams = {}
        
        team_pattern = r'([A-Z][a-z]+[\s\w]*?)\s*\((\d+-\d+-\d+)\):\s*([^(]+?)(?=\n|\(|$)'
        
        team_matches = list(re.finditer(team_pattern, text))
        
        for i, match in enumerate(team_matches[:2]):
            team_name = match.group(1).strip()
            formation = match.group(2)
            players_text = match.group(3)
            
            players = [p.strip() for p in players_text.split(', ') if p.strip()]
            
            team_key = f'team_{i+1}' if i > 0 else 'team_1'
            teams[team_key] = {
                'name': team_name,
                'formation': formation,
                'starting_xi': [{'name': p, 'number': None, 'position': None} for p in players[:11]],
                'substitutes': [{'name': p, 'number': None, 'position': None} for p in players[11:15]],
                'bench': [{'name': p, 'number': None, 'position': None} for p in players[15:]]
            }
        
        return teams

    def save_to_json(self, data: Dict, filename: str = 'lineup_data_with_screenshots.json') -> None:
        """Save data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"💾 Data saved to {filename}")

    def print_lineup(self, data: Dict) -> None:
        """Pretty print the lineup data"""
        
        if 'match' in data:
            match = data['match']
            print(f"\n{'='*80}")
            print(f"MATCH: {match.get('home_team', 'Unknown'):20} vs {match.get('away_team', 'Unknown'):20}")
            print(f"{'='*80}")
            print(f"Score:       {match.get('result', 'N/A')}")
            print(f"Status:      {match.get('status', 'N/A')}")
            print(f"Competition: {match.get('competition', 'N/A')}")
            print(f"Date:        {match.get('date', 'N/A')}")
        
        # Print screenshot info
        if data.get('screenshots'):
            print(f"\n📸 Screenshots captured: {len(data['screenshots'])}")
            for screenshot in data['screenshots']:
                print(f"  - {screenshot}")
        
        # Print OCR data
        if data.get('ocr_extracted'):
            print(f"\n🔍 OCR Extracted Data:")
            for key, ocr_data in data['ocr_extracted'].items():
                print(f"  {key}:")
                print(f"    Players found: {len(ocr_data.get('players', []))}")
                if ocr_data.get('players'):
                    for player in ocr_data['players'][:5]:
                        print(f"      - {player}")
                print(f"    Teams found: {ocr_data.get('teams', [])}")
        
        # Print image analysis
        if data.get('image_analysis'):
            print(f"\n📊 Image Analysis:")
            for key, analysis in data['image_analysis'].items():
                print(f"  {key}:")
                print(f"    Contours found: {analysis.get('contours_found', 0)}")
                print(f"    Potential player cards: {analysis.get('potential_card_count', 0)}")
                print(f"    Has sufficient content: {analysis.get('has_sufficient_content', False)}")
        
        # Print parsed teams
        if data.get('teams'):
            print(f"\n{'='*80}")
            print("PARSED LINEUP DATA")
            print(f"{'='*80}")
            for team_key, team in data['teams'].items():
                if not team:
                    continue
                
                team_name = team.get('name', 'Unknown').upper()
                formation = team.get('formation', 'N/A')
                
                print(f"\n{team_name} ({formation})")
                print(f"{'Starting XI':^40}")
                if team.get('starting_xi'):
                    for j, player in enumerate(team['starting_xi'][:5], 1):
                        name = player.get('name', 'Unknown')
                        print(f"  {j}. {name}")
                print(f"  ... and {len(team.get('starting_xi', [])) - 5} more")


def main():
    """CLI interface"""
    if len(sys.argv) < 2:
        print("FotMob Lineup Scraper with Screenshots & OCR")
        print("\nUsage:")
        print("  python fotmob_scraper_screenshots.py <fotmob_url>")
        print("  python fotmob_scraper_screenshots.py --html <html_file>")
        print("\nExamples:")
        print("  python fotmob_scraper_screenshots.py https://www.fotmob.com/matches/bologna-vs-celtic/38i6ey")
        print("  python fotmob_scraper_screenshots.py --html match.html")
        sys.exit(1)
    
    scraper = FotMobLineupScreenshotScraper()
    
    try:
        if sys.argv[1] == '--html':
            if len(sys.argv) < 3:
                print("ERROR: HTML file path required")
                sys.exit(1)
            data = scraper.scrape_from_html_file(sys.argv[2])
        else:
            url = sys.argv[1]
            data = scraper.scrape_with_screenshots(url)
        
        scraper.print_lineup(data)
        scraper.save_to_json(data)
        
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
