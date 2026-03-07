import pandas as pd
import asyncio
import json
import os
from typing import Dict, List, Any
from playwright.async_api import async_playwright, Playwright
from urllib.parse import urlparse, parse_qs
from functools import reduce
from sqlalchemy import create_engine, text
from typing import Dict, Optional, List, Union
import asyncio
import json
from playwright.async_api import async_playwright


async def scrape_match_player_data(url):
    browsers = ['webkit']
    match_id = url.split('#')[-1].split(':')[0]
    async with async_playwright() as p:
        for browser_type in browsers:
            try:
                browser = await p[browser_type].launch()
                page = await browser.new_page()
                
                print("Loading page...")
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(1000)
                
                # Wait for and click Stats button
                print("Clicking Stats button...")
                stats_button = page.get_by_role("button", name="Stats", exact=True)
                await stats_button.wait_for(timeout=15000)
                await stats_button.click()
                await page.wait_for_timeout(3000)  # Wait longer for tab to switch
                
                # Scroll to find player stats section (it might be below the fold)
                print("Scrolling to find player stats section...")
                for i in range(5):
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 1.5);")
                    await page.wait_for_timeout(1000)
                    # Check if we can see player stats tables
                    tables = await page.locator("table").all()
                    if len(tables) > 0:
                        print(f"  Found {len(tables)} tables at scroll position {i+1}")
                        break
                
                # Wait for network to be idle (matchDetails might load via API)
                print("Waiting for network requests to complete...")
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    print("  Network is idle")
                except:
                    print("  Network idle timeout, continuing...")
                await page.wait_for_timeout(3000)  # Extra wait for dynamic content
                
                # Try to click "All stats" button/link to expand player stats
                print("Looking for 'All stats' button to expand player stats...")
                try:
                    # Try multiple ways to find "All stats"
                    all_stats_selectors = [
                        "button:has-text('All stats')",
                        "a:has-text('All stats')",
                        "text=All stats",
                        "[aria-label*='All stats']",
                        "//button[contains(text(), 'All stats')]",
                        "//a[contains(text(), 'All stats')]"
                    ]
                    
                    all_stats_clicked = False
                    for selector in all_stats_selectors:
                        try:
                            if selector.startswith("//"):
                                # XPath selector
                                element = page.locator(f"xpath={selector}")
                            else:
                                element = page.locator(selector)
                            
                            count = await element.count()
                            if count > 0:
                                print(f"Found 'All stats' with selector: {selector}")
                                await element.first.click()
                                await page.wait_for_timeout(3000)  # Wait longer for content to load
                                all_stats_clicked = True
                                break
                        except Exception as e:
                            continue
                    
                    if not all_stats_clicked:
                        print("Could not find 'All stats' button, continuing...")
                    else:
                        # After clicking "All stats", wait a bit more and check if player stats section appeared
                        print("Waiting for player stats section to load...")
                        await page.wait_for_timeout(2000)
                        
                        # Check if we can see any of the category buttons now
                        test_categories = ['Top stats', 'Attack', 'Passes']
                        found_any = False
                        for cat in test_categories:
                            try:
                                elem = page.locator(f"a:has-text('{cat}'), button:has-text('{cat}')")
                                if await elem.count() > 0:
                                    found_any = True
                                    print(f"  Confirmed: Found '{cat}' after clicking All stats")
                                    break
                            except:
                                pass
                        
                        if not found_any:
                            print("  Warning: Category buttons not immediately visible, will search more thoroughly...")
                        
                        # Wait for network again after clicking All stats (matchDetails might load now)
                        print("  Waiting for network after clicking All stats...")
                        try:
                            await page.wait_for_load_state("networkidle", timeout=10000)
                            print("  Network is idle after All stats")
                        except:
                            print("  Network idle timeout, continuing...")
                        await page.wait_for_timeout(2000)
                except Exception as e:
                    print(f"Error looking for 'All stats': {e}")
                
                # Scroll down to find player stats section with category buttons
                print("Scrolling to find player stats section...")
                categories_found_during_scroll = set()
                for i in range(10):  # Scroll more times
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 1.2);")
                    await page.wait_for_timeout(2000)  # Wait longer between scrolls
                    
                    # Check if we found the stats filter buttons
                    test_buttons = await page.locator("button[class*='FilterButton'], a").all()
                    for btn in test_buttons:
                        try:
                            text = await btn.evaluate("el => el.innerText || el.textContent")
                            if text:
                                text = text.strip()
                                for desired_cat in desired_categories:
                                    if desired_cat.lower() == text.lower() or text.lower().startswith(desired_cat.lower() + " "):
                                        if desired_cat not in categories_found_during_scroll:
                                            categories_found_during_scroll.add(desired_cat)
                                            print(f"  Found '{desired_cat}' button while scrolling (scroll {i+1})")
                        except:
                            pass
                    
                    # Also check all clickable elements
                    all_clickable = await page.locator("button, a").all()
                    for elem in all_clickable:
                        try:
                            text = await elem.evaluate("el => el.innerText || el.textContent")
                            if text:
                                text = text.strip()
                                for desired_cat in desired_categories:
                                    if desired_cat.lower() == text.lower():
                                        if desired_cat not in categories_found_during_scroll:
                                            categories_found_during_scroll.add(desired_cat)
                                            print(f"  Found '{desired_cat}' element while scrolling (scroll {i+1})")
                        except:
                            pass
                    
                    # If we found multiple categories, we're probably in the right area
                    if len(categories_found_during_scroll) >= 3:
                        print(f"  Found {len(categories_found_during_scroll)} categories, stopping scroll")
                        break
                
                print(f"  Categories found during scroll: {sorted(categories_found_during_scroll)}")
                
                # Wait a bit more for content to stabilize
                await page.wait_for_timeout(3000)
                
                # Find category selector - it's a SELECT dropdown, not buttons!
                print("Finding category selector dropdown...")
                category_select = None
                available_categories_from_select = []  # Initialize this variable
                desired_categories = ['Top stats', 'Attack', 'Passes', 'Defense', 'Duels', 'Goalkeeping']
                
                # Scroll to find player stats section and the select dropdown
                print("Scrolling to find player stats section with category selector...")
                for scroll_attempt in range(8):
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 0.8);")
                    await page.wait_for_timeout(1500)
                    
                    # Look for select dropdown near tables (player stats are in tables)
                    tables = await page.locator("table").all()
                    if len(tables) > 0:
                        print(f"  Found {len(tables)} tables at scroll {scroll_attempt + 1}, checking for select dropdown...")
                        
                        # Look for select elements near tables
                        for table in tables:
                            try:
                                # Get select elements that are siblings or parents of the table
                                # Try finding select before the table
                                select_before = await table.locator("xpath=preceding::select").all()
                                for sel in select_before[:5]:  # Check first 5 preceding selects
                                    options = await sel.locator("option").all()
                                    option_texts = []
                                    for opt in options:
                                        text = await opt.text_content()
                                        if text:
                                            option_texts.append(text.strip())
                                    
                                    # Check if it has our desired categories
                                    found_categories = [cat for cat in desired_categories if any(cat.lower() in txt.lower() for txt in option_texts)]
                                    if len(found_categories) >= 3:
                                        category_select = sel
                                        print(f"  ✓ Found category select near table: {option_texts}")
                                        # Scroll select into view
                                        await sel.scroll_into_view_if_needed()
                                        await page.wait_for_timeout(500)
                                        break
                                
                                if category_select:
                                    break
                                    
                                # Also check for select in parent containers
                                parent = await table.evaluate_handle("el => el.parentElement")
                                if parent:
                                    select_in_parent = await page.locator("select").all()
                                    for sel in select_in_parent:
                                        # Check if this select is near our table
                                        sel_box = await sel.bounding_box()
                                        table_box = await table.bounding_box()
                                        if sel_box and table_box:
                                            # If select is within 500px vertically of table, it's probably related
                                            if abs(sel_box['y'] - table_box['y']) < 500:
                                                options = await sel.locator("option").all()
                                                option_texts = []
                                                for opt in options:
                                                    text = await opt.text_content()
                                                    if text:
                                                        option_texts.append(text.strip())
                                                
                                                found_categories = [cat for cat in desired_categories if any(cat.lower() in txt.lower() for txt in option_texts)]
                                                if len(found_categories) >= 3:
                                                    category_select = sel
                                                    print(f"  ✓ Found category select near table (in parent): {option_texts}")
                                                    await sel.scroll_into_view_if_needed()
                                                    await page.wait_for_timeout(500)
                                                    break
                                
                                if category_select:
                                    break
                            except Exception as e:
                                continue
                        
                        if category_select:
                            break
                
                # If still not found, try all selects on page
                if category_select is None:
                    print("  Searching all select elements on page...")
                    try:
                        selects = await page.locator("select").all()
                        print(f"  Found {len(selects)} total select elements on page")
                        for sel in selects:
                            try:
                                # Scroll select into view
                                await sel.scroll_into_view_if_needed()
                                await page.wait_for_timeout(300)
                                
                                # Check if it has our category options
                                options = await sel.locator("option").all()
                                option_texts = []
                                for opt in options:
                                    text = await opt.text_content()
                                    if text:
                                        option_texts.append(text.strip())
                                
                                print(f"    Select has options: {option_texts}")
                                
                                # Check if it has our desired categories
                                found_categories = [cat for cat in desired_categories if any(cat.lower() in txt.lower() for txt in option_texts)]
                                if len(found_categories) >= 3:  # If it has at least 3 of our categories
                                    category_select = sel
                                    print(f"  ✓ Found category select with options: {option_texts}")
                                    break
                            except Exception as e:
                                continue
                    except Exception as e:
                        print(f"  Error searching selects: {e}")
                
                # Try by aria-label as fallback
                if category_select is None:
                    try:
                        category_select = page.locator("select[aria-label*='Selected'], select[aria-label*='Category'], select[aria-label*='Stats']")
                        count = await category_select.count()
                        if count > 0:
                            category_select = category_select.first
                            await category_select.scroll_into_view_if_needed()
                            print(f"  ✓ Found category select by aria-label")
                    except:
                        pass
                
                # If we found the select, we'll use it directly instead of finding buttons
                if category_select is not None:
                    try:
                        count = await category_select.count()
                    except:
                        count = 0
                    
                    if count > 0:
                        print(f"  ✓ Found category selector dropdown!")
                        # Get available options
                        options = await category_select.locator("option").all()
                        for opt in options:
                            text = await opt.text_content()
                            if text:
                                text = text.strip()
                                # Map option values to category names
                                if text.lower() in ['top stats', 'attack', 'passes', 'defense', 'duels', 'goalkeeping']:
                                    # Normalize to our desired format
                                    if text.lower() == 'top stats':
                                        available_categories_from_select.append('Top stats')
                                    elif text.lower() == 'attack':
                                        available_categories_from_select.append('Attack')
                                    elif text.lower() == 'passes':
                                        available_categories_from_select.append('Passes')
                                    elif text.lower() == 'defense':
                                        available_categories_from_select.append('Defense')
                                    elif text.lower() == 'duels':
                                        available_categories_from_select.append('Duels')
                                    elif text.lower() == 'goalkeeping':
                                        available_categories_from_select.append('Goalkeeping')
                        
                        print(f"  Available categories in dropdown: {available_categories_from_select}")
                    else:
                        print("  ✗ Category select found but count is 0")
                        category_select = None
                else:
                    print("  ✗ Could not find category selector dropdown, falling back to button search...")
                    category_select = None
                
                # Find filter buttons - look specifically in stats area (fallback if no select found)
                print("Finding filter buttons (fallback)...")
                filter_buttons = []
                
                # Strategy 1: Look for elements (buttons OR links) with specific text content
                print("Strategy 1: Searching for elements by text content...")
                for category in desired_categories:
                    # Try as button (exact match)
                    try:
                        btn = page.get_by_role("button", name=category, exact=True)
                        count = await btn.count()
                        if count > 0:
                            filter_buttons.append(btn.first)
                            print(f"  Found '{category}' as button (exact)")
                    except:
                        pass
                    
                    # Try as link (exact match)
                    try:
                        link = page.get_by_role("link", name=category, exact=True)
                        count = await link.count()
                        if count > 0:
                            filter_buttons.append(link.first)
                            print(f"  Found '{category}' as link (exact)")
                    except:
                        pass
                    
                    # Try using locator with text (contains match)
                    try:
                        # Look for links/buttons that contain the category text
                        element = page.locator(f"a:has-text('{category}'), button:has-text('{category}')")
                        count = await element.count()
                        if count > 0:
                            # Get the first one and verify it's actually clickable
                            first_elem = element.first
                            tag_name = await first_elem.evaluate("el => el.tagName.toLowerCase()")
                            if tag_name in ['button', 'a']:
                                # Verify the text actually contains our category
                                text = await first_elem.evaluate("el => el.innerText || el.textContent")
                                if text and category.lower() in text.lower():
                                    filter_buttons.append(first_elem)
                                    print(f"  Found '{category}' as {tag_name} (contains match)")
                    except Exception as e:
                        pass
                    
                    # Try XPath for more precise matching
                    try:
                        xpath = f"//a[contains(text(), '{category}')] | //button[contains(text(), '{category}')]"
                        elements = page.locator(f"xpath={xpath}")
                        count = await elements.count()
                        if count > 0:
                            for i in range(count):
                                elem = elements.nth(i)
                                text = await elem.evaluate("el => el.innerText || el.textContent")
                                # Make sure it's actually the category, not just containing it
                                if text and category.lower() == text.strip().lower():
                                    if elem not in filter_buttons:
                                        filter_buttons.append(elem)
                                        print(f"  Found '{category}' via XPath")
                                        break
                    except:
                        pass
                
                # Strategy 2: Original selector (class-based)
                if len(filter_buttons) < len(desired_categories):
                    print("Strategy 2: Searching by FilterButton class...")
                    class_buttons = await page.locator("button[class*='FilterButton']").all()
                    print(f"  Found {len(class_buttons)} buttons with FilterButton class")
                    
                    # Debug: Show what these buttons contain
                    for i, btn in enumerate(class_buttons):
                        try:
                            text = await btn.evaluate("el => el.innerText || el.textContent")
                            if text:
                                print(f"    FilterButton[{i}]: '{text.strip()[:50]}'")
                        except:
                            pass
                    
                    for btn in class_buttons:
                        try:
                            text = await btn.evaluate("el => el.innerText || el.textContent")
                            if text:
                                text = text.strip()
                                for desired_cat in desired_categories:
                                    # More precise matching
                                    if desired_cat.lower() == text.lower() or text.lower().startswith(desired_cat.lower() + " "):
                                        # Check if already added
                                        is_duplicate = False
                                        for existing in filter_buttons:
                                            try:
                                                existing_text = await existing.evaluate("el => el.innerText || el.textContent")
                                                if existing_text:
                                                    existing_text = existing_text.strip().lower()
                                                    if (desired_cat.lower() == existing_text or 
                                                        desired_cat.lower() in existing_text):
                                                        is_duplicate = True
                                                        break
                                            except:
                                                pass
                                        
                                        if not is_duplicate:
                                            filter_buttons.append(btn)
                                            print(f"  Found '{desired_cat}' via FilterButton class")
                                        break
                        except:
                            pass
                
                # Strategy 3: Look for all clickable elements and filter by text
                if len(filter_buttons) < len(desired_categories):
                    print("Strategy 3: Searching all clickable elements...")
                    all_clickable = await page.locator("button, a").all()
                    print(f"  Checking {len(all_clickable)} clickable elements...")
                    
                    # Debug: Show some sample texts to understand what we're seeing
                    sample_texts = []
                    for i, elem in enumerate(all_clickable[:50]):  # Check first 50
                        try:
                            text = await elem.evaluate("el => el.innerText || el.textContent")
                            if text and text.strip():
                                text = text.strip()
                                # Check if it contains any of our desired categories
                                for desired_cat in desired_categories:
                                    if desired_cat.lower() in text.lower():
                                        sample_texts.append(f"  [{i}] '{text[:50]}' -> matches '{desired_cat}'")
                                        break
                        except:
                            pass
                    
                    if sample_texts:
                        print(f"  Sample matches found:")
                        for sample in sample_texts[:10]:  # Show first 10
                            print(sample)
                    
                    # Now actually collect the buttons
                    for elem in all_clickable:
                        try:
                            # Try innerText first (more accurate)
                            text = await elem.evaluate("el => el.innerText || el.textContent")
                            if text:
                                text = text.strip()
                                # Check if text matches or contains any desired category
                                for desired_cat in desired_categories:
                                    # More precise matching: category should be a word boundary match
                                    # or exact match (to avoid matching "Attack" in "Attacker")
                                    text_lower = text.lower()
                                    cat_lower = desired_cat.lower()
                                    
                                    # Exact match
                                    if cat_lower == text_lower:
                                        match = True
                                    # Category is at the start of text (likely the button label)
                                    elif text_lower.startswith(cat_lower + " ") or text_lower.startswith(cat_lower + "\n"):
                                        match = True
                                    # Category is standalone word in text (word boundary)
                                    elif f" {cat_lower} " in f" {text_lower} " or f" {cat_lower}\n" in f" {text_lower}\n":
                                        match = True
                                    # For "Top stats", check if text starts with it
                                    elif desired_cat == "Top stats" and text_lower.startswith("top stats"):
                                        match = True
                                    else:
                                        match = False
                                    
                                    if match:
                                        # Make sure we don't add duplicates
                                        is_duplicate = False
                                        for existing in filter_buttons:
                                            try:
                                                existing_text = await existing.evaluate("el => el.innerText || el.textContent")
                                                if existing_text:
                                                    existing_text = existing_text.strip().lower()
                                                    # Check if they match the same category
                                                    if (cat_lower == existing_text or 
                                                        cat_lower in existing_text or 
                                                        existing_text in cat_lower):
                                                        is_duplicate = True
                                                        break
                                            except:
                                                pass
                                        
                                        if not is_duplicate:
                                            filter_buttons.append(elem)
                                            print(f"  Found '{desired_cat}' as clickable element (text: '{text[:40]}')")
                                        break
                        except Exception as e:
                            pass
                
                # Strategy 4: Look for a container with all filter buttons (they're usually grouped)
                if len(filter_buttons) < len(desired_categories):
                    print("Strategy 4: Looking for filter button container...")
                    # Try to find a container that has multiple category buttons
                    # Common patterns: nav, div with class containing 'filter', 'tab', 'category', 'stats'
                    container_selectors = [
                        "nav",
                        "[class*='filter']",
                        "[class*='tab']",
                        "[class*='category']",
                        "[class*='stats']",
                        "[role='tablist']",
                        "[role='navigation']"
                    ]
                    
                    for selector in container_selectors:
                        try:
                            containers = await page.locator(selector).all()
                            for container in containers:
                                # Check if this container has multiple desired category buttons
                                buttons_in_container = await container.locator("button, a").all()
                                found_in_container = 0
                                for btn in buttons_in_container:
                                    try:
                                        text = await btn.evaluate("el => el.innerText || el.textContent")
                                        if text:
                                            for desired_cat in desired_categories:
                                                if desired_cat.lower() in text.lower():
                                                    # Check if already added
                                                    is_duplicate = False
                                                    for existing in filter_buttons:
                                                        try:
                                                            existing_text = await existing.evaluate("el => el.innerText || el.textContent")
                                                            if existing_text and desired_cat.lower() in existing_text.lower():
                                                                is_duplicate = True
                                                                break
                                                        except:
                                                            pass
                                                    
                                                    if not is_duplicate:
                                                        filter_buttons.append(btn)
                                                        found_in_container += 1
                                                        print(f"  Found '{desired_cat}' in container ({selector})")
                                                    break
                                    except:
                                        pass
                                
                                if found_in_container > 0:
                                    print(f"  Found {found_in_container} categories in container")
                        except:
                            pass
                
                # Strategy 5: Look near tables (stats are usually in tables)
                if len(filter_buttons) < len(desired_categories):
                    print("Strategy 5: Searching near tables...")
                    tables = await page.locator("table").all()
                    if len(tables) > 0:
                        print(f"  Found {len(tables)} tables")
                        # Look for buttons/links before tables (filters are usually above tables)
                        for table in tables:
                            try:
                                # Get elements before this table
                                before_table = await table.locator("xpath=preceding::button | preceding::a").all()
                                for elem in before_table[:20]:  # Check first 20 preceding elements
                                    try:
                                        text = await elem.evaluate("el => el.innerText || el.textContent")
                                        if text:
                                            for desired_cat in desired_categories:
                                                if desired_cat.lower() in text.lower():
                                                    # Check if already added
                                                    is_duplicate = False
                                                    for existing in filter_buttons:
                                                        try:
                                                            existing_text = await existing.evaluate("el => el.innerText || el.textContent")
                                                            if existing_text and desired_cat.lower() in existing_text.lower():
                                                                is_duplicate = True
                                                                break
                                                        except:
                                                            pass
                                                    
                                                    if not is_duplicate:
                                                        filter_buttons.append(elem)
                                                        print(f"  Found '{desired_cat}' before table")
                                                    break
                                    except:
                                        pass
                            except:
                                pass
                
                print(f"\nFound {len(filter_buttons)} filter buttons/elements")
                
                # Final pass: After all scrolling, do one more comprehensive search for category buttons
                if len(filter_buttons) < len(desired_categories):
                    print("\nFinal pass: Comprehensive search for all category buttons...")
                    # Scroll to top of stats section and search again
                    await page.evaluate("window.scrollTo(0, 0);")
                    await page.wait_for_timeout(1000)
                    
                    # Scroll down slowly and check for buttons
                    for scroll_pos in range(0, 5000, 500):
                        await page.evaluate(f"window.scrollTo(0, {scroll_pos});")
                        await page.wait_for_timeout(1000)
                        
                        # Search for all category buttons/links
                        for category in desired_categories:
                            # Check if we already have this category
                            already_found = False
                            for existing in filter_buttons:
                                try:
                                    existing_text = await existing.evaluate("el => el.innerText || el.textContent")
                                    if existing_text and category.lower() in existing_text.lower():
                                        already_found = True
                                        break
                                except:
                                    pass
                            
                            if not already_found:
                                # Try to find this specific category
                                try:
                                    # Try as link first (they seem to be links)
                                    elem = page.locator(f"a:has-text('{category}')")
                                    count = await elem.count()
                                    if count > 0:
                                        # Verify it's actually the category
                                        text = await elem.first.evaluate("el => el.innerText || el.textContent")
                                        if text and category.lower() in text.lower():
                                            filter_buttons.append(elem.first)
                                            print(f"  Found '{category}' in final pass")
                                except:
                                    pass
                    
                    # Scroll back to where stats tables should be
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 3);")
                    await page.wait_for_timeout(1000)
                
                print(f"\nFinal count: {len(filter_buttons)} filter buttons/elements found")
                
                # Get category names from buttons/elements
                all_categories = []
                for btn in filter_buttons:
                    try:
                        # Try to get just the direct text, not nested content
                        text = await btn.evaluate("el => el.innerText || el.textContent")
                        if text:
                            text = text.strip()
                            # Check if the text contains one of our desired categories
                            # (in case there's extra text)
                            for desired_cat in desired_categories:
                                if desired_cat.lower() in text.lower():
                                    if desired_cat not in all_categories:
                                        all_categories.append(desired_cat)
                                        print(f"  Extracted category: {desired_cat} from text: {text[:50]}")
                                    break
                            # If no match found, add the text as-is (might be useful for debugging)
                            if not any(desired_cat.lower() in text.lower() for desired_cat in desired_categories):
                                # Try to get just the first word or check if it's a short text
                                if len(text.split()) <= 3:  # Short text might be the category
                                    all_categories.append(text)
                    except Exception as e:
                        print(f"  Error extracting text from element: {e}")
                        continue
                
                print(f"\nAll categories found: {all_categories}")
                
                # Debug: Print all button texts if we didn't find the right ones
                if not any(cat in ['Top stats', 'Attack', 'Passes', 'Defense', 'Duels', 'Goalkeeping'] for cat in all_categories):
                    print("Warning: Expected categories not found. Checking all buttons on page...")
                    all_page_buttons = await page.locator("button").all()
                    button_texts = []
                    for btn in all_page_buttons:
                        try:
                            text = await btn.text_content()
                            if text and text.strip():
                                button_texts.append(text.strip())
                        except:
                            pass
                    print(f"Total buttons found: {len(button_texts)}")
                    print(f"All button texts: {button_texts}")
                    
                    # Also check for links that might be the filter buttons
                    print("\nChecking for links that might be filter buttons...")
                    all_links = await page.locator("a").all()
                    link_texts = []
                    for link in all_links[:50]:  # Check first 50 links
                        try:
                            text = await link.text_content()
                            if text and text.strip() in ['Top stats', 'Attack', 'Passes', 'Defense', 'Duels', 'Goalkeeping']:
                                link_texts.append(text.strip())
                        except:
                            pass
                    if link_texts:
                        print(f"Found potential filter links: {link_texts}")
                    
                    # Check page HTML for stats-related content
                    print("\nChecking page content for stats keywords...")
                    page_content = await page.content()
                    if 'Top stats' in page_content:
                        print("Found 'Top stats' in page HTML")
                    if 'Attack' in page_content:
                        print("Found 'Attack' in page HTML")
                    if 'Passes' in page_content:
                        print("Found 'Passes' in page HTML")
                
                # Determine which categories to scrape
                # If we found the select dropdown, use those categories
                if available_categories_from_select:
                    categories = available_categories_from_select
                    print(f"Using categories from dropdown: {categories}\n")
                else:
                    # Fallback: Filter to only desired categories from buttons
                    desired_categories = ['Top stats', 'Attack', 'Passes', 'Defense', 'Duels', 'Goalkeeping']
                    categories = [cat for cat in all_categories if cat in desired_categories]
                    
                    # If we didn't find all categories, try one more time with a different approach
                    if len(categories) < len(desired_categories):
                        print(f"\nOnly found {len(categories)}/{len(desired_categories)} categories. Trying alternative search...")
                        # Scroll to top and search again
                        await page.evaluate("window.scrollTo(0, 0);")
                        await page.wait_for_timeout(2000)
                        
                        # Look for links/buttons that are siblings or in the same container
                        for category in desired_categories:
                            if category not in categories:
                                # Try to find it using multiple methods
                                try:
                                    # Method 1: Direct text search
                                    elem = page.locator(f"a:has-text('{category}'), button:has-text('{category}')")
                                    count = await elem.count()
                                    if count > 0:
                                        # Verify it's actually clickable and contains the category
                                        for i in range(count):
                                            candidate = elem.nth(i)
                                            text = await candidate.evaluate("el => el.innerText || el.textContent")
                                            if text and category.lower() in text.lower():
                                                # Check if it's a short text (likely a category button)
                                                if len(text.strip().split()) <= 2:
                                                    categories.append(category)
                                                    print(f"  Found '{category}' via alternative search")
                                                    break
                                except:
                                    pass
                    
                    print(f"Will scrape: {categories}\n")
                
                # Dictionary to store all data
                all_categories_data = {}
                
                # Iterate through each category
                for cat_index, category in enumerate(categories):
                    print(f"[{cat_index + 1}/{len(categories)}] Scraping {category}...", end=" ", flush=True)
                    
                    try:
                        # If we have a select dropdown, use it
                        if category_select is not None:
                            try:
                                # Ensure select is visible and in view
                                await category_select.scroll_into_view_if_needed()
                                await page.wait_for_timeout(500)
                                
                                count = await category_select.count()
                                if count > 0:
                                    # Get all options first to find the right one
                                    options = await category_select.locator("option").all()
                                    option_found = False
                                    
                                    for idx, opt in enumerate(options):
                                        text = await opt.text_content()
                                        if text:
                                            text = text.strip()
                                            # Check if this option matches our category
                                            if category.lower() in text.lower() or text.lower() in category.lower():
                                                # Try selecting by index (most reliable)
                                                try:
                                                    await category_select.select_option(index=idx)
                                                    print(f"  Selected '{category}' (option: '{text}')", end=" ")
                                                    await page.wait_for_timeout(3000)  # Wait longer for table to update
                                                    option_found = True
                                                    break
                                                except Exception as e1:
                                                    # Try by value attribute
                                                    try:
                                                        value = await opt.get_attribute("value")
                                                        if value:
                                                            await category_select.select_option(value=value)
                                                            print(f"  Selected '{category}' by value", end=" ")
                                                            await page.wait_for_timeout(3000)
                                                            option_found = True
                                                            break
                                                    except Exception as e2:
                                                        # Try by label
                                                        try:
                                                            await category_select.select_option(label=text)
                                                            print(f"  Selected '{category}' by label", end=" ")
                                                            await page.wait_for_timeout(3000)
                                                            option_found = True
                                                            break
                                                        except Exception as e3:
                                                            continue
                                    
                                    if not option_found:
                                        print(f"✗ Could not find option for '{category}'")
                                        continue
                                else:
                                    # Select element not found, fall through to button method
                                    category_select = None
                            except Exception as e:
                                print(f"✗ Error accessing select: {e}")
                                category_select = None
                        
                        # Fallback: Use button/link method if no select dropdown
                        if category_select is None or (category_select is not None and await category_select.count() == 0):
                            # Fallback: Find the button/link for this category
                            button = None
                            
                            # Try multiple ways to find it
                            # Method 1: Use get_by_role
                            try:
                                btn = page.get_by_role("button", name=category, exact=False)
                                if await btn.count() > 0:
                                    button = btn.first
                            except:
                                pass
                            
                            if not button:
                                try:
                                    link = page.get_by_role("link", name=category, exact=False)
                                    if await link.count() > 0:
                                        button = link.first
                                except:
                                    pass
                            
                            # Method 2: Use text locator
                            if not button:
                                try:
                                    elem = page.locator(f"text={category}").first
                                    tag_name = await elem.evaluate("el => el.tagName.toLowerCase()")
                                    if tag_name in ['button', 'a']:
                                        button = elem
                                except:
                                    pass
                            
                            # Method 3: Search all clickable elements
                            if not button:
                                all_clickable = await page.locator("button, a").all()
                                for elem in all_clickable:
                                    try:
                                        text = await elem.text_content()
                                        if text and text.strip() == category:
                                            button = elem
                                            break
                                    except:
                                        pass
                            
                            if not button:
                                print(f"✗ Button/link not found")
                                continue
                            
                            # Click the button/link
                            await button.click()
                            await page.wait_for_timeout(1800)
                        
                        # Wait for table to load
                        await page.wait_for_timeout(1800)
                        
                        # Find tables for this category
                        # Wait for tables to load after selecting category
                        await page.wait_for_timeout(2000)
                        
                        # Scroll to ensure tables are visible
                        await page.evaluate("window.scrollBy(0, 300);")
                        await page.wait_for_timeout(1000)
                        
                        # Try to wait for tables to appear - use the specific class if possible
                        try:
                            # Try waiting for the styled table class
                            await page.wait_for_selector("table.css-fsgm2u-StyledTable, table", timeout=5000)
                        except:
                            pass
                        
                        # Look for tables with the specific class first
                        tables = await page.locator("table.css-fsgm2u-StyledTable").all()
                        if len(tables) == 0:
                            # Fall back to all tables
                            tables = await page.locator("table").all()
                        
                        # Filter to only player stats tables (they should have multiple rows with player data)
                        player_stats_tables = []
                        for table in tables:
                            try:
                                rows = await table.locator("tbody tr, tr").all()
                                # Filter out header rows
                                data_rows = []
                                for row in rows:
                                    th_count = await row.locator("th").count()
                                    if th_count == 0:  # Not a header row
                                        data_rows.append(row)
                                
                                # Player stats tables should have multiple data rows (at least 2 players)
                                if len(data_rows) >= 2:
                                    player_stats_tables.append(table)
                            except:
                                pass
                        
                        # Use player stats tables if found, otherwise use all tables
                        if len(player_stats_tables) > 0:
                            tables = player_stats_tables
                        
                        print(f"  Found {len(tables)} tables for {category}")
                        
                        if len(tables) == 0:
                            print(f"✗ No tables")
                            # Try scrolling to see if tables appear
                            await page.evaluate("window.scrollBy(0, 500);")
                            await page.wait_for_timeout(1000)
                            tables = await page.locator("table").all()
                            if len(tables) > 0:
                                print(f"  Found {len(tables)} tables after scrolling")
                            else:
                                continue
                        
                        category_tables = []
                        
                        for i, table in enumerate(tables):
                            # Scroll table into view to ensure it's loaded and all rows are rendered
                            try:
                                await table.scroll_into_view_if_needed()
                                await page.wait_for_timeout(1000)  # Wait longer for lazy-loaded content
                                
                                # Scroll within the table to ensure all rows are loaded (if table is scrollable)
                                try:
                                    table_height = await table.evaluate("el => el.scrollHeight")
                                    viewport_height = await page.evaluate("window.innerHeight")
                                    if table_height > viewport_height:
                                        # Table is scrollable, scroll to bottom to load all rows
                                        await table.evaluate("el => el.scrollTop = el.scrollHeight")
                                        await page.wait_for_timeout(1000)
                                        await table.evaluate("el => el.scrollTop = 0")  # Scroll back to top
                                        await page.wait_for_timeout(500)
                                except:
                                    pass
                            except:
                                pass
                            
                            # Get headers - try multiple selectors
                            headers = []
                            try:
                                # Try the specific header cell class
                                headers = await table.locator("thead th.css-7c7pzk-HeaderCell, thead th").all()
                            except:
                                pass
                            
                            if len(headers) == 0:
                                # Try alternative header selectors
                                try:
                                    headers = await table.locator("th").all()
                                except:
                                    pass
                            
                            header_texts = []
                            for h in headers:
                                try:
                                    text = await h.text_content()
                                    if text:
                                        header_texts.append(text.strip())
                                except:
                                    pass
                            
                            # Get rows - try multiple selectors
                            rows = []
                            try:
                                # Try the specific row class
                                rows = await table.locator("tbody tr.css-1ypg8l2-TableRowStyled, tbody tr").all()
                            except:
                                pass
                            
                            if len(rows) == 0:
                                # Try without specific class
                                try:
                                    rows = await table.locator("tbody tr").all()
                                except:
                                    pass
                            
                            if len(rows) == 0:
                                # Try without tbody
                                try:
                                    rows = await table.locator("tr").all()
                                except:
                                    pass
                            
                            # Filter out header rows (rows with th elements)
                            data_rows = []
                            for row in rows:
                                try:
                                    th_count = await row.locator("th").count()
                                    if th_count == 0:  # Not a header row
                                        data_rows.append(row)
                                except:
                                    pass
                            
                            rows = data_rows
                            
                            print(f"    Table {i+1}: {len(header_texts)} headers, {len(rows)} data rows")
                            
                            table_data = {
                                'table_number': i,
                                'headers': header_texts,
                                'rows': []
                            }
                            
                            for j, row in enumerate(rows):
                                try:
                                    # Scroll row into view to ensure it's rendered (for lazy-loaded tables)
                                    try:
                                        await row.scroll_into_view_if_needed()
                                        await page.wait_for_timeout(100)  # Small wait for rendering
                                    except:
                                        pass
                                    
                                    # Get all cells in the row - try specific class first
                                    cells = await row.locator("td.css-eca6ih-TableCell, td").all()
                                    
                                    # Skip if no cells (might be a header row)
                                    if len(cells) == 0:
                                        continue
                                    
                                    row_data = []
                                    
                                    for k, cell in enumerate(cells):
                                        try:
                                            # Get text from cell - try multiple methods
                                            cell_text_value = None
                                            
                                            # Method 1: Get text from innerText (most reliable, excludes hidden text)
                                            try:
                                                cell_text_value = await cell.evaluate("el => el.innerText")
                                                if cell_text_value:
                                                    cell_text_value = cell_text_value.strip()
                                            except:
                                                pass
                                            
                                            # Method 2: If innerText is empty, try textContent
                                            if not cell_text_value:
                                                try:
                                                    cell_text_value = await cell.text_content()
                                                    if cell_text_value:
                                                        cell_text_value = cell_text_value.strip()
                                                        # Clean up whitespace
                                                        cell_text_value = " ".join(cell_text_value.split())
                                                except:
                                                    pass
                                            
                                            # Method 3: For player names, try getting from link
                                            if not cell_text_value or len(cell_text_value) < 2:
                                                link_in_cell = await cell.locator("a").count()
                                                if link_in_cell > 0:
                                                    try:
                                                        link = cell.locator("a").first
                                                        # Try innerText of link first
                                                        link_text = await link.evaluate("el => el.innerText")
                                                        if link_text:
                                                            cell_text_value = link_text.strip()
                                                        else:
                                                            # Try getting from player name span
                                                            player_name_elem = link.locator(".css-j5c6be-PlayerNameCSS, span")
                                                            if await player_name_elem.count() > 0:
                                                                name_text = await player_name_elem.first.text_content()
                                                                if name_text:
                                                                    cell_text_value = name_text.strip()
                                                    except:
                                                        pass
                                            
                                            # Method 4: Try getting from span elements (for stat values)
                                            if not cell_text_value:
                                                try:
                                                    spans = await cell.locator("span").all()
                                                    if len(spans) > 0:
                                                        # Get text from all spans and join
                                                        span_texts = []
                                                        for span in spans:
                                                            span_text = await span.text_content()
                                                            if span_text:
                                                                span_texts.append(span_text.strip())
                                                        if span_texts:
                                                            cell_text_value = " ".join(span_texts)
                                                except:
                                                    pass
                                            
                                            # Final fallback: use empty string
                                            if not cell_text_value:
                                                cell_text_value = ""
                                            
                                            # Check for links (player links)
                                            cell_data = {
                                                'text': cell_text_value,
                                                'has_link': False,
                                                'url': None
                                            }
                                            
                                            # Try to get link
                                            try:
                                                link_count = await cell.locator("a").count()
                                                
                                                if link_count > 0:
                                                    link = cell.locator("a").first
                                                    href = await asyncio.wait_for(
                                                        link.get_attribute("href"),
                                                        timeout=0.8
                                                    )
                                                    
                                                    if href:
                                                        cell_data['url'] = href
                                                        cell_data['has_link'] = True
                                            except asyncio.TimeoutError:
                                                pass
                                            except Exception:
                                                pass
                                            
                                            row_data.append(cell_data)
                                        except Exception as cell_err:
                                            # If cell extraction fails, add placeholder
                                            row_data.append({
                                                'text': 'ERROR',
                                                'has_link': False,
                                                'url': None
                                            })
                                    
                                    if row_data:
                                        table_data['rows'].append(row_data)
                                
                                except Exception as row_err:
                                    continue
                            
                            category_tables.append(table_data)
                        
                        all_categories_data[category] = category_tables
                        total_rows = sum(len(t['rows']) for t in category_tables)
                        
                        # Debug: Show what we found
                        if total_rows > 0:
                            print(f"✓ {total_rows} rows")
                            # Show first and last rows as samples
                            if len(category_tables) > 0 and len(category_tables[0]['rows']) > 0:
                                first_row = category_tables[0]['rows'][0]
                                last_row = category_tables[0]['rows'][-1]
                                first_row_texts = [cell.get('text', '')[:20] for cell in first_row[:3]]
                                last_row_texts = [cell.get('text', '')[:20] for cell in last_row[:3]]
                                print(f"    First row: {first_row_texts}")
                                print(f"    Last row: {last_row_texts}")
                                
                                # Count unique players (assuming first column is player name)
                                if len(first_row) > 0:
                                    player_names = set()
                                    for table in category_tables:
                                        for row in table['rows']:
                                            if len(row) > 0:
                                                player_name = row[0].get('text', '').strip()
                                                if player_name:
                                                    player_names.add(player_name)
                                    print(f"    Unique players found: {len(player_names)}")
                        else:
                            print(f"✗ No rows found")
                            # Try scrolling more
                            await page.evaluate("window.scrollBy(0, 1000);")
                            await page.wait_for_timeout(2000)
                            # Try finding tables again
                            tables_retry = await page.locator("table").all()
                            if len(tables_retry) > len(tables):
                                print(f"    Found {len(tables_retry)} tables after additional scroll")
                                # Re-scrape this category
                                # (We'll add this data to category_tables)
                    
                    except Exception as e:
                        print(f"✗ {str(e)[:50]}")
                        all_categories_data[category] = []
                        continue
                
                # Save all data to JSON
                print(f"\nSaving to file...")
                # Create raw_data directory if it doesn't exist
                os.makedirs("raw_data", exist_ok=True)
                with open(f"raw_data/player_stats_{browser_type}_{match_id}.json", "w") as f:
                    json.dump(all_categories_data, f, indent=2)
                
                print(f"✓ Saved to raw_data/player_stats_{browser_type}_{match_id}.json")
                
                # Print summary
                total_all_rows = sum(
                    len(table['rows']) 
                    for category_tables in all_categories_data.values() 
                    for table in category_tables
                )
                categories_completed = len([c for c in all_categories_data if all_categories_data[c]])
                print(f"✓ Total: {total_all_rows} rows across {categories_completed}/{len(categories)} categories")
                
            except Exception as e:
                print(f"Fatal error: {e}")
                import traceback
                traceback.print_exc()
            finally:
                try:
                    await browser.close()
                except:
                    pass

def split_fraction_column(df, col):
    extracted = df[col].str.extract(r"(?P<success>\d+)/(?P<attempts>\d+)")
    
    df[f"{col}_success"] = pd.to_numeric(extracted["success"], errors="coerce")
    df[f"{col}_attempts"] = pd.to_numeric(extracted["attempts"], errors="coerce")
    
    df.drop(columns=col, inplace=True)


def extract_ids(url):
    parsed = urlparse(url)

    # match id: last path segment
    match_id = parsed.path.rstrip("/").split("/")[-1]

    # player id: query param
    player_id = parse_qs(parsed.query).get("player", [None])[0]

    return match_id, player_id


