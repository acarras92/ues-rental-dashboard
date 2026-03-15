#!/usr/bin/env python3
"""
Scrape Compass for UES rental listings matching:
  - 3+ bedrooms, $12,000-$18,000/month
  - 67th to 95th St, west of 3rd Ave (plus East End Ave)
  - Doorman buildings only

Uses Playwright to bypass AWS WAF bot protection.
Outputs listings.json matching the dashboard schema.

Usage:
    python scrape_compass.py                # default: 3+ BR, $12K-$18K, doorman
    python scrape_compass.py --min-beds 2   # include 2+ BR
    python scrape_compass.py --no-doorman   # skip doorman filter
    python scrape_compass.py --merge        # merge with existing listings.json
    python scrape_compass.py --dry-run      # scrape but don't write file
"""

import argparse
import json
import re
import time
import random
import sys
from datetime import datetime, date
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright is required.")
    print("Install with: pip install playwright && python -m playwright install chromium")
    sys.exit(1)


# --- Configuration ---

BASE_URL = "https://www.compass.com"
OUTPUT_FILE = Path(__file__).parent / "listings.json"

# UES geographic bounds: 67th to 95th St, west of 3rd Ave (lng ~ -73.956)
# East End Ave is the eastern boundary at ~-73.943
# We use a bounding box that covers this area
UES_BOUNDS = {
    "lat_min": 40.7649,   # ~67th St
    "lat_max": 40.7843,   # ~95th St
    "lng_min": -73.9680,  # ~west of Lexington/Park Ave (generous west bound)
    "lng_max": -73.9430,  # ~East End Ave
}

# Rate limiting: random delay between requests (seconds)
DELAY_MIN = 1.0
DELAY_MAX = 2.5

# How many results per search page on Compass
RESULTS_PER_PAGE = 41


def rate_limit():
    """Sleep a random duration for polite scraping."""
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    time.sleep(delay)


def parse_int(s):
    """Parse a string like '1,505' or '3' into an int, or return None."""
    if not s or s in ('-', 'Unavailable', '—'):
        return None
    try:
        return int(s.replace(',', ''))
    except (ValueError, AttributeError):
        return None


def parse_float(s):
    """Parse a string like '2.5' into a float, or return None."""
    if not s or s in ('-', 'Unavailable', '—'):
        return None
    try:
        return float(s.replace(',', ''))
    except (ValueError, AttributeError):
        return None


def parse_price(s):
    """Parse '$12,000' into 12000."""
    if not s:
        return None
    m = re.search(r'\$?([\d,]+)', s)
    if m:
        return parse_int(m.group(1))
    return None


def is_in_ues_bounds(lat, lng):
    """Check if coordinates fall within UES target area."""
    if lat is None or lng is None:
        return False
    return (UES_BOUNDS["lat_min"] <= lat <= UES_BOUNDS["lat_max"] and
            UES_BOUNDS["lng_min"] <= lng <= UES_BOUNDS["lng_max"])


def extract_street_number(address):
    """Extract the street number (e.g., 87 from '170 East 87th Street')."""
    m = re.search(r'(?:East|E\.?)\s+(\d+)', address, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def is_ues_address(address):
    """Check if address is on the UES between 67th and 95th, east side.

    For East-side cross streets, we check the street number directly.
    For north-south avenues, we rely on lat/lng verification in Phase 3
    (the detail page provides exact coordinates). Here we just flag avenues
    that run through UES as potential candidates.
    """
    if not address:
        return False
    addr_lower = address.lower()
    # Check for East side streets (e.g. "200 East 79th Street")
    if 'east' in addr_lower or ' e ' in addr_lower or ' e.' in addr_lower:
        st_num = extract_street_number(address)
        if st_num and 67 <= st_num <= 95:
            return True
    # Avenues that run through UES — these will be verified by lat/lng in Phase 3.
    # We require a building number that could plausibly be in the UES range.
    # Park Ave UES: ~500-1100, 5th Ave UES: ~800-1100, Madison: ~800-1400, Lex: ~700-1400
    ues_avenues = {
        'park ave': (400, 1200),
        'madison ave': (700, 1500),
        'lexington ave': (600, 1500),
        '3rd ave': (1100, 1800),
        '2nd ave': (1200, 1800),
        '1st ave': (1200, 1800),
        'york ave': (1200, 1700),
        'east end ave': (1, 999),
        'fifth ave': (700, 1200),
        '5th ave': (700, 1200),
    }
    for ave, (num_lo, num_hi) in ues_avenues.items():
        if ave in addr_lower:
            # Extract building number
            bldg_match = re.match(r'(\d+)', address)
            if bldg_match:
                bldg_num = int(bldg_match.group(1))
                if num_lo <= bldg_num <= num_hi:
                    return True
            else:
                # No building number — accept as candidate, lat/lng will verify
                return True
    return False


def parse_card_data(card):
    """Extract listing data from a search result card element."""
    text = card.inner_text()
    lines = [l.strip() for l in text.split('\n') if l.strip()]

    # Detail URL
    link_el = card.query_selector('a[href*="homedetails"]')
    detail_path = link_el.get_attribute('href') if link_el else None

    # Price: first line starting with $
    price = None
    for line in lines:
        if line.startswith('$'):
            price = parse_price(line)
            break

    # Beds, baths, sqft: these follow a pattern where the number precedes the label
    beds = baths = sqft = None
    for j, line in enumerate(lines):
        if 'Bedroom' in line and j > 0:
            beds = parse_int(lines[j - 1])
        if 'Bathroom' in line and j > 0:
            baths = parse_float(lines[j - 1])
        if 'Square Feet' in line and j > 0:
            sqft = parse_int(lines[j - 1])

    # Address: line containing Street/Avenue/Place etc. with a unit
    address = None
    for line in lines:
        if any(x in line for x in ['Street', 'Avenue', 'Place', 'Drive', 'Broadway']) and '$' not in line:
            address = line
            break

    # Neighborhood: line right after address
    neighborhood = None
    if address and address in lines:
        addr_idx = lines.index(address)
        if addr_idx + 1 < len(lines):
            candidate = lines[addr_idx + 1]
            # Neighborhood won't start with numbers or $
            if not candidate.startswith('$') and not candidate[0].isdigit():
                neighborhood = candidate

    # No Fee badge
    no_fee = 'No Fee' in text

    return {
        "detail_path": detail_path,
        "price": price,
        "beds": beds,
        "baths": baths,
        "sqft": sqft,
        "address": address,
        "neighborhood": neighborhood,
        "no_fee": no_fee,
    }


def parse_detail_page(page, url):
    """Visit a listing detail page and extract extended data."""
    full_url = BASE_URL + url if url.startswith('/') else url
    print(f"    Fetching detail: {full_url}")

    try:
        page.goto(full_url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(3)
    except Exception as e:
        print(f"    ERROR loading detail page: {e}")
        return {}

    html = page.content()
    text = page.inner_text('body')

    result = {}

    # --- JSON-LD structured data ---
    jsonld_blocks = re.findall(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL
    )
    for block in jsonld_blocks:
        try:
            data = json.loads(block)
            graph = data.get('@graph', [data])
            for item in graph:
                types = item.get('@type', [])
                if isinstance(types, str):
                    types = [types]

                # Lat/lng from geo (if present)
                geo = item.get('geo', {})
                if geo and geo.get('latitude'):
                    result['lat'] = float(geo['latitude'])
                    result['lng'] = float(geo['longitude'])

                # Address — check both direct and containedInPlace
                for addr_source in [item, item.get('containedInPlace', {})]:
                    if not isinstance(addr_source, dict):
                        continue
                    addr = addr_source.get('address', {})
                    if isinstance(addr, dict) and addr.get('streetAddress'):
                        result['full_address'] = addr['streetAddress']

                # Amenities (doorman, etc.)
                amenities = item.get('amenityFeature', [])
                if amenities:
                    amenity_names = [a.get('name', '') for a in amenities]
                    result['amenities'] = amenity_names
                    result['doorman'] = any(
                        'doorman' in a.lower() for a in amenity_names
                    )
        except (json.JSONDecodeError, TypeError, KeyError):
            continue

    # --- Lat/lng from embedded JS data (if not in JSON-LD) ---
    # Compass embeds property coordinates in a JS object with city/state/zipCode context
    if 'lat' not in result:
        # Match the property's coordinates (near neighborhood/zipCode, not transit stops)
        m = re.search(
            r'"(?:zipCode|postalCode)"\s*:\s*"\d+"'
            r'.*?"longitude"\s*:\s*(-?\d+\.\d+)\s*,'
            r'\s*"latitude"\s*:\s*(-?\d+\.\d+)',
            html, re.DOTALL
        )
        if m:
            result['lng'] = float(m.group(1))
            result['lat'] = float(m.group(2))
        else:
            # Reverse order: latitude before longitude
            m = re.search(
                r'"(?:zipCode|postalCode)"\s*:\s*"\d+"'
                r'.*?"latitude"\s*:\s*(-?\d+\.\d+)\s*,'
                r'\s*"longitude"\s*:\s*(-?\d+\.\d+)',
                html, re.DOTALL
            )
            if m:
                result['lat'] = float(m.group(1))
                result['lng'] = float(m.group(2))
        # Fallback: look for the property-specific lat/lng pattern
        # (appears after neighborhood name and before geoId)
        if 'lat' not in result:
            m = re.search(
                r'"city"\s*:\s*"Manhattan".*?'
                r'"longitude"\s*:\s*(-?\d+\.\d+)\s*,\s*'
                r'"latitude"\s*:\s*(-?\d+\.\d+)',
                html
            )
            if m:
                result['lng'] = float(m.group(1))
                result['lat'] = float(m.group(2))

    # --- Text-based extraction ---

    # Days on Market
    m = re.search(r'Days on Market\s*\t?\s*(\d+)', text)
    if m:
        result['dom'] = int(m.group(1))

    # Available Date
    m = re.search(r'Available Date\s*\t?\s*([\d/]+/\d{4})', text)
    if m:
        try:
            result['available_date'] = datetime.strptime(m.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            result['available_date'] = m.group(1)

    # Pet Policy
    m = re.search(r'Pet Policy\s*\n?\s*(.*?)(?:\n|Pet Policy Details)', text, re.DOTALL)
    if m:
        pet = m.group(1).strip()
        if pet and pet != '-':
            result['pet_policy'] = pet

    # Pet Policy Details (more specific)
    m = re.search(r'Pet Policy Details\s*\n?\s*(.*?)(?:\n|Year Built)', text, re.DOTALL)
    if m:
        details = m.group(1).strip()
        if details and details != '-':
            result['pet_policy_details'] = details

    # Rental Incentives / Concessions
    m = re.search(r'Rental Incentives\s*\t?\s*(.*?)(?:\n|Costs)', text)
    if m:
        incentive = m.group(1).strip()
        if incentive and incentive != '-':
            result['rental_incentives'] = incentive

    # Lease Term
    m = re.search(r'Lease (?:Term|Length)[^\n]*?(\d+)\s*-?\s*(\d*)\s*mo', text)
    if m:
        result['lease_min_months'] = int(m.group(1))
        if m.group(2):
            result['lease_max_months'] = int(m.group(2))

    # Costs & Fees - try to find the breakdown
    # Look for "No Fee" in the page text
    if 'no fee' in text.lower() or 'owner pays' in text.lower():
        result['broker_fee'] = 'owner'

    # Listing updated date (useful for calculating true DOM)
    m = re.search(r'LISTING UPDATED:\s*([\d/]+\s+[\d:]+\s*[AP]M)', text)
    if m:
        result['listing_updated'] = m.group(1)

    return result


def scrape_search_page(page, start=0, search_url=None):
    """Scrape one page of search results. Returns list of card data dicts."""
    url = search_url or f"{BASE_URL}/for-rent/manhattan-ny/12000-18000-price/"
    if start > 0:
        url += f"start={start}/"

    print(f"  Loading search page: {url}")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(6)
    except Exception as e:
        print(f"  ERROR loading search page: {e}")
        return [], 0

    # Get total result count
    total = 0
    result_els = page.query_selector_all('[data-tn*="result"]')
    for el in result_els:
        t = el.inner_text()
        m = re.search(r'of\s+(\d+)', t)
        if m:
            total = int(m.group(1))
            break

    # Parse all cards
    cards = page.query_selector_all('[data-testid="cx-react-listingCard"]')
    print(f"  Found {len(cards)} cards (total: {total})")

    results = []
    for card in cards:
        try:
            data = parse_card_data(card)
            results.append(data)
        except Exception as e:
            print(f"  WARNING: Failed to parse card: {e}")
            continue

    return results, total


def build_listing(card_data, detail_data, listing_id):
    """Combine card and detail data into the dashboard schema."""
    address = detail_data.get('full_address') or card_data.get('address', '')

    # Split address into street and unit
    apt = ''
    street = address
    unit_match = re.search(r',?\s*(?:Unit|Apt\.?|#)\s*(.+)$', address, re.IGNORECASE)
    if unit_match:
        apt = unit_match.group(1).strip()
        street = address[:unit_match.start()].strip().rstrip(',')

    # Broker fee
    broker_fee = detail_data.get('broker_fee', 'tenant')
    if card_data.get('no_fee'):
        broker_fee = 'owner'

    # Pet policy
    pet_policy = detail_data.get('pet_policy')
    if pet_policy:
        pet_details = detail_data.get('pet_policy_details')
        if pet_details:
            pet_policy = f"{pet_policy} ({pet_details})"
    else:
        pet_policy = None

    # Concessions
    free_months = 0
    lease_term = 12
    incentives = detail_data.get('rental_incentives', '')
    if incentives:
        fm = re.search(r'(\d+)\s*(?:month|mo)', incentives, re.IGNORECASE)
        if fm:
            free_months = int(fm.group(1))
    lease_min = detail_data.get('lease_min_months', 12)
    lease_term = lease_min if lease_min else 12
    # If there are free months, the lease term is typically longer
    if free_months > 0 and lease_term <= 12:
        lease_term = 12 + free_months

    # Listed date (approximate from DOM)
    dom = detail_data.get('dom', card_data.get('dom'))
    listed = None
    if dom is not None:
        listed_date = date.today().toordinal() - dom
        listed = date.fromordinal(listed_date).isoformat()

    return {
        "id": listing_id,
        "address": street,
        "apt": apt,
        "lat": detail_data.get('lat'),
        "lng": detail_data.get('lng'),
        "rent": card_data.get('price'),
        "beds": card_data.get('beds'),
        "baths": card_data.get('baths'),
        "sqft": card_data.get('sqft'),
        "dom": dom,
        "listed": listed,
        "doorman": detail_data.get('doorman', False),
        "petPolicy": pet_policy,
        "brokerFee": broker_fee,
        "concessions": {
            "freeMonths": free_months,
            "leaseTermMonths": lease_term,
        },
        "availableDate": detail_data.get('available_date'),
        "source": "Compass",
        "sourceUrl": BASE_URL + card_data['detail_path'] if card_data.get('detail_path') else None,
    }


def load_existing_listings():
    """Load existing listings.json if it exists."""
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def merge_listings(existing, new_listings):
    """Merge new Compass listings with existing ones, deduplicating by address+apt."""
    # Index existing non-Compass listings by address+apt
    merged = [l for l in existing if l.get('source') != 'Compass']
    existing_keys = {(l['address'], l['apt']) for l in merged}

    # Add all new Compass listings
    next_id = max((l.get('id', 0) for l in merged), default=0) + 1
    for l in new_listings:
        key = (l['address'], l['apt'])
        if key not in existing_keys:
            l['id'] = next_id
            merged.append(l)
            existing_keys.add(key)
            next_id += 1

    # Re-number IDs sequentially
    for i, l in enumerate(merged, start=1):
        l['id'] = i

    return merged


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape Compass for UES rental listings")
    parser.add_argument('--min-beds', type=int, default=3, help='Minimum bedrooms (default: 3)')
    parser.add_argument('--min-price', type=int, default=12000, help='Min rent (default: 12000)')
    parser.add_argument('--max-price', type=int, default=18000, help='Max rent (default: 18000)')
    parser.add_argument('--no-doorman', action='store_true', help='Skip doorman requirement')
    parser.add_argument('--merge', action='store_true', help='Merge with existing listings.json (keeps non-Compass entries)')
    parser.add_argument('--dry-run', action='store_true', help='Scrape and report but do not write file')
    return parser.parse_args()


def main():
    args = parse_args()

    search_url = f"{BASE_URL}/for-rent/manhattan-ny/{args.min_price}-{args.max_price}-price/"

    print("=" * 60)
    print("Compass UES Rental Scraper")
    print(f"Target: {args.min_beds}+ BR, ${args.min_price:,}-${args.max_price:,}/mo")
    print(f"Area: UES 67th-95th St, west of 3rd Ave + East End Ave")
    print(f"Doorman: {'any' if args.no_doorman else 'required'}")
    print(f"Output: {OUTPUT_FILE} ({'merge' if args.merge else 'overwrite'})")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        # --- Phase 1: Scrape search results ---
        print("\n[Phase 1] Scraping search result pages...")
        all_cards = []
        start = 0

        # First page
        cards, total = scrape_search_page(page, start=0, search_url=search_url)
        all_cards.extend(cards)
        print(f"  Total listings to paginate: {total}")

        # Remaining pages
        while start + RESULTS_PER_PAGE < total:
            start += RESULTS_PER_PAGE
            rate_limit()
            cards, _ = scrape_search_page(page, start=start, search_url=search_url)
            if not cards:
                print("  No more cards, stopping pagination.")
                break
            all_cards.extend(cards)

        print(f"\n  Total cards scraped: {len(all_cards)}")

        # --- Phase 2: Filter candidates ---
        print(f"\n[Phase 2] Filtering for UES {args.min_beds}+ BR candidates...")
        candidates = []
        for card in all_cards:
            # Bed filter
            if card.get('beds') is not None and card['beds'] < args.min_beds:
                continue

            # Must have a detail page URL
            if not card.get('detail_path'):
                continue

            # Neighborhood/address filter for UES
            neighborhood = (card.get('neighborhood') or '').lower()

            is_candidate = False
            if any(n in neighborhood for n in ['upper east', 'ues', 'yorkville', 'lenox hill', 'carnegie hill']):
                is_candidate = True
            elif is_ues_address(card.get('address', '')):
                is_candidate = True

            if is_candidate:
                candidates.append(card)

        print(f"  Candidates after neighborhood/address filter: {len(candidates)}")

        # --- Phase 3: Fetch detail pages ---
        print("\n[Phase 3] Fetching detail pages for each candidate...")
        listings = []
        listing_id = 1

        for i, card in enumerate(candidates):
            print(f"\n  [{i+1}/{len(candidates)}] {card.get('address', 'Unknown')}")
            rate_limit()

            try:
                detail = parse_detail_page(page, card['detail_path'])
            except Exception as e:
                print(f"    ERROR: {e}")
                continue

            # Verify location with lat/lng
            lat = detail.get('lat')
            lng = detail.get('lng')
            if lat and lng and not is_in_ues_bounds(lat, lng):
                print(f"    SKIP: Outside UES bounds (lat={lat:.4f}, lng={lng:.4f})")
                continue

            # Doorman filter
            if not args.no_doorman and not detail.get('doorman', False):
                print(f"    SKIP: No doorman")
                continue

            listing = build_listing(card, detail, listing_id)
            listings.append(listing)
            listing_id += 1
            print(f"    ADDED: {listing['address']} #{listing['apt']} - ${listing['rent']:,}/mo")

        browser.close()

    # --- Phase 4: Write output ---
    print(f"\n[Phase 4] {len(listings)} listings matched all criteria")

    if args.dry_run:
        print("\n[DRY RUN] Not writing file.")
        if listings:
            print("\nWould write:")
            for l in listings:
                print(f"  {l['address']} #{l['apt']}: ${l['rent']:,}/mo ({l['beds']}BR)")
        return

    if not listings:
        print("\nWARNING: No Compass listings matched all criteria.")
        print("This is normal — the Compass UES inventory in this price range fluctuates.")
        print("Keeping existing listings.json unchanged.")
        return

    if args.merge:
        existing = load_existing_listings()
        listings = merge_listings(existing, listings)
        print(f"  Merged: {len(listings)} total listings (including non-Compass)")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(listings, f, indent=2, ensure_ascii=False)

    print(f"\nDone! {len(listings)} listings written to {OUTPUT_FILE}")
    print("\nSummary:")
    compass_listings = [l for l in listings if l.get('source') == 'Compass']
    for l in compass_listings:
        ner = l['rent']
        if l['concessions']['freeMonths'] > 0:
            paid = l['rent'] * (l['concessions']['leaseTermMonths'] - l['concessions']['freeMonths'])
            ner = round(paid / l['concessions']['leaseTermMonths'])
        print(f"  {l['address']} #{l['apt']}: ${l['rent']:,}/mo "
              f"({l['beds']}BR/{l['baths']}BA) "
              f"{'No Fee' if l['brokerFee'] == 'owner' else 'Fee'} "
              f"{'DM' if l['doorman'] else ''} "
              f"NER: ${ner:,}")


if __name__ == '__main__':
    main()
