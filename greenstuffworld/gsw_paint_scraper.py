#!/usr/bin/env python3
"""
Green Stuff World Paint Scraper

Scrapes greenstuffworld.com to build a paint database with hex colors.

Requirements:
    pip install requests beautifulsoup4 pillow

Usage:
    python gsw_paint_scraper.py [--range RANGE_NAME] [--output OUTPUT_FILE]

Examples:
    # Scrape a single range
    python gsw_paint_scraper.py --range acrylic

    # Scrape all ranges and generate individual JSON files
    python gsw_paint_scraper.py --range all --generate

    # Scrape without color sampling (faster, for testing)
    python gsw_paint_scraper.py --range metallic --no-colors

Output format matches the standard paint database schema:
{
    "brand": "Green Stuff World",
    "brandData": {"colorShift": true, "uvReactive": true},
    "category": "",
    "discontinued": false,
    "hex": "#8B4513",
    "id": "gsw-1192",
    "impcat": {"layerId": null, "shadeId": null},
    "name": "Acrylic Color OLIVE-BROWN OPS",
    "range": "Acrylic Paints",
    "sku": "8436574502466ES",
    "type": "opaque",
    "url": "https://www.greenstuffworld.com/en/..."
}
"""

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from PIL import Image

# Base URL
BASE_URL = "https://www.greenstuffworld.com"

# Green Stuff World paint ranges
# Format: range_key -> {name, range, type, url, category_id}
GSW_RANGES = {
    "acrylic": {
        "name": "Acrylic Paints",
        "range": "Acrylic Paints",
        "type": "opaque",
        "url": "https://www.greenstuffworld.com/en/122-acrylic-paints",
        "category_id": 122
    },
    "military": {
        "name": "Military Paints",
        "range": "Military Paints",
        "type": "opaque",
        "url": "https://www.greenstuffworld.com/en/259-military-acrylic-paints",
        "category_id": 259
    },
    "metallic": {
        "name": "Metallic Paints",
        "range": "Metallic Paints",
        "type": "metallic",
        "url": "https://www.greenstuffworld.com/en/126-metallic-acrylic-paints",
        "category_id": 126
    },
    "chameleon": {
        "name": "Chameleon Paints",
        "range": "Chameleon Paints",
        "type": "metallic",
        "url": "https://www.greenstuffworld.com/en/153-chameleon-acrylic-paints",
        "category_id": 153
    },
    "chrome": {
        "name": "Chrome Paints",
        "range": "Chrome Paints",
        "type": "metallic",
        "url": "https://www.greenstuffworld.com/en/403-chrome-paints",
        "category_id": 403
    },
    "fluor": {
        "name": "Fluorescent Paints",
        "range": "Fluorescent Paints",
        "type": "fluorescent",
        "url": "https://www.greenstuffworld.com/en/187-fluorescent-acrylic-paints",
        "category_id": 187
    },
    "dipping_ink": {
        "name": "Dipping Inks",
        "range": "Dipping Inks",
        "type": "contrast",
        "url": "https://www.greenstuffworld.com/en/387-dipping-inks",
        "category_id": 387
    },
    "acrylic_ink": {
        "name": "Acrylic Inks",
        "range": "Acrylic Inks",
        "type": "ink",
        "url": "https://www.greenstuffworld.com/en/129-acrylic-inks",
        "category_id": 129
    },
    "dry_brush": {
        "name": "Dry Brush Paints",
        "range": "Dry Brush Paints",
        "type": "opaque",
        "url": "https://www.greenstuffworld.com/en/470-dry-brush-paints",
        "category_id": 470
    },
    "opaque": {
        "name": "Opaque Colors",
        "range": "Opaque Colors",
        "type": "opaque",
        "url": "https://www.greenstuffworld.com/en/495-opaque-colors",
        "category_id": 495
    },
    "effect": {
        "name": "Effect Paints",
        "range": "Effect Paints",
        "type": "technical",
        "url": "https://www.greenstuffworld.com/en/264-effect-paints",
        "category_id": 264
    },
    "crackle": {
        "name": "Crackle Paint",
        "range": "Crackle Paint",
        "type": "technical",
        "url": "https://www.greenstuffworld.com/en/438-crackle-paint",
        "category_id": 438
    },
    "airbrush": {
        "name": "Airbrush Paint",
        "range": "Airbrush Paint",
        "type": "air",
        "url": "https://www.greenstuffworld.com/en/533-airbrush-paint",
        "category_id": 533
    },
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Words that indicate non-paint products - exclude these
# Note: Use exact matches or phrases to avoid false positives
EXCLUDE_KEYWORDS = [
    'bundle', ' set', 'case', 'collection', 'kit', 'pack', 'combo',
    'palette', 'tool', 'cup', 'handle', 'mixing ball',
    'display', 'rack', 'holder', 'organizer',
    'texture paste', 'putty', 'sculpt',
    'empty pot', 'empty dropper', 'empty bottle',
    'stir stick', 'mixing stick',
    'paint brush', 'synthetic brush', 'kolinsky',
]

# Phrases that indicate a product IS a paint even if it contains exclude keywords
INCLUDE_OVERRIDES = [
    'dry brush paint', 'dry brush -',
    'crackle paint',
]

# Product name patterns that indicate specific paint types
TYPE_PATTERNS = {
    'metallic': ['metallic', 'metal', 'chrome', 'gold', 'silver', 'copper', 'bronze'],
    'wash': ['wash', 'shade'],
    'contrast': ['dipping ink', 'contrast'],
    'ink': ['ink', 'tinta'],
    'fluorescent': ['fluor', 'fluorescent', 'neon', 'uv reactive'],
    'transparent': ['candy', 'transparent', 'clear'],
    'primer': ['primer', 'surface primer'],
    'varnish': ['varnish', 'barniz'],
    'technical': ['effect', 'crackle', 'texture', 'blood', 'slime', 'rust'],
}

# Mapping of range keys to output filenames
RANGE_TO_FILE = {
    'acrylic': 'gsw_acrylic.json',
    'military': 'gsw_military.json',
    'metallic': 'gsw_metallic.json',
    'chameleon': 'gsw_chameleon.json',
    'chrome': 'gsw_chrome.json',
    'fluor': 'gsw_fluor.json',
    'dipping_ink': 'gsw_dipping_ink.json',
    'acrylic_ink': 'gsw_acrylic_ink.json',
    'dry_brush': 'gsw_dry_brush.json',
    'opaque': 'gsw_opaque.json',
    'effect': 'gsw_effect.json',
    'crackle': 'gsw_crackle.json',
    'airbrush': 'gsw_airbrush.json',
}


def fetch_page(url: str, retries: int = 3) -> BeautifulSoup:
    """Fetch a page and return BeautifulSoup object."""
    for attempt in range(retries):
        try:
            print(f"    Fetching: {url}")
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            if attempt < retries - 1:
                print(f"    Retry {attempt + 1}/{retries}: {e}")
                time.sleep(2)
            else:
                raise


def is_valid_hex(hex_color: str) -> bool:
    """Validate hex color format."""
    if not hex_color:
        return False
    return bool(re.match(r'^#[0-9A-Fa-f]{6}$', hex_color))


def is_valid_sku(sku: str) -> bool:
    """Validate SKU format - GSW uses EAN-style barcodes."""
    if not sku:
        return False
    # GSW SKUs are typically 13-digit barcodes with ES suffix
    return bool(re.match(r'^\d{13}[A-Z]{0,2}$', sku))


def is_valid_name(name: str) -> bool:
    """Validate paint name."""
    if not name or len(name) < 3:
        return False
    # Name should contain at least some letters
    if not re.search(r'[a-zA-Z]', name):
        return False
    return True


def is_valid_url(url: str) -> bool:
    """Validate product URL."""
    if not url:
        return False
    return url.startswith('https://www.greenstuffworld.com/') and '.html' in url


def validate_paint(paint: dict) -> tuple[bool, list[str]]:
    """Validate a paint entry and return (is_valid, list of issues)."""
    issues = []

    # Required fields
    if not paint.get('id'):
        issues.append("Missing ID")
    if not is_valid_name(paint.get('name', '')):
        issues.append(f"Invalid name: '{paint.get('name', '')}'")
    if not is_valid_url(paint.get('url', '')):
        issues.append(f"Invalid URL: '{paint.get('url', '')}'")

    # Optional but preferred fields
    if not is_valid_sku(paint.get('sku', '')):
        issues.append(f"Invalid SKU: '{paint.get('sku', '')}'")
    if paint.get('hex') and not is_valid_hex(paint.get('hex', '')):
        issues.append(f"Invalid hex: '{paint.get('hex', '')}'")

    # Type validation
    valid_types = ['opaque', 'metallic', 'wash', 'contrast', 'ink', 'fluorescent',
                   'transparent', 'primer', 'varnish', 'technical', 'air']
    if paint.get('type') not in valid_types:
        issues.append(f"Invalid type: '{paint.get('type', '')}'")

    return len(issues) == 0, issues


def is_paint_product(product: dict) -> bool:
    """Filter out non-paint products like bundles, sets, tools."""
    title = (product.get('title') or '').lower()
    url = (product.get('url') or '').lower()

    # Check for override patterns first - these are definitely paints
    for override in INCLUDE_OVERRIDES:
        if override in title:
            return True

    # Check exclusion keywords
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in title or keyword in url:
            return False

    return True


def normalize_paint_name(raw_name: str) -> str:
    """Clean up paint name for user-friendly display.

    Transforms names like:
      'Acrylic Color ABYSS BLUE' -> 'Abyss Blue'
      'Dipping ink 17 ml - Zombie Dip' -> 'Zombie Dip'
      'Metallic Paint SHINY GOLD' -> 'Shiny Gold'
      'Fluor Paint ORANGE' -> 'Orange'
      'Dry Brush - ALPHA TURQUOISE 30 ml' -> 'Alpha Turquoise'
    """
    name = raw_name.strip()

    # Remove common prefixes (order matters - longer/more specific first)
    prefixes_to_remove = [
        'Acrylic Color ',
        'Acrylic Ink Opaque- ',
        'Acrylic Ink Opaque - ',
        'Transparent Acrylic Ink - ',
        'Metallic Dry Brush - ',
        'Metallic Paint ',
        'Chameleon Paint ',
        'Chrome Paint - ',
        'Fluor Acrylic Ink - ',
        'Fluor Paint ',
        'Dipping ink 60 ml - ',
        'Dipping ink 17 ml - ',
        'Candy Ink ',
        'Intensity Ink ',
        'Wash Ink ',
        'Opaque Colors - ',
        'Dry Brush - ',
        'Crackle Paint - ',
        'Metal Filters - ',
        'Liquid Pigments ',
        'Blood effect - ',
        'Acrylic white paint ',
    ]

    for prefix in prefixes_to_remove:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    # Remove common suffixes like "17ml", "30 ml", "60ml"
    name = re.sub(r'\s*\d+\s*ml\s*$', '', name, flags=re.IGNORECASE)

    # Convert ALL CAPS to Title Case, but preserve mixed case
    if name.isupper():
        name = name.title()

    return name.strip()


def get_paint_type(name: str, default_type: str) -> str:
    """Determine paint type from name, checking for overrides."""
    name_lower = name.lower()

    for paint_type, keywords in TYPE_PATTERNS.items():
        for keyword in keywords:
            if keyword in name_lower:
                return paint_type

    return default_type


def get_brand_data(name: str, range_key: str) -> dict:
    """Extract special brand data based on paint name and range."""
    brand_data = {}
    name_lower = name.lower()

    # Chameleon / color-shift detection
    if range_key == 'chameleon' or 'chameleon' in name_lower or 'colorshift' in name_lower:
        brand_data['colorShift'] = True

    # UV reactive / fluorescent detection
    if range_key == 'fluor' or 'fluor' in name_lower or 'uv' in name_lower or 'neon' in name_lower:
        brand_data['uvReactive'] = True

    # Transparent / candy detection
    if 'candy' in name_lower or 'transparent' in name_lower:
        brand_data['transparent'] = True

    # Glow in the dark
    if 'glow' in name_lower:
        brand_data['glowInDark'] = True

    return brand_data


def extract_products_from_page(soup: BeautifulSoup) -> list:
    """Extract product data from a GSW category page."""
    products = []
    seen_ids = set()

    # GSW uses article.product-miniature elements
    for article in soup.select('article.product-miniature'):
        try:
            # Get product ID
            product_id = article.get('data-id-product')
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            # Get product URL and title
            title_link = article.select_one('h3.product-title a')
            if not title_link:
                continue

            product_url = title_link.get('href', '')
            title = title_link.get_text(strip=True)

            # Get SKU/reference
            ref_elem = article.select_one('.pl_reference span strong')
            sku = ref_elem.get_text(strip=True) if ref_elem else ''

            # Get image URL
            img_elem = article.select_one('img')
            img_url = None
            if img_elem:
                # Get the large image URL
                img_url = img_elem.get('data-full-size-image-url') or img_elem.get('src')
                if img_url and not img_url.startswith('http'):
                    img_url = urljoin(BASE_URL, img_url)

            products.append({
                'id': product_id,
                'title': title,
                'sku': sku,
                'url': product_url,
                'img_url': img_url
            })

        except Exception as e:
            print(f"      Warning: Error parsing product: {e}")

    return products


def get_next_page_url(soup: BeautifulSoup) -> str:
    """Get the URL for the next page of results."""
    # GSW uses standard pagination
    next_link = soup.select_one('a.next.js-search-link, a[rel="next"]')
    if next_link:
        href = next_link.get('href')
        if href:
            return urljoin(BASE_URL, href)
    return None


def get_total_pages(soup: BeautifulSoup) -> int:
    """Get total number of pages from pagination."""
    # Look for pagination items
    page_items = soup.select('.pagination .page-item a.page-link, .pagination a.js-search-link')
    max_page = 1
    for item in page_items:
        text = item.get_text(strip=True)
        if text.isdigit():
            max_page = max(max_page, int(text))
    return max_page


def sample_color_from_image(img_url: str, range_hint: str = '') -> str:
    """Download image and sample the dominant paint color.

    GSW product images typically show paint bottles or swatches.
    We sample from central regions to get the paint color.
    """
    try:
        if not img_url:
            return None

        # Handle protocol-relative URLs
        if img_url.startswith('//'):
            img_url = 'https:' + img_url

        response = requests.get(img_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content)).convert('RGB')
        width, height = img.size

        # GSW images vary - some are bottles, some are swatches
        # Sample from multiple regions and pick the most saturated color
        sample_regions = [
            # Center area
            (int(width * 0.5), int(height * 0.4)),
            (int(width * 0.5), int(height * 0.5)),
            (int(width * 0.5), int(height * 0.6)),
            # Left-center (often where paint is visible on bottle)
            (int(width * 0.3), int(height * 0.5)),
            (int(width * 0.4), int(height * 0.5)),
            # Right-center
            (int(width * 0.6), int(height * 0.5)),
            (int(width * 0.7), int(height * 0.5)),
            # Upper area
            (int(width * 0.5), int(height * 0.3)),
        ]

        best_color = None
        best_score = -1

        for x, y in sample_regions:
            # Sample a small region around the point
            colors = []
            for dx in range(-8, 9, 2):
                for dy in range(-8, 9, 2):
                    px = max(0, min(x + dx, width - 1))
                    py = max(0, min(y + dy, height - 1))
                    colors.append(img.getpixel((px, py)))

            # Average the colors
            r = sum(c[0] for c in colors) // len(colors)
            g = sum(c[1] for c in colors) // len(colors)
            b = sum(c[2] for c in colors) // len(colors)

            # Score: prefer saturated, non-white, non-black, non-gray colors
            max_c = max(r, g, b)
            min_c = min(r, g, b)
            saturation = (max_c - min_c) / max(max_c, 1) if max_c > 0 else 0
            brightness = (r + g + b) / 3

            # Skip near-white, near-black, or very gray
            if brightness > 240 or brightness < 15:
                continue
            if max_c - min_c < 10 and 50 < brightness < 200:
                # Very gray, skip unless it's the best we have
                pass

            # Prefer mid-brightness, saturated colors
            brightness_penalty = abs(brightness - 127) / 127
            score = saturation * (1 - brightness_penalty * 0.3) + 0.1

            if score > best_score:
                best_score = score
                best_color = (r, g, b)

        if best_color:
            return "#{:02X}{:02X}{:02X}".format(*best_color)

        # Fallback: sample from center
        x, y = int(width * 0.5), int(height * 0.5)
        r, g, b = img.getpixel((x, y))
        return "#{:02X}{:02X}{:02X}".format(r, g, b)

    except Exception as e:
        print(f"        Error sampling color: {e}")
        return None


def sample_secondary_color(img_url: str) -> str:
    """Sample a secondary color for color-shift paints.

    For chameleon paints, sample from different regions to capture
    the color shift effect.
    """
    try:
        if not img_url:
            return None

        if img_url.startswith('//'):
            img_url = 'https:' + img_url

        response = requests.get(img_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content)).convert('RGB')
        width, height = img.size

        # Sample from edges/corners for secondary color
        sample_regions = [
            (int(width * 0.2), int(height * 0.3)),
            (int(width * 0.8), int(height * 0.7)),
            (int(width * 0.2), int(height * 0.7)),
            (int(width * 0.8), int(height * 0.3)),
        ]

        all_colors = []
        for x, y in sample_regions:
            colors = []
            for dx in range(-5, 6, 2):
                for dy in range(-5, 6, 2):
                    px = max(0, min(x + dx, width - 1))
                    py = max(0, min(y + dy, height - 1))
                    colors.append(img.getpixel((px, py)))

            r = sum(c[0] for c in colors) // len(colors)
            g = sum(c[1] for c in colors) // len(colors)
            b = sum(c[2] for c in colors) // len(colors)
            all_colors.append((r, g, b))

        if all_colors:
            r, g, b = all_colors[0]
            # Only return if it's a valid color (not too dark/light)
            brightness = (r + g + b) / 3
            if 20 < brightness < 235:
                return "#{:02X}{:02X}{:02X}".format(r, g, b)

        return None

    except Exception:
        return None


def process_product(product: dict, range_info: dict, sample_colors: bool = True, verbose: bool = False) -> dict:
    """Process a single product and return paint entry."""
    raw_title = product.get('title', '')
    product_id = product.get('id', '')
    sku = product.get('sku', '')
    url = product.get('url', '')
    img_url = product.get('img_url')
    range_key = range_info.get('key', '')

    # Normalize paint name for user-friendly display
    name = normalize_paint_name(raw_title)

    # Determine paint type (use raw title for pattern matching)
    paint_type = get_paint_type(raw_title, range_info['type'])

    # Get brand data for special effects (use raw title for pattern matching)
    brand_data = get_brand_data(raw_title, range_key)

    # Sample colors
    hex_color = None
    if sample_colors and img_url:
        if verbose:
            print(f"      Sampling: {name}")
        hex_color = sample_color_from_image(img_url, range_key)

        # For color-shift paints, try to get secondary color
        if brand_data.get('colorShift') and hex_color:
            secondary_hex = sample_secondary_color(img_url)
            if secondary_hex and secondary_hex != hex_color:
                brand_data['secondaryHex'] = secondary_hex

    # Create paint ID
    paint_id = f"gsw-{product_id}"

    return {
        "brand": "Green Stuff World",
        "brandData": brand_data,
        "category": "",
        "discontinued": False,
        "hex": hex_color or "",
        "id": paint_id,
        "impcat": {"layerId": None, "shadeId": None},
        "name": name,
        "range": range_info['range'],
        "sku": sku,
        "type": paint_type,
        "url": url
    }


def scrape_range(range_key: str, sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> list:
    """Scrape all paints from a GSW range."""
    if range_key not in GSW_RANGES:
        print(f"Unknown range: {range_key}")
        return []

    range_info = GSW_RANGES[range_key].copy()
    range_info['key'] = range_key
    range_name = range_info['name']
    base_url = range_info['url']

    print(f"\n{'='*60}")
    print(f"Scraping: {range_name} ({range_key})")
    print('='*60)

    all_products = []
    page = 1
    current_url = base_url

    while current_url:
        try:
            soup = fetch_page(current_url)
            products = extract_products_from_page(soup)

            if not products:
                if page == 1:
                    print(f"    No products found for: {range_key}")
                break

            # Filter non-paint products
            before_filter = len(products)
            products = [p for p in products if is_paint_product(p)]
            if len(products) < before_filter:
                print(f"    Page {page}: {before_filter} products, {len(products)} after filtering")
            else:
                print(f"    Page {page}: {len(products)} products")

            all_products.extend(products)

            # Check for next page
            next_url = get_next_page_url(soup)
            if next_url and next_url != current_url:
                current_url = next_url
                page += 1
                time.sleep(0.5)  # Be polite
            else:
                break

        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break

    print(f"  Total products found: {len(all_products)}")

    if not all_products:
        return []

    # Process products with color sampling
    paints = []

    if sample_colors and max_workers > 1:
        print(f"  Sampling colors ({max_workers} threads)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_product, p, range_info, True, verbose): p
                for p in all_products
            }
            completed = 0
            for future in as_completed(futures):
                completed += 1
                try:
                    paint = future.result()
                    paints.append(paint)
                    if verbose or completed % 10 == 0 or completed == len(all_products):
                        print(f"      [{completed}/{len(all_products)}] {paint['name']}: {paint['hex']}")
                except Exception as e:
                    print(f"      Error processing product: {e}")
    else:
        for i, product in enumerate(all_products):
            paint = process_product(product, range_info, sample_colors, verbose)
            paints.append(paint)
            if verbose or (i + 1) % 10 == 0:
                print(f"      [{i+1}/{len(all_products)}] {paint['name']}")

    # Sort by name
    paints.sort(key=lambda x: x['name'].lower())

    # Deduplicate by normalized name (handles different sizes like 17ml vs 60ml)
    seen_names = {}
    deduplicated_paints = []
    duplicates_removed = 0

    for paint in paints:
        name_key = paint['name'].lower()
        if name_key in seen_names:
            duplicates_removed += 1
            # Keep the one with the better hex color (non-empty preferred)
            existing = seen_names[name_key]
            if not existing.get('hex') and paint.get('hex'):
                # Replace with new paint that has hex
                idx = deduplicated_paints.index(existing)
                deduplicated_paints[idx] = paint
                seen_names[name_key] = paint
        else:
            seen_names[name_key] = paint
            deduplicated_paints.append(paint)

    paints = deduplicated_paints
    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} size duplicates")

    # Validate all paints
    valid_paints = []
    validation_issues = []
    missing_hex = 0
    missing_sku = 0

    for paint in paints:
        is_valid, issues = validate_paint(paint)

        # Track missing optional fields
        if not paint.get('hex'):
            missing_hex += 1
        if not is_valid_sku(paint.get('sku', '')):
            missing_sku += 1

        # Filter out critical issues (missing name, id, url)
        critical_issues = [i for i in issues if 'name' in i.lower() or 'id' in i.lower() or 'url' in i.lower()]
        if critical_issues:
            validation_issues.append((paint.get('name', 'Unknown'), critical_issues))
        else:
            valid_paints.append(paint)

    # Report validation results
    print(f"  Total paints: {len(valid_paints)}")
    if missing_hex > 0:
        print(f"  Warning: {missing_hex} paints missing hex color")
    if missing_sku > 0:
        print(f"  Warning: {missing_sku} paints with invalid/missing SKU")
    if validation_issues:
        print(f"  Rejected {len(validation_issues)} invalid entries:")
        for name, issues in validation_issues[:5]:  # Show first 5
            print(f"    - {name}: {', '.join(issues)}")
        if len(validation_issues) > 5:
            print(f"    ... and {len(validation_issues) - 5} more")

    return valid_paints


def scrape_all_ranges(sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> dict:
    """Scrape all GSW paint ranges."""
    all_data = {}

    for range_key in GSW_RANGES.keys():
        paints = scrape_range(range_key, sample_colors, verbose, max_workers)
        all_data[range_key] = {
            'name': GSW_RANGES[range_key]['name'],
            'range': GSW_RANGES[range_key]['range'],
            'paints': paints
        }
        time.sleep(1)  # Be polite between ranges

    return all_data


def validate_json_file(filepath: str) -> None:
    """Validate an existing JSON file and report issues."""
    print(f"Validating: {filepath}")
    print('=' * 60)

    try:
        with open(filepath, 'r') as f:
            paints = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {filepath}")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}")
        return

    if not isinstance(paints, list):
        print("Error: JSON root must be an array")
        return

    print(f"Total entries: {len(paints)}")

    # Validation statistics
    valid_count = 0
    issues_by_type = {}
    missing_hex = 0
    missing_sku = 0

    # Check for duplicates
    seen_ids = {}
    seen_skus = {}
    seen_names = {}
    duplicates = []

    for i, paint in enumerate(paints):
        # Duplicate detection
        paint_id = paint.get('id', '')
        sku = paint.get('sku', '')
        name = paint.get('name', '')

        if paint_id and paint_id in seen_ids:
            duplicates.append(f"Duplicate ID '{paint_id}': entries {seen_ids[paint_id]} and {i}")
        else:
            seen_ids[paint_id] = i

        if sku and sku in seen_skus:
            duplicates.append(f"Duplicate SKU '{sku}': {seen_names.get(seen_skus[sku], '?')} and {name}")
        else:
            seen_skus[sku] = i
            seen_names[i] = name

        # Validation
        is_valid, issues = validate_paint(paint)

        if not paint.get('hex'):
            missing_hex += 1
        if not is_valid_sku(paint.get('sku', '')):
            missing_sku += 1

        if is_valid:
            valid_count += 1
        else:
            for issue in issues:
                issue_type = issue.split(':')[0] if ':' in issue else issue
                issues_by_type[issue_type] = issues_by_type.get(issue_type, 0) + 1

    # Report results
    print(f"\nValidation Results:")
    print(f"  Valid entries: {valid_count}/{len(paints)}")
    print(f"  Missing hex colors: {missing_hex}")
    print(f"  Invalid/missing SKUs: {missing_sku}")

    if duplicates:
        print(f"\nDuplicates found ({len(duplicates)}):")
        for dup in duplicates[:10]:
            print(f"  - {dup}")
        if len(duplicates) > 10:
            print(f"  ... and {len(duplicates) - 10} more")

    if issues_by_type:
        print(f"\nIssues by type:")
        for issue_type, count in sorted(issues_by_type.items(), key=lambda x: -x[1]):
            print(f"  - {issue_type}: {count}")

    # Sample some entries with issues
    print(f"\nSample entries with issues:")
    shown = 0
    for paint in paints:
        is_valid, issues = validate_paint(paint)
        if not is_valid and shown < 5:
            print(f"  {paint.get('name', 'Unknown')}:")
            for issue in issues:
                print(f"    - {issue}")
            shown += 1

    if valid_count == len(paints) and not duplicates:
        print(f"\n✓ All {len(paints)} entries are valid!")
    else:
        print(f"\n✗ Found issues in {len(paints) - valid_count} entries")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Green Stuff World paint data with hex colors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available ranges:
  acrylic         Acrylic Paints (standard colors)
  military        Military Paints
  metallic        Metallic Paints
  chameleon       Chameleon Paints (color-shift)
  chrome          Chrome Paints
  fluor           Fluorescent Paints (UV reactive)
  dipping_ink     Dipping Inks (contrast-style)
  acrylic_ink     Acrylic Inks
  dry_brush       Dry Brush Paints
  opaque          Opaque Colors
  effect          Effect Paints
  crackle         Crackle Paint
  airbrush        Airbrush Paint

  all             Scrape everything
        """
    )
    parser.add_argument('--range', '-r', default='all',
                       help='Range to scrape (default: all)')
    parser.add_argument('--no-colors', action='store_true',
                       help='Skip color sampling')
    parser.add_argument('--workers', '-w', type=int, default=8,
                       help='Number of parallel threads for image sampling (default: 8)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--validate', type=str, metavar='FILE',
                       help='Validate an existing JSON file instead of scraping')

    args = parser.parse_args()
    sample_colors = not args.no_colors

    # Validate mode
    if args.validate:
        validate_json_file(args.validate)
        return

    if args.range == 'all':
        print("Scraping ALL Green Stuff World ranges...")
        data = scrape_all_ranges(sample_colors, args.verbose, args.workers)

        # Always generate separate files per range (default behavior)
        print(f"\nGenerating {len(data)} catalogue files:")
        total_paints = 0
        for range_key, range_data in data.items():
            output_file = RANGE_TO_FILE.get(range_key, f'gsw_{range_key}.json')
            paints = range_data['paints']
            with open(output_file, 'w') as f:
                json.dump(paints, f, indent=2)
            print(f"  {output_file}: {len(paints)} paints")
            total_paints += len(paints)
        print(f"\nTotal: {total_paints} paints across {len(data)} ranges")
    else:
        if args.range not in GSW_RANGES:
            print(f"Unknown range: {args.range}")
            print(f"Available: {', '.join(GSW_RANGES.keys())}")
            return

        paints = scrape_range(args.range, sample_colors, args.verbose, args.workers)

        # Always use the standard output file for the range
        output_file = RANGE_TO_FILE.get(args.range, f'gsw_{args.range}.json')

        with open(output_file, 'w') as f:
            json.dump(paints, f, indent=2)
        print(f"\nSaved: {output_file} ({len(paints)} paints)")


if __name__ == '__main__':
    main()
