#!/usr/bin/env python3
"""
Turbodork Paint Scraper

Scrapes turbodork.com to build a paint database with hex colors.

Requirements:
    pip install requests beautifulsoup4 pillow

Usage:
    python turbodork_paint_scraper.py [--range RANGE_NAME] [--output OUTPUT_FILE]

Examples:
    # Scrape a single range
    python turbodork_paint_scraper.py --range turboshift

    # Scrape all ranges and generate individual JSON files
    python turbodork_paint_scraper.py --range all --generate

    # Scrape without color sampling (faster, for testing)
    python turbodork_paint_scraper.py --range metallic --no-colors

Output format matches the standard paint database schema:
{
    "brand": "Turbodork",
    "brandData": {"colorShift": true, "secondaryHex": "#RRGGBB"},
    "category": "",
    "discontinued": false,
    "hex": "#8B4513",
    "id": "turbodork-3d-glasses",
    "impcat": {"layerId": null, "shadeId": null},
    "name": "3D Glasses",
    "range": "Turboshift",
    "sku": "TDK015014",
    "type": "metallic",
    "url": "https://turbodork.com/products/..."
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
BASE_URL = "https://turbodork.com"

# Turbodork paint ranges
# Format: range_key -> {name, range, type, url}
TURBODORK_RANGES = {
    "turboshift": {
        "name": "Turboshift",
        "range": "Turboshift",
        "type": "metallic",
        "url": "https://turbodork.com/collections/turboshift-paints"
    },
    "metallic": {
        "name": "Metallic",
        "range": "Metallic",
        "type": "metallic",
        "url": "https://turbodork.com/collections/metallic-paints"
    },
    "zenishift": {
        "name": "ZeniShift",
        "range": "ZeniShift",
        "type": "metallic",
        "url": "https://turbodork.com/collections/zenishift"
    },
    "mediums": {
        "name": "Mediums",
        "range": "Mediums",
        "type": "technical",
        "url": "https://turbodork.com/collections/mediums"
    },
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Words that indicate non-paint products - exclude these
EXCLUDE_KEYWORDS = [
    'bundle', 'set', 'case', 'full case', 'collection', 'kit', 'pack',
    'brush', 'palette', 'tool', 'cup', 'handle', 'stick', 'mixing',
    'gift card', 'hat', 'sticker', 'merchandise',
    '6 count', '12 count', '24 count',
]

# Mapping of range keys to output filenames
RANGE_TO_FILE = {
    'turboshift': 'turbodork_turboshift.json',
    'metallic': 'turbodork_metallic.json',
    'zenishift': 'turbodork_zenishift.json',
    'mediums': 'turbodork_mediums.json',
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


def fetch_json(url: str, retries: int = 3) -> dict:
    """Fetch JSON from a URL."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                print(f"    Retry {attempt + 1}/{retries}: {e}")
                time.sleep(2)
            else:
                raise


def is_paint_product(product: dict) -> bool:
    """Filter out non-paint products like bundles, sets, tools."""
    title = (product.get('title') or '').lower()
    handle = (product.get('handle') or '').lower()
    product_type = (product.get('product_type') or '').lower()

    # Check exclusion keywords
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in title or keyword in handle:
            return False

    # Bundles are often in product_type
    if 'bundle' in product_type or 'set' in product_type:
        return False

    return True


def get_product_list_from_collection(collection_url: str) -> list:
    """Get all products from a Shopify collection using the JSON API."""
    products = []
    page = 1

    while True:
        # Shopify collections expose a products.json endpoint
        json_url = f"{collection_url}/products.json?page={page}&limit=250"
        try:
            print(f"    Fetching: {json_url}")
            data = fetch_json(json_url)
            page_products = data.get('products', [])

            if not page_products:
                break

            products.extend(page_products)
            print(f"    Page {page}: {len(page_products)} products")

            if len(page_products) < 250:
                break

            page += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"    Error fetching page {page}: {e}")
            # Fall back to HTML scraping
            break

    return products


def get_products_from_html(soup: BeautifulSoup) -> list:
    """Extract product data from HTML page (fallback method)."""
    products = []

    # Shopify typically uses product-card or similar classes
    for item in soup.select('.product-card, .grid-item, [class*="product"]'):
        try:
            # Get link to product page
            link = item.select_one('a[href*="/products/"]')
            if not link:
                continue

            href = link.get('href', '')
            if not href.startswith('/products/'):
                continue

            product_url = urljoin(BASE_URL, href)

            # Get title
            title_elem = item.select_one('.product-title, .card-title, h3, h2')
            title = title_elem.get_text(strip=True) if title_elem else None

            # Get image
            img = item.select_one('img')
            img_url = None
            if img:
                src = img.get('src') or img.get('data-src') or img.get('data-srcset', '').split()[0]
                if src:
                    if src.startswith('//'):
                        src = 'https:' + src
                    img_url = src

            if title:
                products.append({
                    'title': title,
                    'handle': href.replace('/products/', '').rstrip('/'),
                    'url': product_url,
                    'image_url': img_url
                })

        except Exception as e:
            print(f"      Warning: Error parsing product item: {e}")

    return products


def scrape_product_page(product_url: str) -> dict:
    """Scrape detailed product info from a product page."""
    try:
        # Try JSON endpoint first
        json_url = f"{product_url}.json"
        data = fetch_json(json_url)
        product = data.get('product', {})

        return {
            'id': product.get('id'),
            'title': product.get('title'),
            'handle': product.get('handle'),
            'product_type': product.get('product_type', ''),
            'vendor': product.get('vendor'),
            'tags': product.get('tags', []),
            'variants': product.get('variants', []),
            'images': product.get('images', []),
            'body_html': product.get('body_html', ''),
        }
    except Exception as e:
        print(f"      Error fetching product JSON: {e}")
        return None


def extract_sku(product: dict) -> str:
    """Extract SKU from product data."""
    variants = product.get('variants', [])
    if variants:
        sku = variants[0].get('sku', '')
        if sku:
            # Clean up SKU - remove _1 suffix often added
            sku = re.sub(r'_\d+$', '', sku)
            return sku
    return ''


def extract_color_info_from_tags(tags: list) -> dict:
    """Extract color and type info from product tags."""
    info = {
        'color_shift': False,
        'tone': None,
        'basecoat': None,
        'shift_colors': None,  # e.g., "pink-gold" from "zeni:pink-gold"
        'colors': [],  # e.g., ["pink", "gold"] from "color:pink", "color:gold"
    }

    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(',')]

    for tag in tags:
        tag_lower = tag.lower()
        if 'shift' in tag_lower:
            info['color_shift'] = True
        if tag_lower.startswith('tone:'):
            info['tone'] = tag_lower.replace('tone:', '').title()
        if tag_lower.startswith('primer:'):
            info['basecoat'] = tag_lower.replace('primer:', '').title()
        if tag_lower.startswith('zeni:'):
            # e.g., "zeni:pink-gold" -> "pink-gold"
            info['shift_colors'] = tag_lower.replace('zeni:', '')
        if tag_lower.startswith('color:'):
            # e.g., "color:pink" -> "pink"
            info['colors'].append(tag_lower.replace('color:', ''))

    return info


def get_swatch_image_url(product: dict) -> str:
    """Find the color swatch image URL from product images."""
    images = product.get('images', [])

    for img in images:
        src = img.get('src', '')
        # Look for swatch images (usually have 'swatch' in name)
        if 'swatch' in src.lower():
            return src

    # Fallback: use the second image (often swatch) or first
    if len(images) >= 2:
        return images[1].get('src', '')
    elif images:
        return images[0].get('src', '')

    return ''


def sample_color_from_image(img_url: str, is_swatch: bool = True) -> str:
    """Download image and sample the dominant color."""
    try:
        # Handle protocol-relative URLs
        if img_url.startswith('//'):
            img_url = 'https:' + img_url

        response = requests.get(img_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content)).convert('RGB')
        width, height = img.size

        if is_swatch:
            # For swatch images, sample from center region
            sample_regions = [
                (int(width * 0.5), int(height * 0.5)),
                (int(width * 0.4), int(height * 0.5)),
                (int(width * 0.6), int(height * 0.5)),
                (int(width * 0.5), int(height * 0.4)),
                (int(width * 0.5), int(height * 0.6)),
            ]
        else:
            # For bottle images, sample from upper area where paint is visible
            sample_regions = [
                (int(width * 0.5), int(height * 0.3)),
                (int(width * 0.4), int(height * 0.35)),
                (int(width * 0.6), int(height * 0.35)),
                (int(width * 0.5), int(height * 0.25)),
            ]

        best_color = None
        best_score = -1

        for x, y in sample_regions:
            # Sample a small region around the point
            colors = []
            for dx in range(-10, 11, 3):
                for dy in range(-10, 11, 3):
                    px = max(0, min(x + dx, width - 1))
                    py = max(0, min(y + dy, height - 1))
                    colors.append(img.getpixel((px, py)))

            # Average the colors
            r = sum(c[0] for c in colors) // len(colors)
            g = sum(c[1] for c in colors) // len(colors)
            b = sum(c[2] for c in colors) // len(colors)

            # Score: prefer saturated, non-white, non-black colors
            max_c = max(r, g, b)
            min_c = min(r, g, b)
            saturation = (max_c - min_c) / max(max_c, 1) if max_c > 0 else 0
            brightness = (r + g + b) / 3

            # Skip near-white or near-black
            if brightness > 240 or brightness < 15:
                continue

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
    """Sample a secondary color from a color-shift paint swatch.

    For color-shift paints, the swatch often shows gradients.
    We sample from a different region to get the secondary color.
    """
    try:
        if img_url.startswith('//'):
            img_url = 'https:' + img_url

        response = requests.get(img_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content)).convert('RGB')
        width, height = img.size

        # Sample from edges/corners for secondary color
        sample_regions = [
            (int(width * 0.2), int(height * 0.2)),
            (int(width * 0.8), int(height * 0.8)),
            (int(width * 0.2), int(height * 0.8)),
            (int(width * 0.8), int(height * 0.2)),
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

        # Find the most different color from center
        if all_colors:
            r, g, b = all_colors[0]
            return "#{:02X}{:02X}{:02X}".format(r, g, b)

        return None

    except Exception:
        return None


def process_product(product_data: dict, range_info: dict, sample_colors: bool = True, verbose: bool = False) -> dict:
    """Process a single product and return paint entry."""
    title = product_data.get('title', '')
    handle = product_data.get('handle', '')
    product_type = product_data.get('product_type', '')
    tags = product_data.get('tags', [])

    # Extract SKU
    sku = extract_sku(product_data)

    # Build product URL
    url = f"{BASE_URL}/products/{handle}"

    # Determine paint type from product_type or range
    paint_type = range_info['type']
    product_type_lower = product_type.lower()
    if 'turboshift' in product_type_lower:
        paint_type = 'metallic'
        is_color_shift = True
    elif 'zenishift' in product_type_lower:
        paint_type = 'metallic'
        is_color_shift = True
    elif 'metallic' in product_type_lower:
        paint_type = 'metallic'
        is_color_shift = False
    elif 'medium' in product_type_lower:
        paint_type = 'technical'
        is_color_shift = False
    else:
        is_color_shift = 'shift' in range_info['name'].lower()

    # Extract color info from tags
    color_info = extract_color_info_from_tags(tags)
    is_color_shift = is_color_shift or color_info['color_shift']

    # Sample colors from swatch image
    hex_color = None
    secondary_hex = None

    if sample_colors:
        swatch_url = get_swatch_image_url(product_data)
        if swatch_url:
            if verbose:
                print(f"      Sampling: {title}")
            hex_color = sample_color_from_image(swatch_url, is_swatch=True)

            # For color-shift paints, try to get secondary color
            if is_color_shift and hex_color:
                secondary_hex = sample_secondary_color(swatch_url)

    # Build brand data
    brand_data = {}
    if is_color_shift:
        brand_data['colorShift'] = True
        # Only include secondary hex if it's valid and different from primary
        if secondary_hex and secondary_hex != hex_color:
            # Skip if it's basically black or white (edge artifact)
            sec_brightness = sum(int(secondary_hex[i:i+2], 16) for i in (1, 3, 5)) / 3
            if 20 < sec_brightness < 235:
                brand_data['secondaryHex'] = secondary_hex
        # Include shift color names from tags if available
        if color_info['shift_colors']:
            brand_data['shiftColors'] = color_info['shift_colors']
        elif len(color_info['colors']) >= 2:
            brand_data['shiftColors'] = '-'.join(color_info['colors'][:2])
    if color_info['tone']:
        brand_data['tone'] = color_info['tone']
    if color_info['basecoat']:
        brand_data['recommendedBasecoat'] = color_info['basecoat']

    # Create ID from handle
    paint_id = f"turbodork-{handle.replace('/', '-')}"
    # Remove trailing -1 if present (common in Shopify handles)
    paint_id = re.sub(r'-1$', '', paint_id)

    return {
        "brand": "Turbodork",
        "brandData": brand_data,
        "category": "",
        "discontinued": False,
        "hex": hex_color or "",
        "id": paint_id,
        "impcat": {"layerId": None, "shadeId": None},
        "name": title,
        "range": range_info['range'],
        "sku": sku,
        "type": paint_type,
        "url": url
    }


def scrape_range(range_key: str, sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> list:
    """Scrape all paints from a Turbodork range."""
    if range_key not in TURBODORK_RANGES:
        print(f"Unknown range: {range_key}")
        return []

    range_info = TURBODORK_RANGES[range_key]
    range_name = range_info['name']
    collection_url = range_info['url']

    print(f"\n{'='*60}")
    print(f"Scraping: {range_name} ({range_key})")
    print('='*60)

    # Get products from collection JSON API
    print(f"  Fetching collection: {collection_url}")

    try:
        # Shopify collections JSON endpoint
        products = get_product_list_from_collection(collection_url)
    except Exception as e:
        print(f"  JSON API failed: {e}")
        print(f"  Falling back to HTML scraping...")
        soup = fetch_page(collection_url)
        products = get_products_from_html(soup)

    if not products:
        print(f"    No products found for: {range_key}")
        return []

    print(f"  Found {len(products)} products")

    # Filter out non-paint products
    before_filter = len(products)
    products = [p for p in products if is_paint_product(p)]
    if len(products) < before_filter:
        print(f"  Filtered to {len(products)} paint products (removed {before_filter - len(products)} non-paint items)")

    # Fetch detailed info for each product
    print(f"  Fetching product details...")
    detailed_products = []

    for i, product in enumerate(products):
        if 'variants' not in product:
            # Need to fetch full product data
            handle = product.get('handle', '')
            if handle:
                product_url = f"{BASE_URL}/products/{handle}"
                try:
                    detail = scrape_product_page(product_url)
                    if detail:
                        detailed_products.append(detail)
                    time.sleep(0.3)
                except Exception as e:
                    print(f"      Error fetching {handle}: {e}")
        else:
            detailed_products.append(product)

        if (i + 1) % 10 == 0:
            print(f"    Processed {i + 1}/{len(products)} products")

    print(f"  Processing {len(detailed_products)} products...")

    # Process products with color sampling
    paints = []

    if sample_colors and max_workers > 1:
        print(f"  Sampling colors ({max_workers} threads)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_product, p, range_info, True, verbose): p
                for p in detailed_products
            }
            completed = 0
            for future in as_completed(futures):
                completed += 1
                try:
                    paint = future.result()
                    paints.append(paint)
                    if verbose or completed % 10 == 0:
                        print(f"      [{completed}/{len(detailed_products)}] {paint['name']}: {paint['hex']}")
                except Exception as e:
                    print(f"      Error processing product: {e}")
    else:
        for product in detailed_products:
            paint = process_product(product, range_info, sample_colors, verbose)
            paints.append(paint)

    # Sort by name
    paints.sort(key=lambda x: x['name'].lower())

    print(f"  Total: {len(paints)} paints")
    return paints


def scrape_all_ranges(sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> dict:
    """Scrape all Turbodork ranges."""
    all_data = {}

    for range_key in TURBODORK_RANGES.keys():
        paints = scrape_range(range_key, sample_colors, verbose, max_workers)
        all_data[range_key] = {
            'name': TURBODORK_RANGES[range_key]['name'],
            'range': TURBODORK_RANGES[range_key]['range'],
            'paints': paints
        }
        time.sleep(1)  # Be polite between ranges

    return all_data


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Turbodork paint data with hex colors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available ranges:
  turboshift      Turboshift (color-shifting metallics)
  metallic        Metallic (standard metallics)
  zenishift       ZeniShift (zenithal color-shift)
  mediums         Mediums (thinners, additives)

  all             Scrape everything
        """
    )
    parser.add_argument('--range', '-r', default='all',
                       help='Range to scrape (default: all)')
    parser.add_argument('--output', '-o', default='turbodork_paints.json',
                       help='Output JSON file')
    parser.add_argument('--no-colors', action='store_true',
                       help='Skip color sampling')
    parser.add_argument('--workers', '-w', type=int, default=8,
                       help='Number of parallel threads for image sampling (default: 8)')
    parser.add_argument('--generate', '-g', action='store_true',
                       help='Generate separate catalogue files per range')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')

    args = parser.parse_args()
    sample_colors = not args.no_colors

    if args.range == 'all':
        print("Scraping ALL Turbodork ranges...")
        data = scrape_all_ranges(sample_colors, args.verbose, args.workers)

        if args.generate:
            # Generate separate files per range
            print(f"\nGenerating {len(data)} catalogue files:")
            total_paints = 0
            for range_key, range_data in data.items():
                output_file = RANGE_TO_FILE.get(range_key, f'turbodork_{range_key}.json')
                paints = range_data['paints']
                with open(output_file, 'w') as f:
                    json.dump(paints, f, indent=2)
                print(f"  {output_file}: {len(paints)} paints")
                total_paints += len(paints)
            print(f"\nTotal: {total_paints} paints across {len(data)} ranges")
        else:
            # Save all to single file
            all_paints = []
            for range_data in data.values():
                all_paints.extend(range_data['paints'])

            with open(args.output, 'w') as f:
                json.dump(all_paints, f, indent=2)
            print(f"\nSaved: {args.output} ({len(all_paints)} paints)")
    else:
        if args.range not in TURBODORK_RANGES:
            print(f"Unknown range: {args.range}")
            print(f"Available: {', '.join(TURBODORK_RANGES.keys())}")
            return

        paints = scrape_range(args.range, sample_colors, args.verbose, args.workers)

        if args.generate:
            output_file = RANGE_TO_FILE.get(args.range, f'turbodork_{args.range}.json')
        else:
            output_file = args.output

        with open(output_file, 'w') as f:
            json.dump(paints, f, indent=2)
        print(f"\nSaved: {output_file} ({len(paints)} paints)")


if __name__ == '__main__':
    main()
