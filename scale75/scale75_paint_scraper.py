#!/usr/bin/env python3
"""
Scale75 Paint Scraper

Scrapes scale75.com to build a paint database with hex colors.
Uses embedded JSON data from Shopify collection pages.

Requirements:
    pip install requests beautifulsoup4 pillow

Usage:
    python scale75_paint_scraper.py [--range RANGE_NAME] [--output OUTPUT_FILE]

Examples:
    # Scrape a single range
    python scale75_paint_scraper.py --range scalecolor

    # Scrape all ranges and generate individual JSON files
    python scale75_paint_scraper.py --range all --generate

    # Scrape without color sampling (faster, for testing)
    python scale75_paint_scraper.py --range scalecolor --no-colors

Output format matches the standard paint database schema:
{
    "brand": "Scale 75",
    "brandData": {},
    "category": "",
    "discontinued": false,
    "hex": "#8B4513",
    "id": "scale75-sc-00",
    "impcat": {"layerId": null, "shadeId": null},
    "name": "Black",
    "range": "Scale Color",
    "sku": "SC-00",
    "type": "opaque",
    "url": "https://scale75.com/en/products/..."
}
"""

import argparse
import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from PIL import Image

# Scale75 paint ranges - collection URL slugs and metadata
# Format: key -> {name, range, type, collection_slug}
SCALE75_RANGES = {
    "scalecolor": {
        "name": "Scale Color",
        "range": "Scale Color",
        "type": "opaque",
        "collection": "scalecolor-individual"
    },
    "fantasy-games": {
        "name": "Fantasy & Games",
        "range": "Fantasy & Games",
        "type": "opaque",
        "collection": "fantasy-games-individuales"
    },
    "metal-n-alchemy": {
        "name": "Metal n' Alchemy",
        "range": "Metal n' Alchemy",
        "type": "metallic",
        "collection": "metal-n-alchemy-individuales"
    },
    "instant-colors": {
        "name": "Instant Colors",
        "range": "Instant Colors",
        "type": "contrast",
        "collection": "instant-individuales"
    },
    "artist": {
        "name": "Scalecolor Artist",
        "range": "Scalecolor Artist",
        "type": "opaque",
        "collection": "artist-individuales"
    },
    "inktensity": {
        "name": "Inktensity",
        "range": "Inktensity",
        "type": "ink",
        "collection": "inktensity-individuales"
    },
    "fx-fluor": {
        "name": "FX Fluor",
        "range": "FX Fluor",
        "type": "opaque",  # Fluorescent paints - using opaque as base type
        "collection": "fx-fluor-individuales"
    },
    "warfront": {
        "name": "Warfront",
        "range": "Warfront",
        "type": "opaque",
        "collection": "warfront-individuales"
    },
    "drop-paint": {
        "name": "Drop & Paint",
        "range": "Drop & Paint",
        "type": "opaque",
        "collection": "drop-paint-individuales"
    },
    "flow": {
        "name": "Flow",
        "range": "Flow",
        "type": "opaque",
        "collection": "flow-individuales"
    },
    "scalecolor-games": {
        "name": "Scalecolor Games",
        "range": "Scalecolor Games",
        "type": "opaque",
        "collection": "scalecolor-games-individuales"
    },
}

# Type overrides based on name keywords
# Valid types: opaque, metallic, wash, ink, transparent, contrast, technical, spray, primer, varnish, thinner, air
TYPE_OVERRIDES = {
    'metallic': 'metallic',
    'metal': 'metallic',
    'gold': 'metallic',
    'silver': 'metallic',
    'copper': 'metallic',
    'bronze': 'metallic',
    'brass': 'metallic',
    'alchemy': 'metallic',
    'chrome': 'metallic',
    'ink': 'ink',
    'wash': 'wash',
    'primer': 'primer',
    'varnish': 'varnish',
    'glaze': 'transparent',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

BASE_URL = "https://scale75.com"


def to_title_case(name: str) -> str:
    """Convert name to title case: 'DECAY BLACK' -> 'Decay Black'"""
    if not name:
        return name
    # Decode HTML entities first
    name = html.unescape(name)
    words = name.split()
    result = []
    for word in words:
        # Handle all-caps or all-lowercase
        if word.upper() == word or word.lower() == word:
            word = word.title()
        result.append(word)
    return ' '.join(result)


def get_paint_type(name: str, default_type: str) -> str:
    """Determine paint type from name keywords."""
    name_lower = name.lower()
    for keyword, paint_type in TYPE_OVERRIDES.items():
        if keyword in name_lower:
            return paint_type
    return default_type


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


def extract_products_from_page(soup: BeautifulSoup) -> list:
    """Extract product data from embedded JSON in the page."""
    products = []

    # Get raw HTML text for regex parsing
    html_text = str(soup)

    # Extract individual product objects using a pattern that captures each product
    # Products have structure: {"id":..., "handle":"...", "variants":[{"sku":"..."}]}
    product_pattern = r'\{"id":(\d+),"gid":"[^"]+","vendor":"[^"]*","type":"[^"]*","handle":"([^"]+)","variants":\[\{"id":\d+,"price":\d+,"name":"([^"]+)","public_title":[^,]*,"sku":"([^"]+)"\}'

    matches = re.findall(product_pattern, html_text)

    for match in matches:
        product_id, handle, name, sku = match
        products.append({
            'id': int(product_id),
            'handle': handle,
            'name': name,
            'sku': sku,
        })

    # Fallback: Try the JSON array approach if regex didn't find anything
    if not products:
        for script in soup.find_all('script'):
            script_text = script.string or ''

            # Look for Shopify analytics meta containing products array
            if '"products":' in script_text:
                # Extract products array using regex - use greedy match
                match = re.search(r'"products":\s*(\[.*\])\s*,\s*"', script_text, re.DOTALL)
                if match:
                    try:
                        products_json = match.group(1)
                        # Clean up any trailing commas that might break JSON parsing
                        products_json = re.sub(r',\s*]', ']', products_json)
                        products_data = json.loads(products_json)

                        for product in products_data:
                            variants = product.get('variants', [])
                            if variants:
                                variant = variants[0]
                                products.append({
                                    'id': product.get('id'),
                                    'handle': product.get('handle'),
                                    'name': variant.get('name') or product.get('title', ''),
                                    'sku': variant.get('sku', ''),
                                    'price': variant.get('price'),
                                })
                    except json.JSONDecodeError as e:
                        print(f"      Warning: Failed to parse products JSON: {e}")

    return products


def extract_product_images(soup: BeautifulSoup) -> dict:
    """Extract product image URLs from the page."""
    images = {}

    # Find product cards with images
    for card in soup.select('.card-wrapper, .product-card, .product-item'):
        # Try to find the product link/handle
        link = card.select_one('a[href*="/products/"]')
        if not link:
            continue

        href = link.get('href', '')
        handle_match = re.search(r'/products/([^/?]+)', href)
        if not handle_match:
            continue

        handle = handle_match.group(1)

        # Find image
        img = card.select_one('img')
        if img:
            # Try srcset for highest resolution, fallback to src
            srcset = img.get('srcset', '')
            src = img.get('src') or img.get('data-src', '')

            img_url = None
            if srcset:
                # Parse srcset to get highest resolution
                parts = srcset.split(',')
                best_url = None
                best_width = 0
                for part in parts:
                    part = part.strip()
                    match = re.match(r'(\S+)\s+(\d+)w', part)
                    if match:
                        url, width = match.groups()
                        if int(width) > best_width:
                            best_width = int(width)
                            best_url = url
                if best_url:
                    img_url = best_url

            if not img_url and src:
                img_url = src

            if img_url:
                # Make URL absolute if needed
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                elif img_url.startswith('/'):
                    img_url = BASE_URL + img_url

                images[handle] = img_url

    return images


def get_product_images(handle: str) -> tuple:
    """Fetch product page to get both image URLs (main and swatch).

    Returns (main_image_url, swatch_image_url) tuple.
    The swatch image is typically the second image and shows the paint color.
    """
    try:
        url = f"{BASE_URL}/en/products/{handle}"
        soup = fetch_page(url)

        # Find all unique image URLs from the page
        image_urls = []
        seen = set()

        # Look for product images in various locations
        for img in soup.select('img[src*="/files/"], img[data-src*="/files/"]'):
            src = img.get('src') or img.get('data-src', '')
            if src and '/files/' in src:
                # Extract base filename without query params
                base_match = re.search(r'/files/(\d+)\.jpg', src)
                if base_match:
                    file_num = base_match.group(1)
                    if file_num not in seen:
                        seen.add(file_num)
                        if src.startswith('//'):
                            src = 'https:' + src
                        elif src.startswith('/'):
                            src = BASE_URL + src
                        # Remove width params to get full size
                        src = re.sub(r'\?.*$', '', src)
                        image_urls.append((int(file_num), src))

        # Sort by file number
        image_urls.sort(key=lambda x: x[0])

        if len(image_urls) >= 2:
            # Return main (higher number) and swatch (lower number)
            return (image_urls[-1][1], image_urls[0][1])
        elif len(image_urls) == 1:
            return (image_urls[0][1], image_urls[0][1])

        return (None, None)
    except Exception as e:
        print(f"      Warning: Failed to fetch product images for {handle}: {e}")
        return (None, None)


def sample_color_from_image(img_url: str, verbose: bool = False, is_bottle_only: bool = False) -> str:
    """Download image and sample the paint color.

    For swatch images: horizontal stripe of paint color at y=0.5
    For bottle-only images: paint visible in bottle body at y=0.4
    """
    try:
        response = requests.get(img_url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content)).convert('RGB')
        width, height = img.size

        if is_bottle_only:
            # Bottle-only images: paint color visible through bottle at y=0.4
            sample_regions = [
                # Bottle body region where paint is visible
                (int(width * 0.40), int(height * 0.40)),
                (int(width * 0.45), int(height * 0.40)),
                (int(width * 0.50), int(height * 0.40)),
                (int(width * 0.55), int(height * 0.40)),
                (int(width * 0.60), int(height * 0.40)),
                # Slightly above and below
                (int(width * 0.50), int(height * 0.35)),
                (int(width * 0.50), int(height * 0.45)),
                (int(width * 0.45), int(height * 0.38)),
                (int(width * 0.55), int(height * 0.42)),
            ]
        else:
            # Swatch images have the paint color as a horizontal stripe
            # in the middle of the image (y=0.5). Sample across the stripe.
            sample_regions = [
                # Center horizontal stripe at y=0.5
                (int(width * 0.35), int(height * 0.50)),
                (int(width * 0.40), int(height * 0.50)),
                (int(width * 0.45), int(height * 0.50)),
                (int(width * 0.50), int(height * 0.50)),
                (int(width * 0.55), int(height * 0.50)),
                (int(width * 0.60), int(height * 0.50)),
                (int(width * 0.65), int(height * 0.50)),
                # Slightly above and below center
                (int(width * 0.50), int(height * 0.48)),
                (int(width * 0.50), int(height * 0.52)),
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

            # Score: prefer saturated, non-white, non-black colors
            max_c = max(r, g, b)
            min_c = min(r, g, b)
            saturation = (max_c - min_c) / max(max_c, 1) if max_c > 0 else 0
            brightness = (r + g + b) / 3

            # Skip near-white or near-black (likely background/shadows)
            if brightness > 245 or brightness < 10:
                continue

            # Prefer mid-brightness, saturated colors
            brightness_penalty = abs(brightness - 127) / 127
            score = saturation * (1 - brightness_penalty * 0.3) + 0.1

            if score > best_score:
                best_score = score
                best_color = (r, g, b)

        if best_color:
            hex_color = "#{:02X}{:02X}{:02X}".format(*best_color)
            if verbose:
                print(f"        -> {hex_color} (score: {best_score:.3f})")
            return hex_color

        # Fallback: sample from center
        x, y = int(width * 0.50), int(height * 0.55)
        r, g, b = img.getpixel((x, y))
        return "#{:02X}{:02X}{:02X}".format(r, g, b)

    except Exception as e:
        print(f"        Error sampling color: {e}")
        return None


def sample_paint_color(paint: dict, verbose: bool = False) -> dict:
    """Sample color for a single paint. Returns the paint dict with hex added."""
    handle = paint.get('handle')
    if handle:
        # Get swatch image URL - prefer the swatch image for color sampling
        swatch_url = paint.get('swatch_url')
        main_url = paint.get('img_url')

        if not swatch_url:
            # Fetch both images from product page, use swatch for sampling
            main_url, swatch_url = get_product_images(handle)
            if main_url:
                paint['img_url'] = main_url
            if swatch_url:
                paint['swatch_url'] = swatch_url

        # Check if we only have one image (bottle only, no swatch)
        is_bottle_only = (main_url == swatch_url) or (swatch_url is None)
        img_url = swatch_url or main_url

        if img_url:
            paint['hex'] = sample_color_from_image(img_url, verbose, is_bottle_only)
        else:
            paint['hex'] = None
    return paint


def scrape_range(range_key: str, sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> list:
    """Scrape all paints from a Scale75 range."""
    if range_key not in SCALE75_RANGES:
        print(f"Unknown range: {range_key}")
        return []

    range_info = SCALE75_RANGES[range_key]
    range_name = range_info['name']
    collection = range_info['collection']
    default_type = range_info['type']

    print(f"\n{'='*60}")
    print(f"Scraping: {range_name} ({range_key})")
    print('='*60)

    all_products = []
    all_images = {}
    page = 1

    while True:
        url = f"{BASE_URL}/en/collections/{collection}"
        if page > 1:
            url += f"?page={page}"

        try:
            soup = fetch_page(url)

            # Extract products from embedded JSON
            products = extract_products_from_page(soup)

            # Extract image URLs from the page
            images = extract_product_images(soup)
            all_images.update(images)

            if not products:
                if page == 1:
                    print(f"    No products found for: {range_key}")
                break

            print(f"    Page {page}: {len(products)} products")
            all_products.extend(products)

            # Check if there's a next page link
            next_link = soup.select_one('a[aria-label="Next page"], a.next, [rel="next"]')
            if next_link:
                page += 1
                time.sleep(0.5)  # Be polite
            else:
                break

        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break

    # Add image URLs to products
    for product in all_products:
        handle = product.get('handle')
        if handle and handle in all_images:
            product['img_url'] = all_images[handle]

    # Sample colors if requested
    if sample_colors and all_products:
        print(f"    Sampling colors ({max_workers} threads)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(sample_paint_color, paint, verbose): paint for paint in all_products}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                paint = future.result()
                sku = paint.get('sku') or '?'
                hex_val = paint.get('hex') or 'failed'
                if verbose or completed % 10 == 0 or completed == len(all_products):
                    print(f"      [{completed}/{len(all_products)}] {sku}: {hex_val}")

    # Add metadata to each product
    for product in all_products:
        product['paint_type'] = get_paint_type(product.get('name', ''), default_type)
        product['range_name'] = range_info['range']
        product['product_url'] = f"{BASE_URL}/en/products/{product.get('handle', '')}"

    print(f"  Total: {len(all_products)} paints")
    return all_products


def scrape_all_ranges(sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> dict:
    """Scrape all Scale75 ranges."""
    all_data = {}

    for range_key in SCALE75_RANGES.keys():
        paints = scrape_range(range_key, sample_colors, verbose, max_workers)
        all_data[range_key] = {
            'name': SCALE75_RANGES[range_key]['name'],
            'range': SCALE75_RANGES[range_key]['range'],
            'paints': paints
        }
        time.sleep(1)  # Be polite between ranges

    return all_data


def generate_catalogue(scraped_data: list, range_name: str) -> list:
    """Generate a fresh catalogue in standard format from scraped data."""
    catalogue = []
    seen_skus = {}

    for paint in scraped_data:
        sku = paint.get('sku', '')
        if not sku:
            continue

        # Skip duplicates
        if sku in seen_skus:
            continue

        name = to_title_case(paint.get('name', ''))

        entry = {
            "brand": "Scale 75",
            "brandData": {},
            "category": "",
            "discontinued": False,
            "hex": paint.get('hex', ''),
            "id": f"scale75-{sku.lower().replace(' ', '-')}",
            "impcat": {
                "layerId": None,
                "shadeId": None
            },
            "name": name,
            "range": paint.get('range_name', range_name),
            "sku": sku,
            "type": paint.get('paint_type', 'opaque'),
            "url": paint.get('product_url', '')
        }
        seen_skus[sku] = len(catalogue)
        catalogue.append(entry)

    # Sort by SKU
    catalogue.sort(key=lambda x: x['sku'])
    return catalogue


# Mapping of range keys to output filenames
RANGE_TO_FILE = {
    'scalecolor': 'scale75_scale_color.json',
    'fantasy-games': 'scale75_fantasy_games.json',
    'metal-n-alchemy': 'scale75_metal_alchemy.json',
    'instant-colors': 'scale75_instant_colors.json',
    'artist': 'scale75_artist.json',
    'inktensity': 'scale75_inktensity.json',
    'fx-fluor': 'scale75_fx_fluor.json',
    'warfront': 'scale75_warfront.json',
    'drop-paint': 'scale75_drop_paint.json',
    'flow': 'scale75_flow.json',
    'scalecolor-games': 'scale75_scalecolor_games.json',
}


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Scale75 paint data with hex colors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available ranges:
  scalecolor          Scale Color (main acrylic line)
  fantasy-games       Fantasy & Games
  metal-n-alchemy     Metal n' Alchemy (metallics)
  instant-colors      Instant Colors (speed paints)
  artist              Scalecolor Artist
  inktensity          Inktensity (inks)
  fx-fluor            FX Fluor (fluorescent)
  warfront            Warfront
  drop-paint          Drop & Paint
  flow                Flow
  scalecolor-games    Scalecolor Games

  all                 Scrape everything
        """
    )
    parser.add_argument('--range', '-r', default='all',
                       help='Range to scrape (default: all)')
    parser.add_argument('--output', '-o', default='scale75_paints.json',
                       help='Output JSON file')
    parser.add_argument('--no-colors', action='store_true',
                       help='Skip color sampling')
    parser.add_argument('--workers', '-w', type=int, default=8,
                       help='Number of parallel threads for image sampling (default: 8)')
    parser.add_argument('--generate', '-g', action='store_true',
                       help='Generate fresh catalogue files instead of single output')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')

    args = parser.parse_args()
    sample_colors = not args.no_colors

    if args.range == 'all':
        print("Scraping ALL Scale75 ranges...")
        data = scrape_all_ranges(sample_colors, args.verbose, args.workers)

        if args.generate:
            # Generate separate files per range
            print(f"\nGenerating {len(data)} catalogue files:")
            for range_key, range_data in data.items():
                output_file = RANGE_TO_FILE.get(range_key, f'scale75_{range_key}.json')
                catalogue = generate_catalogue(range_data['paints'], range_data['range'])
                with open(output_file, 'w') as f:
                    json.dump(catalogue, f, indent=2)
                print(f"  {output_file}: {len(catalogue)} paints")
            print("\nDone!")
        else:
            # Flatten all paints
            all_paints = []
            for range_data in data.values():
                all_paints.extend(range_data['paints'])

            with open(args.output, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"\nSaved: {args.output}")
    else:
        if args.range not in SCALE75_RANGES:
            print(f"Unknown range: {args.range}")
            print(f"Available: {', '.join(SCALE75_RANGES.keys())}")
            return

        paints = scrape_range(args.range, sample_colors, args.verbose, args.workers)

        if args.generate:
            output_file = RANGE_TO_FILE.get(args.range, f'scale75_{args.range}.json')
            range_name = SCALE75_RANGES[args.range]['range']
            catalogue = generate_catalogue(paints, range_name)
            with open(output_file, 'w') as f:
                json.dump(catalogue, f, indent=2)
            print(f"\nGenerated {output_file}: {len(catalogue)} paints")
        else:
            output_data = {
                'range': args.range,
                'name': SCALE75_RANGES[args.range]['name'],
                'paints': paints
            }
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"\nSaved: {args.output}")


if __name__ == '__main__':
    main()
