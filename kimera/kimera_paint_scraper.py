#!/usr/bin/env python3
"""
Kimera Kolors Paint Scraper

Scrapes elgrecominiatures.co.uk (Shopify) to build a paint database with hex colors
for Kimera Kolors paints.

Kimera Kolors is known for high-end artist-quality paints with pure single-pigment formulations.

Requirements:
    pip install requests pillow

Usage:
    python kimera_paint_scraper.py [--range RANGE_NAME] [--output OUTPUT_FILE]

Examples:
    # Scrape a single range
    python kimera_paint_scraper.py --range pure-pigments

    # Scrape all ranges and generate individual JSON files
    python kimera_paint_scraper.py --range all --generate

    # Scrape without color sampling (faster, for testing)
    python kimera_paint_scraper.py --range signatures --no-colors

Output format matches the standard paint database schema:
{
    "brand": "Kimera",
    "brandData": {"pigmentCode": "PY42", "singlePigment": true},
    "category": "",
    "discontinued": false,
    "hex": "#RRGGBB",
    "id": "kimera-the-red",
    "impcat": {"layerId": null, "shadeId": null},
    "name": "The Red",
    "range": "Pure Pigments",
    "sku": "KM-PPS1-03",
    "type": "opaque",
    "url": "https://www.elgrecominiatures.co.uk/products/the-red-series-1"
}
"""

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import requests
from PIL import Image

# Base URL for Kimera products at El Greco Miniatures (Shopify store)
BASE_URL = "https://www.elgrecominiatures.co.uk"
COLLECTION_URL = f"{BASE_URL}/collections/kimera-kolors/products.json"

# Mapping for output filenames
RANGE_TO_FILE = {
    'pure-pigments': 'kimera_pure_pigments.json',
    'signatures': 'kimera_signatures.json',
}

# Known pigment codes for Kimera Pure Pigments (from product documentation)
# Map by normalized name (lowercase, no series suffix)
PIGMENT_CODES = {
    # Base Set
    "the white": "PW6",           # Titanium White
    "carbon black": "PBk7",       # Carbon Black
    "the red": "PR254",           # Pyrrole Red
    "orange": "PO73",             # Pyranthrone Orange
    "warm yellow": "PY83",        # Diarylide Yellow HR
    "cold yellow": "PY175",       # Benzimidazolone Yellow
    "phthalo blue (red shade)": "PB15:1",   # Phthalo Blue RS
    "phthalo blue (green shade)": "PB15:3", # Phthalo Blue GS
    "magenta": "PR122",           # Quinacridone Magenta
    "phthalo green": "PG36",      # Phthalo Green
    "violet": "PV23",             # Dioxazine Violet
    "yellow oxide": "PY42",       # Yellow Iron Oxide
    "red oxide": "PR101",         # Red Iron Oxide
    "satin medium": None,         # Not a pigment
    # Expansion Set (Colors of Nature)
    "oxide brown dark": "PBr7",   # Burnt Umber
    "oxide brown medium": "PBr7", # Raw Umber
    "oxide brown light": "PY42",  # Yellow Ochre
    "dark ochre": "PY43",         # Yellow Iron Oxide
    "mars orange": "PR101",       # Mars Orange (Red Iron Oxide)
    "honeymoon yellow": "PY42",   # Yellow Ochre
    "diarylide yellow": "PY170",  # Diarylide Yellow
    "alizarine crimson": "PR177", # Anthraquinone Red
    "royal brown": "PBr7",        # Raw Umber
    "ultramarine blue": "PB29",   # Ultramarine Blue
    "toludine red": "PR3",        # Toluidine Red
    "purple": "PV23",             # Carbazole Violet
    "oxide green": "PG17",        # Chromium Oxide Green
    "cobalt bluegreen": "PB36",   # Cobalt Blue Green
}

# Artist mapping for Signatures
SIGNATURE_ARTISTS = {
    "pisarski": "Michal Pisarski",
    "cartacci": "Danilo Cartacci",
    "karlsson": "Robert Karlsson",
    "russo": "Fabrizio Russo",
}

# SKU prefixes for categorization
SKU_CATEGORIES = {
    "KM-PPS1": "Pure Pigments",      # Base set
    "KMP-": "Pure Pigments",          # Expansion set
    "KM-SSMP": "Signatures",          # Pisarski signatures
    "KM-SSDC": "Signatures",          # Cartacci signatures
    "KM-SSRK": "Signatures",          # Karlsson signatures (if exists)
    "KM-SSFR": "Signatures",          # Russo signatures (if exists)
}

# Products to exclude (sets, accessories, etc.)
EXCLUDE_KEYWORDS = [
    'set', 'palette', 'blend', 'velvet inks', 'signature blend',
    'masclans', 'richiero'  # These are sets not individual paints
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
}


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


def get_all_products() -> list:
    """Fetch all Kimera products from El Greco Shopify API."""
    products = []
    page = 1

    while True:
        url = f"{COLLECTION_URL}?page={page}&limit=250"
        print(f"    Fetching: {url}")

        try:
            data = fetch_json(url)
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
            break

    return products


def is_individual_paint(product: dict) -> bool:
    """Filter out sets, accessories, and non-individual paints."""
    title = (product.get('title') or '').lower()
    handle = (product.get('handle') or '').lower()
    product_type = (product.get('product_type') or '').lower()

    # Check exclusion keywords
    for keyword in EXCLUDE_KEYWORDS:
        if keyword in title or keyword in handle:
            return False

    # Only include products with paint-related SKUs
    variants = product.get('variants', [])
    if variants:
        sku = (variants[0].get('sku') or '').upper()
        # Check if SKU matches known paint patterns
        if sku.startswith('KM-PPS1') or sku.startswith('KMP-') or sku.startswith('KM-SS'):
            return True

    return False


def get_range_from_sku(sku: str) -> str:
    """Determine paint range from SKU prefix."""
    sku_upper = sku.upper()
    for prefix, range_name in SKU_CATEGORIES.items():
        if sku_upper.startswith(prefix):
            return range_name
    return "Pure Pigments"  # Default


def normalize_name(title: str) -> str:
    """Normalize paint name by removing series suffix."""
    # Remove " - Series X" suffix
    name = re.sub(r'\s*-\s*Series\s*\d+\s*$', '', title, flags=re.IGNORECASE)
    return name.strip()


def get_pigment_code(name: str) -> str:
    """Look up pigment code by normalized name."""
    name_lower = name.lower().strip()
    return PIGMENT_CODES.get(name_lower)


def get_artist_from_name(name: str) -> str:
    """Extract artist name from signature paint name."""
    name_lower = name.lower()
    for prefix, artist in SIGNATURE_ARTISTS.items():
        if name_lower.startswith(prefix):
            return artist
    return None


def sample_color_from_image(img_url: str, verbose: bool = False) -> str:
    """Download image and sample the paint color."""
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

        # El Greco images vary:
        # - Bottle images: paint visible in upper-middle area (30-50% from top)
        # - Swatch PNGs (KMP-*): solid color swatches, sample center
        # - Signature images: bottle on LEFT side of dark background (~30% x, 35% y)

        # Check if this is a swatch PNG (usually solid color)
        is_swatch = 'kmp-' in img_url.lower() and img_url.lower().endswith('.png')
        # Signature images have bottle on left side
        is_signature = 'signature-' in img_url.lower()

        if is_swatch:
            # For swatch PNGs, sample from center - they're mostly solid color
            sample_regions = [
                (int(width * 0.5), int(height * 0.5)),
                (int(width * 0.4), int(height * 0.5)),
                (int(width * 0.6), int(height * 0.5)),
                (int(width * 0.5), int(height * 0.4)),
                (int(width * 0.5), int(height * 0.6)),
            ]
        elif is_signature:
            # For signature images, bottle is on the LEFT side
            # Different artists have slightly different layouts
            # Sample broadly across left side where bottle appears
            sample_regions = [
                # Pisarski-style (bottle around x=0.30)
                (int(width * 0.30), int(height * 0.35)),
                (int(width * 0.30), int(height * 0.40)),
                # Cartacci-style (bottle more left, paint lower)
                (int(width * 0.20), int(height * 0.45)),
                (int(width * 0.25), int(height * 0.45)),
                (int(width * 0.20), int(height * 0.50)),
                (int(width * 0.25), int(height * 0.50)),
                # Additional coverage
                (int(width * 0.15), int(height * 0.45)),
                (int(width * 0.30), int(height * 0.45)),
            ]
        else:
            # For bottle images, sample from upper-middle area
            sample_regions = [
                (int(width * 0.50), int(height * 0.35)),
                (int(width * 0.45), int(height * 0.35)),
                (int(width * 0.55), int(height * 0.35)),
                (int(width * 0.50), int(height * 0.40)),
                (int(width * 0.50), int(height * 0.30)),
                (int(width * 0.50), int(height * 0.45)),
            ]

        best_color = None
        best_score = -1

        for x, y in sample_regions:
            # Sample a small region around each point
            colors = []
            for dx in range(-8, 9, 2):
                for dy in range(-8, 9, 2):
                    px = max(0, min(x + dx, width - 1))
                    py = max(0, min(y + dy, height - 1))
                    colors.append(img.getpixel((px, py)))

            # Average the sampled colors
            r = sum(c[0] for c in colors) // len(colors)
            g = sum(c[1] for c in colors) // len(colors)
            b = sum(c[2] for c in colors) // len(colors)

            # Score based on saturation and brightness
            max_c = max(r, g, b)
            min_c = min(r, g, b)
            saturation = (max_c - min_c) / max(max_c, 1) if max_c > 0 else 0
            brightness = (r + g + b) / 3

            # Skip near-white or near-black (likely background)
            if brightness > 245 or brightness < 10:
                continue

            # Prefer saturated, mid-brightness colors
            brightness_penalty = abs(brightness - 127) / 127
            score = saturation * (1 - brightness_penalty * 0.3) + 0.1

            if score > best_score:
                best_score = score
                best_color = (r, g, b)

        if best_color:
            return "#{:02X}{:02X}{:02X}".format(*best_color)

        # Fallback to center sample
        x, y = int(width * 0.5), int(height * 0.5)
        r, g, b = img.getpixel((x, y))
        return "#{:02X}{:02X}{:02X}".format(r, g, b)

    except Exception as e:
        if verbose:
            print(f"        Error sampling color: {e}")
        return None


def slugify(name: str) -> str:
    """Convert name to URL-friendly slug."""
    slug = name.lower()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug


def process_product(product: dict, sample_colors: bool = True, verbose: bool = False) -> dict:
    """Process a single product and return paint entry."""
    title = product.get('title', '')
    handle = product.get('handle', '')
    variants = product.get('variants', [])
    images = product.get('images', [])

    # Get SKU from first variant
    sku = variants[0].get('sku', '') if variants else ''

    # Get image URL
    img_url = images[0].get('src', '') if images else ''

    # Normalize name
    name = normalize_name(title)

    # Determine range from SKU
    range_name = get_range_from_sku(sku)

    # Determine paint type
    paint_type = 'opaque'
    if 'satin medium' in name.lower():
        paint_type = 'medium'

    # Sample color from image
    hex_color = None
    if sample_colors and img_url:
        hex_color = sample_color_from_image(img_url, verbose)

    # Build brand data
    brand_data = {}

    # Add pigment code if known
    pigment_code = get_pigment_code(name)
    if pigment_code:
        brand_data['pigmentCode'] = pigment_code
        brand_data['singlePigment'] = True

    # Add artist for Signatures
    artist = get_artist_from_name(name)
    if artist:
        brand_data['artist'] = artist

    # Create ID
    paint_id = f"kimera-{slugify(name)}"

    # Build product URL
    url = f"{BASE_URL}/products/{handle}"

    return {
        "brand": "Kimera",
        "brandData": brand_data,
        "category": "",
        "discontinued": False,
        "hex": hex_color or "",
        "id": paint_id,
        "impcat": {"layerId": None, "shadeId": None},
        "name": name,
        "range": range_name,
        "sku": sku,
        "type": paint_type,
        "url": url
    }


def scrape_all(sample_colors: bool = True, verbose: bool = False, max_workers: int = 8) -> dict:
    """Scrape all Kimera paints and categorize by range."""
    print("Fetching all Kimera products from El Greco...")

    products = get_all_products()
    print(f"Found {len(products)} total products")

    # Filter to individual paints only
    paint_products = [p for p in products if is_individual_paint(p)]
    print(f"Filtered to {len(paint_products)} individual paints")

    # Process all products
    all_paints = []

    if sample_colors and max_workers > 1:
        print(f"Processing paints ({max_workers} threads)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_product, p, True, verbose): p
                for p in paint_products
            }
            completed = 0
            for future in as_completed(futures):
                completed += 1
                try:
                    paint = future.result()
                    all_paints.append(paint)
                    if verbose or completed % 5 == 0:
                        print(f"    [{completed}/{len(paint_products)}] {paint['name']}: {paint['hex']}")
                except Exception as e:
                    print(f"    Error processing product: {e}")
    else:
        for i, product in enumerate(paint_products):
            paint = process_product(product, sample_colors, verbose)
            all_paints.append(paint)
            if verbose:
                print(f"    [{i+1}/{len(paint_products)}] {paint['name']}: {paint['hex']}")

    # Categorize by range
    ranges = {}
    for paint in all_paints:
        range_name = paint['range']
        range_key = slugify(range_name)
        if range_key not in ranges:
            ranges[range_key] = {
                'name': range_name,
                'paints': []
            }
        ranges[range_key]['paints'].append(paint)

    # Sort paints within each range
    for range_data in ranges.values():
        range_data['paints'].sort(key=lambda x: x['name'].lower())

    return ranges


def main():
    parser = argparse.ArgumentParser(
        description='Scrape Kimera Kolors paint data with hex colors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available ranges:
  pure-pigments   Pure Pigments (Base Set + Expansion Set)
  signatures      Signatures (Artist series)

  all             Scrape everything
        """
    )
    parser.add_argument('--range', '-r', default='all',
                       help='Range to scrape (default: all)')
    parser.add_argument('--output', '-o', default='kimera_paints.json',
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

    print("Scraping Kimera Kolors paints...")
    data = scrape_all(sample_colors, args.verbose, args.workers)

    if args.range != 'all':
        # Filter to specific range
        range_key = args.range
        if range_key not in data:
            print(f"Unknown range: {range_key}")
            print(f"Available: {', '.join(data.keys())}")
            return
        data = {range_key: data[range_key]}

    if args.generate:
        # Generate separate files per range
        print(f"\nGenerating {len(data)} catalogue files:")
        total_paints = 0
        for range_key, range_data in data.items():
            output_file = RANGE_TO_FILE.get(range_key, f'kimera_{range_key}.json')
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

        all_paints.sort(key=lambda x: (x['range'], x['name'].lower()))

        with open(args.output, 'w') as f:
            json.dump(all_paints, f, indent=2)
        print(f"\nSaved: {args.output} ({len(all_paints)} paints)")


if __name__ == '__main__':
    main()
