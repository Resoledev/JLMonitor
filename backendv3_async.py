"""
John Lewis Price Monitor V3 - ASYNC VERSION
==========================================
Major optimizations:
1. Async HTTP fetching with aiohttp (5-10x faster)
2. Concurrent product processing with semaphore-controlled batches
3. Single price history load/save per cycle (not per product)
4. Connection pooling with keep-alive
5. Optimized CSV operations

Matches V2 logic exactly for:
- Category URL patterns and chunking
- Product extraction and variant handling
- Price history tracking
- Exclusion keywords
- CSV cleanup rules
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
import random
import json
import re
import logging
from discord_webhook import DiscordWebhook, DiscordEmbed
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
import os
import signal
import sys
import csv
from collections import defaultdict
from typing import List, Dict, Optional, Set, Tuple
import glob


# Configure directories and logging
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_DIR, 'logs')
STATE_DIR = os.path.join(PROJECT_DIR, 'state')
CSV_FILE = os.path.join(PROJECT_DIR, 'johnlewisv2.csv')
PRICE_HISTORY_FILE = os.path.join(STATE_DIR, 'price_history.json')
LOG_FILE = os.path.join(LOG_DIR, 'price_monitor.log')
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Discord webhook - load from environment
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1369560794769133609/g-XtphNUL0kMICbJj88viJ7t4bUSeJMgRUvOFevKZvBJUcWE-jcLke9epNrzaS0uH2Dl")


# Category configurations - Reduced to Clear categories
CATEGORY_URLS = {
    "Furniture": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/all-furniture-offers/reduced-to-clear/_/N-nt4yZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'furniture_state.json'),
        "log_tag": "Furniture"
    },
    "Lighting": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/all-lighting-offers/reduced-to-clear/_/N-nt1yZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'lighting_state.json'),
        "log_tag": "Lighting"
    },
    "Bedding": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/all-bedding-offers/reduced-to-clear/_/N-nt20Z1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'bedding_state.json'),
        "log_tag": "Bedding"
    },
    "Home Accessories": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/home-accessories-offers/reduced-to-clear/_/N-nt1tZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'home_accessories_state.json'),
        "log_tag": "Home Accessories"
    },
    "Towels & Bathroom": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/all-towel-bathroom-accessories-offers/reduced-to-clear/_/N-nt4zZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'bathroom_state.json'),
        "log_tag": "Towels & Bathroom"
    },
    "Furnishings": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/all-furnishing-offers/reduced-to-clear/_/N-nt51Z1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'furnishings_state.json'),
        "log_tag": "Furnishings"
    },
    "Christmas": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/christmas-offers/reduced-to-clear/_/N-nt1xZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'christmas_state.json'),
        "log_tag": "Christmas"
    },
    "Cook & Dine": {
        "url": "https://www.johnlewis.com/browse/special-offers/home-furniture-offers/all-cook-dine-offers/reduced-to-clear/_/N-nt4wZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'cook_dine_state.json'),
        "log_tag": "Cook & Dine"
    },
    "Fitness Equipment": {
        "url": "https://www.johnlewis.com/browse/sport-leisure/home-gym-equipment/view-all-fitness-machines/reduced-to-clear/_/N-eufZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'fitness_state.json'),
        "log_tag": "Fitness Equipment"
    },
    "Beauty & Fragrance": {
        "url": "https://www.johnlewis.com/browse/special-offers/beauty-fragrance-offers/reduced-to-clear/_/N-eg5Z1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 3,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'beauty_state.json'),
        "log_tag": "Beauty & Fragrance"
    },
    "Tablets & Computing": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/tablet-and-computing-offers/reduced-to-clear/_/N-5vqsZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'tablets_state.json'),
        "log_tag": "Tablets & Computing"
    },
    "Computer Accessories": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/computer-accessory-offers/reduced-to-clear/_/N-7eo5Z1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'computer_accessories_state.json'),
        "log_tag": "Computer Accessories"
    },
    "Phones & Mobile": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/phones-mobile-offers/reduced-to-clear/_/N-5vquZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'phones_state.json'),
        "log_tag": "Phones & Mobile"
    },
    "Sports Watches & Trackers": {
        "url": "https://www.johnlewis.com/browse/special-offers/sports-leisure-offers/activity-trackers-and-sports-watches/reduced-to-clear/_/N-7cjtZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'sports_watches_state.json'),
        "log_tag": "Sports Watches & Trackers"
    },
    "Coffee Machines": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/coffee-machine-offers/reduced-to-clear/_/N-eeyZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'coffee_machines_state.json'),
        "log_tag": "Coffee Machines"
    },
    "Food Preparation": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/food-preparation-offers/reduced-to-clear/_/N-eezZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'food_prep_state.json'),
        "log_tag": "Food Preparation"
    },
    "Health & Personal Care": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/health-personal-care-offers/reduced-to-clear/_/N-ef0Z1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'health_care_state.json'),
        "log_tag": "Health & Personal Care"
    },
    "Headphones": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/headphone-offers/reduced-to-clear/_/N-n5zrZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'headphones_state.json'),
        "log_tag": "Headphones"
    },
    "TVs": {
        "url": "https://www.johnlewis.com/browse/special-offers/electrical-offers/tv-offers/reduced-to-clear/_/N-efbZ1yzwd58?sortBy=discount",
        "min_discount": 50.0,
        "max_pages": 2,
        "max_products_per_page": 192,
        "state_file": os.path.join(STATE_DIR, 'tvs_state.json'),
        "log_tag": "TVs"
    }
}


USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"


# EXACT MATCH with V2 exclusions
EXCLUDED_KEYWORDS = [
    "kids", "baby", "bikini", "top", "bra", "hat", "bodysuit", "dress", "pyjama", "boys", "girls", "Knickers", "Blouse", "Cincher", "Children", "Swimsuit", "Skirt", "Briefs"
]


# Concurrency settings - TUNE THESE FOR SPEED vs RATE LIMIT
MAX_CONCURRENT_REQUESTS = 10  # Number of simultaneous HTTP requests
REQUEST_DELAY_MIN = 0.3       # Minimum delay between batches (seconds)
REQUEST_DELAY_MAX = 0.8       # Maximum delay between batches (seconds)
BATCH_SIZE = 10               # Products per batch

# Counters and constants - EXACT MATCH with V2
cycle_count = 0
ssl_error_count = 0
excluded_keyword_count = 0
NOTIFY_EVERY_CYCLES = 3
MAX_CHUNKS = 8
MAX_PAGE_REQUESTS = 50
RECENTLY_ADDED_HOURS = 24
DAYS_TO_KEEP_UNSEEN = 7


def get_headers():
    """Return headers with fixed User-Agent"""
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }


def clean_price(text: str) -> Optional[float]:
    """Clean price text to float - EXACT MATCH with V2"""
    if not text:
        return None
    text = re.split(r'\s*-\s*', text)[0]
    text = re.sub(r'[^\d.]', '', text)
    try:
        return float(text)
    except ValueError:
        return None


def extract_product_id(url: str) -> Optional[str]:
    """Extract product ID from URL - EXACT MATCH with V2"""
    match = re.search(r"p(\d+)$", url)
    if match:
        return match.group(1)
    logging.error(f"Failed to extract product ID from URL: {url}")
    return None


def normalize_url(url: str) -> str:
    """Normalize URL by removing query parameters - EXACT MATCH with V2"""
    parsed = urlparse(url)
    path = parsed.path.rstrip('/')
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def normalize_size(size: str) -> str:
    """Normalize size formats - EXACT MATCH with V2"""
    size = size.strip()
    size = re.sub(r'^(uk|eu)(\d+)$', r'\1 \2', size, flags=re.I)
    return size


def load_global_seen_products() -> Set[str]:
    """Load all product IDs from all category state files.

    This enables cross-category deduplication - a product is only 'New'
    in the first category that discovers it.
    """
    seen = set()
    state_pattern = os.path.join(STATE_DIR, '*_state.json')
    for state_file in glob.glob(state_pattern):
        # Skip price_history.json and category_state.json (master files)
        basename = os.path.basename(state_file)
        if basename in ('price_history.json', 'category_state.json'):
            continue
        try:
            with open(state_file, 'r') as f:
                data = json.load(f)
                seen.update(data.keys())
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Could not load {state_file}: {e}")
    logging.info(f"Loaded {len(seen)} product IDs from global state files")
    return seen


class PriceHistoryManager:
    """
    Manages price history with single load/save per cycle.
    OPTIMIZATION: Avoids repeated file I/O.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.data: Dict = {}
        self.dirty = False

    def load(self):
        """Load price history from file"""
        try:
            with open(self.filepath, 'r') as f:
                self.data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.data = {}
        self.dirty = False
        logging.info(f"Loaded price history: {len(self.data)} products")

    def save(self):
        """Save price history to file (only if modified)"""
        if not self.dirty:
            return
        try:
            with open(self.filepath, 'w') as f:
                json.dump(self.data, f, indent=2)
            self.dirty = False
            logging.info(f"Saved price history: {len(self.data)} products")
        except Exception as e:
            logging.error(f"Failed to save price history: {e}")

    def update(self, product_id: str, current_price: float, product_name: str) -> bool:
        """Update price history and return if recently reduced - MATCHES V2 LOGIC"""
        current_time = datetime.now().isoformat()

        if product_id not in self.data:
            self.data[product_id] = {
                "name": product_name,
                "initial_price": current_price,
                "prices": [{"price": current_price, "timestamp": current_time}],
                "recently_reduced": False,
                "reduction_from_initial": 0.0
            }
            self.dirty = True
            logging.info(f"New product tracked: {product_name} at £{current_price}")
            return False

        entry = self.data[product_id]
        entry["prices"].append({"price": current_price, "timestamp": current_time})
        entry["prices"] = entry["prices"][-20:]  # Keep last 20

        initial_price = entry.get("initial_price")
        if not initial_price:
            initial_price = entry["prices"][0]["price"]
            entry["initial_price"] = initial_price

        reduction_from_initial = 0.0
        if current_price is not None and initial_price is not None and initial_price > 0:
            reduction_from_initial = ((initial_price - current_price) / initial_price) * 100

        entry["reduction_from_initial"] = reduction_from_initial

        # Check for recent reduction - EXACT MATCH with V2
        recent_reduction_threshold = 5.0
        is_significantly_reduced = reduction_from_initial >= recent_reduction_threshold

        recent_drop = False
        if len(entry["prices"]) >= 3:
            last_3_prices = [p["price"] for p in entry["prices"][-3:] if p["price"] is not None]
            if len(last_3_prices) >= 2:
                recent_drop = last_3_prices[-1] < last_3_prices[0]

        entry["recently_reduced"] = is_significantly_reduced or recent_drop
        self.dirty = True

        if entry["recently_reduced"]:
            logging.info(f"Recently reduced: {product_name} - Initial: £{initial_price}, Current: £{current_price}, Reduction: {reduction_from_initial:.1f}%")

        return entry.get("recently_reduced", False)

    def get_recently_reduced_ids(self) -> Set[str]:
        """Get set of recently reduced product IDs"""
        return {pid for pid, data in self.data.items() if data.get("recently_reduced", False)}


class StateManager:
    """
    Manages category state with single load/save per cycle.
    OPTIMIZATION: Avoids repeated file I/O.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.data: Dict = {}
        self.dirty = False

    def load(self):
        """Load state from file - MATCHES V2 LOGIC"""
        try:
            with open(self.filepath, "r") as f:
                raw_state = json.load(f)
                self.data = {}
                for product_id, data in raw_state.items():
                    try:
                        self.data[product_id] = {
                            "name": data.get("name"),
                            "url": data.get("url", "Unknown"),
                            "original_price": float(data.get("original_price")) if data.get("original_price") else None,
                            "latest_price": float(data.get("latest_price")) if data.get("latest_price") else None,
                            "stock_status": data.get("stock_status", "Unknown"),
                            "first_seen": data.get("first_seen"),
                            "recently_reduced": data.get("recently_reduced", False),
                            "reduced_timestamp": data.get("reduced_timestamp")
                        }
                    except (ValueError, TypeError):
                        continue
        except (FileNotFoundError, json.JSONDecodeError):
            self.data = {}
        self.dirty = False
        logging.info(f"Loaded state from {self.filepath}: {len(self.data)} items")

    def save(self, current_product_ids: Set[str]):
        """Save state to file - MATCHES V2 LOGIC including cleanup"""
        # Remove out of stock products not seen this cycle
        for product_id in list(self.data.keys()):
            if product_id in current_product_ids:
                continue
            stock_status = self.data[product_id].get("stock_status", "")
            if stock_status == "Out of Stock":
                logging.info(f"Removing out of stock: {product_id}")
                del self.data[product_id]
                self.dirty = True

        try:
            with open(self.filepath, "w") as f:
                json.dump(self.data, f, indent=4)
            self.dirty = False
            logging.info(f"Saved state to {self.filepath}: {len(self.data)} items")
        except Exception as e:
            logging.error(f"Failed to save state: {e}")

    def update_product(self, product: Dict, current_time: str):
        """Update a single product in state - MATCHES V2 LOGIC"""
        product_id = product["product_id"]

        if any(kw.lower() in product["name"].lower() for kw in EXCLUDED_KEYWORDS):
            return

        # Preserve first_seen from existing state
        first_seen = self.data.get(product_id, {}).get('first_seen', current_time)

        # Check for price reduction (V2 logic)
        recently_reduced = False
        reduced_timestamp = None

        if product_id in self.data:
            old_price = self.data[product_id].get("latest_price")
            current_price = product["current_price"]

            # If price has dropped
            if old_price is not None and current_price is not None and current_price < old_price:
                recently_reduced = True
                reduced_timestamp = current_time
            # If price hasn't changed, keep existing recently_reduced status if recent
            elif self.data[product_id].get("recently_reduced"):
                prev_ts = self.data[product_id].get("reduced_timestamp")
                if prev_ts:
                    try:
                        reduced_dt = datetime.fromisoformat(prev_ts)
                        if datetime.now() - reduced_dt < timedelta(hours=168):  # 1 week
                            recently_reduced = True
                            reduced_timestamp = prev_ts
                    except ValueError:
                        pass

        self.data[product_id] = {
            "name": product["name"],
            "url": product["url"],
            "original_price": product["original_price"],
            "latest_price": product["current_price"],
            "stock_status": product["stock_status"],
            "first_seen": first_seen,
            "recently_reduced": recently_reduced,
            "reduced_timestamp": reduced_timestamp
        }
        self.dirty = True

    def is_recently_added(self, product_id: str) -> bool:
        """Check if product was added within threshold - MATCHES V2"""
        if product_id not in self.data:
            return True

        first_seen_str = self.data[product_id].get('first_seen')
        if not first_seen_str:
            return False

        try:
            first_seen = datetime.fromisoformat(first_seen_str)
            hours_since_added = (datetime.now() - first_seen).total_seconds() / 3600
            return hours_since_added <= RECENTLY_ADDED_HOURS
        except:
            return False


# Global managers (initialized per cycle)
price_history_manager: Optional[PriceHistoryManager] = None


async def fetch_page_async(session: aiohttp.ClientSession, url: str, retries: int = 3) -> Optional[str]:
    """Fetch a single page asynchronously with retries - MATCHES V2 retry logic"""
    for attempt in range(retries):
        try:
            async with session.get(url, headers=get_headers(), timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.warning(f"HTTP {response.status} for {url} (attempt {attempt+1})")
        except asyncio.TimeoutError:
            logging.error(f"Timeout fetching {url} (attempt {attempt+1})")
        except Exception as e:
            logging.error(f"Error fetching {url} (attempt {attempt+1}): {e}")

        if attempt < retries - 1:
            await asyncio.sleep(random.uniform(1, 2))

    return None


async def fetch_category_page_async(session: aiohttp.ClientSession, base_url: str, page: int, chunk: int) -> List[str]:
    """
    Fetch a category page and extract product URLs - MATCHES V2 LOGIC EXACTLY
    """
    # Build URL exactly like V2
    page_url = f"{base_url}&page={page}&chunk={chunk}" if chunk > 1 else f"{base_url}&page={page}"

    # Add delay like V2
    delay = random.uniform(1.5, 2.5)
    print(f"    Fetching page {page}, chunk {chunk}, waiting {delay:.2f}s...")
    await asyncio.sleep(delay)

    html = await fetch_page_async(session, page_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    product_urls = []

    # Try JSON-LD first - EXACT V2 LOGIC
    json_ld_script = soup.find('script', type='application/ld+json')
    if json_ld_script:
        try:
            json_data = json.loads(json_ld_script.string)
            if json_data.get('@type') == 'ItemList' and 'itemListElement' in json_data:
                product_urls = [
                    item['url'] for item in json_data['itemListElement']
                    if item.get('url') and '/p' in item['url']
                ]
                product_urls = [urljoin("https://www.johnlewis.com", url) for url in product_urls]
        except json.JSONDecodeError:
            logging.warning(f"Failed to parse JSON-LD on {page_url}")

    # Fallback to CSS - EXACT V2 LOGIC
    if not product_urls:
        links = soup.select('a.product-card_c-product-card__link___7IQk')
        product_urls = [urljoin("https://www.johnlewis.com", link.get('href')) for link in links if link.get('href')]

    if not product_urls:
        debug_file = os.path.join(LOG_DIR, f"debug_page_{page}_chunk_{chunk}.html")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        logging.warning(f"No products found on {page_url}. Saved to {debug_file}")

    print(f"    Found {len(product_urls)} products on page {page}, chunk {chunk}")
    logging.info(f"Found {len(product_urls)} products on page {page}, chunk {chunk}")
    return list(set(product_urls))


async def fetch_all_category_products_async(session: aiohttp.ClientSession, category_name: str, category_config: Dict) -> List[str]:
    """
    Fetch all product URLs for a category - MATCHES V2 SEQUENTIAL CHUNKING LOGIC

    The original V2 fetches chunks sequentially and stops when:
    - Low product count (<10)
    - No new products found
    - Max products per page reached

    We replicate this exactly but do the product fetching async later.
    """
    all_product_urls = []
    product_id_set: Set[str] = set()
    request_count = 0
    max_pages = category_config["max_pages"]
    max_products_per_page = category_config["max_products_per_page"]

    for page in range(1, max_pages + 1):
        page_urls = []
        chunk = 1
        previous_chunk_urls: Set[str] = set()
        total_products = 0

        # Sequential chunk fetching with early exit - EXACT V2 LOGIC
        while chunk <= MAX_CHUNKS:
            if request_count >= MAX_PAGE_REQUESTS:
                print(f"    Reached max requests ({MAX_PAGE_REQUESTS})")
                break

            product_urls = await fetch_category_page_async(session, category_config["url"], page, chunk)
            request_count += 1

            if not product_urls or len(product_urls) < 10:
                print(f"    Low product count ({len(product_urls)}), stopping chunks")
                break

            # Normalize and dedupe - EXACT V2 LOGIC
            normalized_urls = []
            new_product_ids = set()

            for url in product_urls:
                normalized_url = normalize_url(url)
                product_id = extract_product_id(normalized_url)

                if not product_id:
                    continue

                if product_id in product_id_set:
                    continue

                product_id_set.add(product_id)
                new_product_ids.add(product_id)
                normalized_urls.append(normalized_url)

            print(f"    Page {page}, Chunk {chunk}: {len(new_product_ids)} new products")

            current_chunk_urls = set(normalized_urls)
            total_products += len(current_chunk_urls - previous_chunk_urls)

            # Stop conditions - EXACT V2 LOGIC
            if current_chunk_urls <= previous_chunk_urls or total_products >= max_products_per_page:
                print(f"    Reached {total_products} products, stopping chunks")
                break

            page_urls.extend(normalized_urls)
            previous_chunk_urls.update(current_chunk_urls)
            chunk += 1

        page_urls = list(set(page_urls))
        all_product_urls.extend(page_urls)
        print(f"  Page {page} complete: {len(page_urls)} URLs")

    all_product_urls = list(set(all_product_urls))
    print(f"  Total URLs for {category_name}: {len(all_product_urls)}")
    return all_product_urls


def extract_all_variants(soup: BeautifulSoup, url: str, category_name: str) -> Optional[List[Dict]]:
    """Extract all variants from a product page - EXACT V2 LOGIC"""
    variants = []

    # Find variant buttons - EXACT V2 LOGIC
    variant_buttons = soup.find_all(['button', 'a'], attrs={
        'data-testid': re.compile(r'colour:option', re.I)
    })

    if not variant_buttons:
        variant_buttons = soup.find_all(['button', 'span'], class_=re.compile(r'.*colour.*option.*', re.I))

    if not variant_buttons:
        logging.debug(f"No variants found for {url}")
        return None

    logging.info(f"Found {len(variant_buttons)} variants, extracting ALL prices...")

    for variant_btn in variant_buttons:
        try:
            # Extract variant name
            variant_name = variant_btn.get_text(strip=True)
            if not variant_name or len(variant_name) > 30:
                variant_name = variant_btn.get('aria-label', 'Unknown')

            # Find price container
            variant_container = variant_btn.find_parent(['div', 'li'])
            if not variant_container:
                continue

            price_container = variant_container.find_next(['div', 'span'], class_=re.compile(r'price', re.I))

            current_price = None
            original_price = None

            if price_container:
                current_price_elem = price_container.select_one('.prod-price__current') or \
                                    price_container.find('span', attrs={'data-testid': 'price-current'})
                if current_price_elem:
                    current_price = clean_price(current_price_elem.get_text(strip=True))

                original_price_elem = price_container.select_one('.prod-price__was') or \
                                     price_container.find('span', attrs={'data-testid': 'price-prev'})
                if original_price_elem:
                    original_price = clean_price(original_price_elem.get_text(strip=True))

            # Method 2: Check siblings
            if not current_price:
                next_sibling = variant_btn.find_next_sibling()
                if next_sibling:
                    price_text = next_sibling.get_text()
                    prices = re.findall(r'£([\d,]+\.?\d*)', price_text)
                    if len(prices) >= 1:
                        current_price = clean_price(prices[0])
                    if len(prices) >= 2:
                        original_price = clean_price(prices[1])

            # Calculate discount
            discount = 0.0
            if original_price and current_price and original_price > current_price > 0:
                discount = ((original_price - current_price) / original_price) * 100

            # Add variants meeting minimum discount - EXACT V2 LOGIC
            category_min_discount = CATEGORY_URLS[category_name]["min_discount"]
            if current_price and discount >= category_min_discount:
                variants.append({
                    'name': variant_name,
                    'current_price': current_price,
                    'original_price': original_price,
                    'discount': discount
                })
                logging.info(f"  Variant: {variant_name}: £{current_price} (was £{original_price}, {discount:.1f}% off)")

        except Exception as e:
            logging.error(f"Error extracting variant: {e}")
            continue

    return variants if variants else None


async def fetch_product_info_async(session: aiohttp.ClientSession, url: str, category_name: str, semaphore: asyncio.Semaphore) -> List[Dict]:
    """
    Fetch and parse a single product page asynchronously.
    Uses semaphore to control concurrency.
    MATCHES V2 EXTRACTION LOGIC EXACTLY.
    """
    global ssl_error_count, excluded_keyword_count

    async with semaphore:
        normalized_url = normalize_url(url)
        base_product_id = extract_product_id(normalized_url)

        if not base_product_id:
            return []

        # Small delay per request
        await asyncio.sleep(random.uniform(0.1, 0.3))

        html = await fetch_page_async(session, url)
        if not html:
            return []

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Extract base name - EXACT V2 LOGIC
            data_script = soup.find('script', type='application/ld+json')
            base_name = "Unknown"
            stock_status = "Unknown"
            image_url = None

            if data_script:
                try:
                    json_data = json.loads(data_script.string)
                    base_name = json_data.get("name") or "Unknown"
                    availability = json_data.get("offers", {}).get("availability")
                    stock_status = "In Stock" if availability and "InStock" in availability else "Out of Stock"
                    image_url = json_data.get("image")
                except:
                    pass

            if base_name == "Unknown":
                name_elem = soup.select_one("h1.product-header__name")
                base_name = name_elem.get_text(strip=True) if name_elem else "Unknown"

            # Check excluded keywords - EXACT V2 LOGIC
            if any(kw.lower() in base_name.lower() for kw in EXCLUDED_KEYWORDS):
                excluded_keyword_count += 1
                logging.warning(f"Excluded: {base_name}")
                return []

            # Extract stock status fallback
            if stock_status == "Unknown":
                stock_elem = soup.select_one(".stock-availability-message")
                stock_status = stock_elem.get_text(strip=True) if stock_elem else "Not listed"

            # Image fallback
            if not image_url:
                image_elem = soup.select_one("img.product-image")
                image_url = image_elem.get("src") if image_elem else None

            # Extract sizes - EXACT V2 LOGIC
            sizes = []
            size_elements = soup.find_all("a", attrs={"data-testid": "size:option:button"}) or \
                           soup.find_all("span", class_=re.compile(r"size", re.I))
            for size in size_elements:
                label = size.get_text(strip=True)
                if label:
                    sizes.append(normalize_size(label))
            if not sizes:
                sizes = ["One Size"]

            # Try to extract all variants - EXACT V2 LOGIC
            variants = extract_all_variants(soup, url, category_name)

            products = []

            if variants:
                # Multi-variant: Create separate product entry for EACH variant
                logging.info(f"Multi-variant product: Creating {len(variants)} entries")

                for variant in variants:
                    variant_name = variant['name']
                    # Create unique ID for this variant - EXACT V2 LOGIC
                    variant_id = f"{base_product_id}_{variant_name.replace(' ', '_')[:20]}"
                    full_name = f"{base_name} - {variant_name}"

                    is_recently_reduced = price_history_manager.update(variant_id, variant['current_price'], full_name)

                    products.append({
                        "product_id": variant_id,
                        "base_product_id": base_product_id,
                        "name": full_name,
                        "url": url,  # Original URL, not normalized
                        "current_price": variant['current_price'],
                        "original_price": variant['original_price'],
                        "discount": variant['discount'],
                        "stock_status": stock_status,
                        "image": image_url or "",
                        "sizes": sizes,
                        "variants": [v['name'] for v in variants],
                        "category": category_name,
                        "recently_reduced": is_recently_reduced
                    })

                    logging.info(f"Variant: {full_name} - £{variant['current_price']} ({variant['discount']:.1f}% off)")
            else:
                # Single-price product - EXACT V2 LOGIC
                current_price = None
                original_price = None

                if data_script:
                    try:
                        json_data = json.loads(data_script.string)
                        current_price = json_data.get("offers", {}).get("price")
                        current_price = float(current_price) if current_price else None
                    except:
                        pass

                if current_price is None:
                    current_price_elem = soup.select_one(".prod-price__current") or \
                                        soup.select_one("span[data-testid='price-current']")
                    current_price = clean_price(current_price_elem.get_text(strip=True)) if current_price_elem else None

                # Original price extraction - EXACT V2 LOGIC
                price_prev = soup.find("span", attrs={"data-testid": "price-prev"})
                if price_prev:
                    original_price = clean_price(price_prev.get_text(strip=True))

                if not original_price:
                    price_was = soup.find(lambda tag: tag.name in ['span', 'div', 's'] and
                                        re.search(r'was\s*£?\d', tag.get_text(strip=True), re.I))
                    if price_was:
                        original_price = clean_price(price_was.get_text(strip=True))

                # Calculate discount
                discount = 0.0
                if original_price and current_price and original_price > current_price > 0:
                    discount = ((original_price - current_price) / original_price) * 100

                category_min_discount = CATEGORY_URLS[category_name]["min_discount"]
                if discount < category_min_discount:
                    logging.warning(f"Below threshold: {base_name} ({discount:.1f}%)")
                    return []

                is_recently_reduced = price_history_manager.update(base_product_id, current_price, base_name)

                # Get variant names for display
                variant_elements = soup.find_all("a", attrs={"data-testid": re.compile(r"colour:option", re.I)})
                variant_list = [v.get_text(strip=True) for v in variant_elements if v.get_text(strip=True)]

                products.append({
                    "product_id": base_product_id,
                    "base_product_id": base_product_id,
                    "name": base_name,
                    "url": url,  # Original URL
                    "current_price": current_price,
                    "original_price": original_price,
                    "discount": discount,
                    "stock_status": stock_status,
                    "image": image_url or "",
                    "sizes": sizes,
                    "variants": variant_list,
                    "category": category_name,
                    "recently_reduced": is_recently_reduced
                })

                logging.info(f"Single: {base_name} - £{current_price} ({discount:.1f}% off)")

            return products

        except Exception as e:
            logging.error(f"Error parsing {url}: {e}")
            return []


async def fetch_products_batch_async(session: aiohttp.ClientSession, urls: List[str], category_name: str) -> List[Dict]:
    """
    Fetch multiple products concurrently with rate limiting.
    OPTIMIZATION: Process products in parallel batches.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    all_products = []

    total_urls = len(urls)
    print(f"  Fetching {total_urls} products with {MAX_CONCURRENT_REQUESTS} concurrent requests...")

    # Process in batches with progress reporting
    for batch_start in range(0, total_urls, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_urls)
        batch_urls = urls[batch_start:batch_end]

        # Create tasks for this batch
        tasks = [
            fetch_product_info_async(session, url, category_name, semaphore)
            for url in batch_urls
        ]

        # Run batch concurrently
        batch_results = await asyncio.gather(*tasks)

        # Collect results
        for products in batch_results:
            all_products.extend(products)

        # Progress update
        progress = (batch_end / total_urls) * 100
        print(f"  Progress: {batch_end}/{total_urls} ({progress:.0f}%) - {len(all_products)} products found")

        # Small delay between batches to avoid rate limiting
        if batch_end < total_urls:
            await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    return all_products


def is_duplicate_in_csv(product_id: str, event_type: str) -> bool:
    """Check if this exact event already exists for this product today.

    Uses Product ID + Event Type + Date to prevent duplicate entries.
    Scans entire CSV (not just last N rows) for robust duplicate detection.
    """
    if not os.path.exists(CSV_FILE):
        return False
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        event_type_lower = event_type.lower()
        with open(CSV_FILE, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if (row.get('Product ID') == product_id and
                    row.get('Event Type', '').lower() == event_type_lower and
                    row.get('Timestamp', '').startswith(today)):
                    return True
        return False
    except Exception as e:
        logging.error(f"Error checking CSV duplicates: {e}")
        return False


def send_webhook_sync(product: Dict, event_type: str, previous_state: Dict, price_diff: float = None, direction: str = None):
    """Send webhook for individual product - EXACT V2 LOGIC"""
    # Check for duplicate using Product ID + Event Type + Date
    if is_duplicate_in_csv(product['product_id'], event_type):
        logging.info(f"Skipping duplicate webhook: {product['name']} ({product['product_id']}) - already logged today")
        return

    webhook = DiscordWebhook(url=WEBHOOK_URL)
    embed = DiscordEmbed(
        title=product["name"][:256],
        url=product["url"],
        color=0x00ff00 if product["stock_status"] == "In Stock" else 0xff0000
    )

    if product.get("image"):
        embed.set_thumbnail(url=product["image"])

    current_price = product["current_price"]
    embed.add_embed_field(
        name="Current Price",
        value=f"£{current_price:.2f}" if current_price is not None else "N/A",
        inline=True
    )

    if event_type == "price_change":
        previous_price = previous_state[product["product_id"]]["latest_price"]
        embed.add_embed_field(
            name="Previous Price",
            value=f"£{previous_price:.2f}" if previous_price is not None else "N/A",
            inline=True
        )
        embed.add_embed_field(
            name="Change",
            value=f"{direction.capitalize()} £{abs(price_diff):.2f}" if price_diff else "N/A",
            inline=True
        )
    else:
        embed.add_embed_field(name="Previous Price", value="N/A (New)", inline=True)
        embed.add_embed_field(name="Change", value="N/A (New)", inline=True)

    original_price = product["original_price"]
    embed.add_embed_field(
        name="Original Price",
        value=f"£{original_price:.2f}" if original_price else "N/A",
        inline=True
    )
    embed.add_embed_field(
        name="Discount",
        value=f"{product['discount']:.2f}%",
        inline=True
    )
    embed.add_embed_field(name="Stock", value=product["stock_status"], inline=True)
    embed.add_embed_field(name="Category", value=product["category"], inline=True)

    sizes_value = ", ".join(product["sizes"][:5]) if product["sizes"] else "One Size"
    embed.add_embed_field(name="Sizes", value=sizes_value[:1024], inline=False)

    variants_value = ", ".join(product["variants"][:5]) if product.get("variants") else "None"
    embed.add_embed_field(name="Variants", value=variants_value[:1024], inline=False)

    # Badge logic - EXACT V2
    badges = [f"{'New' if event_type == 'new' else direction.capitalize()}"]
    if product.get("recently_reduced"):
        badges.append("🔥 Recently Reduced")

    embed.set_footer(text=f"Alternative Assets | {' | '.join(badges)}"[:2048])

    webhook.add_embed(embed)

    for attempt in range(3):
        try:
            time.sleep(random.uniform(1, 1.5))
            webhook.execute()
            logging.info(f"Webhook sent: {product['name']}")

            # Append to CSV - EXACT V2 LOGIC
            append_to_csv(product, event_type)
            return

        except Exception as e:
            logging.error(f"Webhook failed (attempt {attempt+1}): {e}")
            if attempt < 2:
                time.sleep(2)


def append_to_csv(product: Dict, event_type: str):
    """Append product to CSV file - EXACT V2 STRUCTURE"""
    row_data = {
        'Product ID': product["product_id"],
        'Product Name': product['name'],
        'Current Price': f"{product['current_price']:.2f}" if product['current_price'] else "N/A",
        'Original Price': f"{product['original_price']:.2f}" if product['original_price'] else "N/A",
        'Discount': f"{product['discount']:.2f}",
        'Stock Status': product['stock_status'],
        'Sizes': ", ".join(product['sizes'][:10]),
        'URL': product['url'],
        'Event Type': event_type.capitalize(),
        'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Image': product['image'],
        'Category': product['category'],
        'Variants': ", ".join(product.get('variants', [])[:5])
    }

    file_exists = os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row_data.keys(), quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_data)

    logging.info(f"CSV appended: {row_data['Product Name']}")


def process_webhooks(products: List[Dict], previous_state: Dict, category_name: str, global_seen: Set[str] = None) -> int:
    """Process webhooks for new/changed products.

    Args:
        products: List of product dicts to process
        previous_state: State for this category
        category_name: Name of current category
        global_seen: Set of product IDs seen across ALL categories (for cross-category dedup)
    """
    items_to_report = []

    # Load global seen products if not provided (for cross-category deduplication)
    if global_seen is None:
        global_seen = load_global_seen_products()

    # Count how many would be "new" for sanity check
    new_count = 0
    for product in products:
        if product["product_id"] not in previous_state and product["product_id"] not in global_seen:
            new_count += 1

    # SANITY CHECK: If >50% of products appear "new", likely state file corruption
    # Skip sending webhooks and warn instead
    NEW_PRODUCT_THRESHOLD = 0.5  # 50%
    if len(products) > 10 and new_count / len(products) > NEW_PRODUCT_THRESHOLD:
        warning_msg = (
            f"WARNING: {new_count}/{len(products)} ({new_count/len(products)*100:.0f}%) products appear as 'new' in {category_name}. "
            f"This suggests possible state file corruption. Skipping webhooks to prevent spam. "
            f"Check state file: {category_name}"
        )
        logging.warning(warning_msg)
        print(f"  {warning_msg}")
        # Send a single warning webhook instead of hundreds of product webhooks
        try:
            webhook = DiscordWebhook(url=WEBHOOK_URL, content=f"⚠️ {warning_msg}")
            webhook.execute()
        except:
            pass
        return 0

    for product in products:
        product_id = product["product_id"]
        current_price = product["current_price"]

        # Skip excluded keywords
        if any(kw.lower() in product["name"].lower() for kw in EXCLUDED_KEYWORDS):
            continue

        event_type = None
        price_diff = None
        direction = None

        if product_id not in previous_state:
            # Check if product exists in another category's state (cross-category dedup)
            if product_id in global_seen:
                logging.info(f"SKIPPED (exists in other category): {product['name']} (ID: {product_id})")
                continue
            event_type = "new"
            items_to_report.append((product, event_type, price_diff, direction))
            logging.info(f"NEW: {product['name']} (ID: {product_id})")
        else:
            old_price = previous_state[product_id]["latest_price"]

            price_changed = (
                old_price is not None and current_price is not None and
                abs(current_price - old_price) > 0.01
            )

            if price_changed:
                event_type = "price_change"
                price_diff = current_price - old_price
                direction = "increased" if price_diff > 0 else "decreased"
                items_to_report.append((product, event_type, price_diff, direction))
                logging.info(f"PRICE CHANGE: {product['name']} ({old_price} -> {current_price})")

    # Sort by discount - EXACT V2
    items_to_report.sort(key=lambda x: x[0]["discount"] or 0, reverse=True)

    print(f"  Found {len(items_to_report)} updates. Sending webhooks...")

    for i, (product, event_type, price_diff, direction) in enumerate(items_to_report, 1):
        print(f"    Webhook {i}/{len(items_to_report)}: {product['name'][:40]}...")
        send_webhook_sync(product, event_type, previous_state, price_diff, direction)
        time.sleep(random.uniform(1, 1.5))

    return len(items_to_report)


def clean_old_products_from_csv(all_scanned_product_ids: Set[str]):
    """Clean products from CSV that are no longer on John Lewis site.

    Simple rule: if a product wasn't in the current scan, it's gone from the site
    and should be removed from the CSV. John Lewis removes items regularly.
    """
    if not os.path.exists(CSV_FILE):
        return

    try:
        rows_to_keep = []
        removed_count = 0

        with open(CSV_FILE, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames

            for row in reader:
                product_id = row.get('Product ID', '')

                # Simple rule: keep only if in current scan
                if product_id in all_scanned_product_ids:
                    rows_to_keep.append(row)
                else:
                    removed_count += 1
                    logging.info(f"Removing product no longer on site: {row.get('Product Name', 'Unknown')} (ID: {product_id})")

        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            if rows_to_keep and headers:
                writer = csv.DictWriter(csvfile, fieldnames=headers, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(rows_to_keep)

        logging.info(f"CSV cleanup: Removed {removed_count} old products, kept {len(rows_to_keep)}")
        print(f"  CSV cleanup: Removed {removed_count} stale products, kept {len(rows_to_keep)}")

    except Exception as e:
        logging.error(f"Error cleaning CSV: {e}")


def send_status_webhook(message: str):
    """Send a status webhook"""
    try:
        webhook = DiscordWebhook(url=WEBHOOK_URL, content=message)
        time.sleep(random.uniform(0.5, 1.0))
        webhook.execute()
    except Exception as e:
        logging.error(f"Status webhook failed: {e}")


async def run_cycle_async():
    """Run a single monitoring cycle"""
    global price_history_manager, ssl_error_count, excluded_keyword_count

    ssl_error_count = 0
    excluded_keyword_count = 0
    start_time = datetime.now()

    # Load price history ONCE for entire cycle
    price_history_manager = PriceHistoryManager(PRICE_HISTORY_FILE)
    price_history_manager.load()

    total_products = 0
    total_changes = 0
    all_scanned_ids: Set[str] = set()

    # Load global seen products ONCE for cross-category deduplication
    global_seen = load_global_seen_products()

    # Create aiohttp session with connection pooling
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS * 2,
        ttl_dns_cache=300,
        keepalive_timeout=60
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        for category_name, category_config in CATEGORY_URLS.items():
            print(f"\n📦 Processing: {category_name}")
            send_status_webhook(f"🚀 Monitor started - Cycle: {category_name}")

            # Load state for this category
            state_manager = StateManager(category_config["state_file"])
            state_manager.load()

            # Fetch product URLs (sequential chunking like V2)
            product_urls = await fetch_all_category_products_async(session, category_name, category_config)

            # Fetch all products concurrently (this is where we get speedup)
            all_products = await fetch_products_batch_async(session, product_urls, category_name)

            # Track scanned IDs
            current_product_ids: Set[str] = set()
            for product in all_products:
                current_product_ids.add(product["product_id"])
                all_scanned_ids.add(product["product_id"])

            # Process webhooks (with cross-category deduplication)
            changes = 0
            if all_products:
                changes = process_webhooks(all_products, state_manager.data, category_name, global_seen)

                # Update state
                current_time = datetime.now().isoformat()
                for product in all_products:
                    state_manager.update_product(product, current_time)
                state_manager.save(current_product_ids)

            total_products += len(all_products)
            total_changes += changes

            print(f"✅ {category_name}: {len(all_products)} products, {changes} changes")
            await asyncio.sleep(random.uniform(30, 60))

    # Save price history ONCE at end of cycle
    price_history_manager.save()

    # Clean CSV
    print("\n🧹 Cleaning CSV...")
    clean_old_products_from_csv(all_scanned_ids)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        "duration": duration,
        "total_products": total_products,
        "total_changes": total_changes,
        "ssl_errors": ssl_error_count,
        "excluded": excluded_keyword_count
    }


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logging.info("Shutdown signal received")
    print("\nShutting down gracefully...")
    sys.exit(0)


async def main_async():
    """Main async monitoring loop"""
    global cycle_count
    logging.info("Starting John Lewis Monitor V3 (ASYNC)")
    print("🚀 Starting John Lewis Monitor V3 (ASYNC)")
    print(f"Concurrency: {MAX_CONCURRENT_REQUESTS} requests, batch size: {BATCH_SIZE}")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while True:
        try:
            cycle_count += 1

            print(f"\n{'='*60}")
            print(f"CYCLE {cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")

            result = await run_cycle_async()

            print(f"\n{'='*60}")
            print(f"CYCLE {cycle_count} COMPLETE")
            print(f"Duration: {result['duration']:.1f} seconds ({result['duration']/60:.1f} minutes)")
            print(f"Products: {result['total_products']}")
            print(f"Changes: {result['total_changes']}")
            print(f"SSL Errors: {result['ssl_errors']}")
            print(f"Excluded: {result['excluded']}")
            print(f"{'='*60}\n")

            if cycle_count % NOTIFY_EVERY_CYCLES == 0:
                send_status_webhook(f"✅ Cycle {cycle_count}: {result['total_products']} products, {result['total_changes']} changes ({result['duration']:.0f}s)")

            check_interval = random.uniform(6900, 7500)
            print(f"⏰ Next check in {check_interval/60:.1f} minutes...")
            await asyncio.sleep(check_interval)

        except Exception as e:
            logging.error(f"Script crashed: {e}")
            send_status_webhook(f"❌ Monitor crashed: {e}")
            print(f"❌ Error: {e}")
            print("Restarting in 60 seconds...")
            await asyncio.sleep(60)


def main():
    """Entry point"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
