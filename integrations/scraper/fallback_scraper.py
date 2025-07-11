import requests
import re
import time
import logging
import html
from typing import Optional, Tuple
from bs4 import BeautifulSoup
from integrations.scraper.target_scraper import TargetScraper

logger = logging.getLogger(__name__)


class FallbackScraper:
    """Fallback scraper using requests and BeautifulSoup when Selenium fails"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def extract_product_name(self, full_name: str) -> str:
        """Extract product name using the specified rules"""
        if not full_name:
            return "Unknown Product"

        logger.debug(f"Extracting product name from: {full_name}")

        # Decode HTML entities first (&#38; -> &, &#34; -> ", etc.)
        cleaned_name = html.unescape(full_name)

        # Clean up the text - remove "This item is not available" and similar messages
        cleanup_patterns = [
            r"\s*This item is not available\s*",
            r"\s*Currently unavailable\s*",
            r"\s*Out of stock\s*",
            r"\s*Not available\s*",
            r"\s*Temporarily unavailable\s*",
            r"\s*Item not available\s*",
            r"\s*Product unavailable\s*",
            r"\s*Unavailable\s*",
        ]

        for pattern in cleanup_patterns:
            cleaned_name = re.sub(pattern, "", cleaned_name, flags=re.IGNORECASE).strip()

        # Also remove any trailing/leading whitespace and normalize spaces
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()

        # Check for error messages that indicate we should use SKU name instead
        error_patterns = [
            r"We're sorry, something went wrong",
            r"We\u2019re sorry, something went wrong",
            r"Were sorry, something went wrong",
            r"Page not found",
            r"Product not available",
            r"Item not found"
        ]

        for pattern in error_patterns:
            if re.search(pattern, cleaned_name, re.IGNORECASE):
                logger.warning(f"Error page detected: {cleaned_name}")
                return None  # Signal that we should use SKU name

        # If after cleaning we have nothing left, return None
        if not cleaned_name:
            return None

        # Rule 1: For "Pokémon - Trading Card Game: Scarlet & Violet - [name]"
        pattern1 = r"Pokémon - Trading Card Game: Scarlet & Violet - (.+)"
        match1 = re.search(pattern1, cleaned_name)
        if match1:
            extracted = match1.group(1).strip()
            logger.debug(f"Matched pattern 1: {extracted}")
            return extracted

        # Rule 2: For "Pokémon - Trading Card Game: [name]"
        pattern2 = r"Pokémon - Trading Card Game: (.+)"
        match2 = re.search(pattern2, cleaned_name)
        if match2:
            extracted = match2.group(1).strip()
            logger.debug(f"Matched pattern 2: {extracted}")
            return extracted

        # Rule 3: If no pattern matches, return the cleaned name
        logger.debug(f"No pattern matched, using cleaned name: {cleaned_name}")
        return cleaned_name

    def scrape_product_info(self, sku: str) -> Tuple[Optional[str], Optional[str]]:
        """Scrape product info using requests and BeautifulSoup"""
        url = f"https://www.target.com/p/-/A-{sku}"  # Correct Target format

        try:
            logger.info(f"Fallback scraping for SKU {sku} from {url}")

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            product_name = None
            thumbnail_url = None

            # Try to extract product name
            try:
                # Look for various title selectors
                title_selectors = [
                    "h1#pdp-product-title-id",
                    "h1[data-test='product-title']",
                    "h1.ProductTitle",
                    "span[data-test='product-title']",
                    "h1",
                    "[class*='product-title']"
                ]

                title_element = None
                text_content = ""

                for selector in title_selectors:
                    title_element = soup.select_one(selector)
                    if title_element:
                        # Get only the direct text content, not from child elements
                        text_content = ""

                        # Try to get just the main text, excluding child elements
                        for content in title_element.contents:
                            if hasattr(content, 'strip'):  # It's a text node
                                text_content += content.strip() + " "
                            elif hasattr(content, 'get_text'):  # It's an element
                                # Only include if it's not a status message
                                child_text = content.get_text().strip()
                                if not any(phrase in child_text.lower() for phrase in [
                                    'not available', 'unavailable', 'out of stock'
                                ]):
                                    text_content += child_text + " "

                        text_content = text_content.strip()

                        # Fallback to full text if we didn't get anything
                        if not text_content:
                            text_content = title_element.get_text().strip()

                        if text_content:
                            break

                if title_element and text_content:
                    full_name = text_content
                    logger.info(f"Raw product title found: {full_name}")

                    extracted_name = self.extract_product_name(full_name)
                    if extracted_name is None:
                        product_name = f"Unknown Product (SKU: {sku})"
                        logger.warning(f"Error page detected, using SKU name: {product_name}")
                    else:
                        product_name = extracted_name
                        logger.info(f"Extracted product name: {product_name}")
                else:
                    logger.warning(f"Could not find product title for SKU {sku}")
                    product_name = f"Unknown Product (SKU: {sku})"

            except Exception as e:
                logger.error(f"Error extracting product name for SKU {sku}: {e}")
                product_name = f"Unknown Product (SKU: {sku})"

            # Try to extract thumbnail URL
            try:
                # Look for product images
                img_selectors = [
                    'img[src*="target.scene7.com"]',  # Target CDN
                    'img[src*="Target/"]',  # Target images
                    '#PdpImageGallerySection img',  # Target gallery
                    'img[data-test*="product"]',  # Product images
                    'img[alt*="product"]',  # Alt text
                    'picture img',  # Picture elements
                ]

                img_element = None
                for selector in img_selectors:
                    img_element = soup.select_one(selector)
                    if img_element:
                        break

                if img_element:
                    # Try srcset first, then src
                    srcset = img_element.get('srcset')
                    src = img_element.get('src')

                    if srcset:
                        # Extract URLs from srcset
                        urls = re.findall(r'(https://[^\s,]+)', srcset)
                        if urls:
                            thumbnail_url = urls[-1]  # Highest quality
                            logger.info(f"Extracted thumbnail from srcset: {thumbnail_url}")

                    if not thumbnail_url and src:
                        thumbnail_url = src
                        logger.info(f"Using src as thumbnail: {thumbnail_url}")

                else:
                    logger.warning(f"Could not find product image for SKU {sku}")

            except Exception as e:
                logger.error(f"Error extracting thumbnail for SKU {sku}: {e}")

            return product_name, thumbnail_url

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error scraping SKU {sku}: {e}")
            return f"Unknown Product (SKU: {sku})", None
        except Exception as e:
            logger.error(f"Error scraping SKU {sku}: {e}")
            return f"Unknown Product (SKU: {sku})", None

    def close(self):
        """Close the session"""
        self.session.close()


# Update the ProductInfoUpdater to use fallback scraper
class EnhancedProductInfoUpdater:
    def __init__(self, discord_handler):
        self.discord_handler = discord_handler
        self.scraper = None
        self.fallback_scraper = None

    def stop_scraping(self):
        """Stop background scraping"""
        logger.info("Stopping enhanced scraping...")
        try:
            if self.scraper:
                self.scraper.close()
            if self.fallback_scraper:
                self.fallback_scraper.close()
        except Exception as e:
            logger.error(f"Error stopping enhanced scraper: {e}")

    def setup_scrapers(self):
        """Setup both Selenium and fallback scrapers"""

        # Try Selenium first
        self.scraper = TargetScraper()
        selenium_ready = self.scraper.setup_driver()

        if not selenium_ready:
            logger.warning("Selenium scraper failed to initialize, will use fallback scraper")
            self.scraper = None

        # Always setup fallback scraper
        self.fallback_scraper = FallbackScraper()

        return selenium_ready or self.fallback_scraper is not None

    def scrape_with_fallback(self, sku: str) -> Tuple[Optional[str], Optional[str]]:
        """Try Selenium first, fallback to requests if it fails"""

        # Try Selenium first if available
        if self.scraper:
            try:
                logger.debug(f"Trying Selenium scraper for SKU {sku}")
                name, thumbnail = self.scraper.scrape_product_info(sku)
                if name and 'Unknown Product' not in name:
                    logger.info(f"Selenium scraper successful for SKU {sku}")
                    return name, thumbnail
                else:
                    logger.warning(f"Selenium scraper returned unknown product for SKU {sku}")
            except Exception as e:
                logger.warning(f"Selenium scraper failed for SKU {sku}: {e}")

        # Fallback to requests scraper
        if self.fallback_scraper:
            try:
                logger.info(f"Using fallback scraper for SKU {sku}")
                name, thumbnail = self.fallback_scraper.scrape_product_info(sku)
                return name, thumbnail
            except Exception as e:
                logger.error(f"Fallback scraper also failed for SKU {sku}: {e}")

        return f"Unknown Product (SKU: {sku})", None

    def update_products_from_web(self, sku_list: list = None, force_update: bool = False):
        """Enhanced update with fallback scraping"""
        try:
            # Setup scrapers
            if not self.setup_scrapers():
                logger.error("Failed to setup any scrapers")
                return False

            # Load current products
            products = self.discord_handler._load_products()

            # Determine which SKUs to process
            if sku_list:
                skus_to_process = sku_list
            else:
                skus_to_process = []
                for sku, product_info in products.items():
                    name = product_info.get('name', '')
                    thumbnail_url = product_info.get('thumbnail_url', '')
                    needs_name = 'Unknown Product' in name or not name
                    needs_thumbnail = not thumbnail_url or thumbnail_url == ""

                    if force_update or needs_name or needs_thumbnail:
                        skus_to_process.append(sku)

            if not skus_to_process:
                logger.info("No SKUs need scraping")
                return True

            logger.info(f"Starting enhanced web scraping for {len(skus_to_process)} SKUs")

            # Process each SKU
            updated_count = 0
            for sku in skus_to_process:
                try:
                    logger.info(f"Processing SKU {sku}...")

                    # Get current product info
                    current_info = products.get(sku, {})
                    current_name = current_info.get('name', f'Unknown Product (SKU: {sku})')
                    current_thumbnail = current_info.get('thumbnail_url', '')
                    current_send_initial = current_info.get('send_initial', True)

                    # Scrape new info with fallback
                    scraped_name, scraped_thumbnail = self.scrape_with_fallback(sku)

                    # Determine what to update
                    new_name = current_name
                    new_thumbnail = current_thumbnail
                    updated_fields = []

                    # Update name if we got a better one
                    if scraped_name and ('Unknown Product' in current_name or not current_name or force_update):
                        new_name = scraped_name
                        updated_fields.append("name")

                    # Update thumbnail if we got one and don't have one (or force update)
                    if scraped_thumbnail and (not current_thumbnail or force_update):
                        new_thumbnail = scraped_thumbnail
                        updated_fields.append("thumbnail")

                    # Update products.json if anything changed
                    if updated_fields:
                        self.discord_handler.update_product_info(
                            sku,
                            new_name,
                            new_thumbnail,
                            current_send_initial
                        )
                        updated_count += 1
                        logger.info(f"Updated {', '.join(updated_fields)} for SKU {sku}")
                    else:
                        logger.info(f"No updates needed for SKU {sku}")

                    # Add delay between requests
                    time.sleep(10)

                except Exception as e:
                    logger.error(f"Error processing SKU {sku}: {e}")
                    continue

            logger.info(f"Enhanced web scraping completed. Updated {updated_count} products.")
            return True

        except Exception as e:
            logger.error(f"Error in enhanced update_products_from_web: {e}")
            return False
        finally:
            if self.scraper:
                self.scraper.close()
            if self.fallback_scraper:
                self.fallback_scraper.close()