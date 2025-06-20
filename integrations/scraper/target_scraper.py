from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from integrations.discord_handler import DiscordHandler
import time
import logging
import re
import concurrent.futures
import os
import sys
from typing import Dict, Optional, Tuple, List
import threading

logger = logging.getLogger(__name__)


class TargetScraper:
    def __init__(self):
        self.driver = None
        self.wait = None

    def setup_driver(self):
        """Setup Chrome driver with headless options and suppressed output"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Faster loading
            chrome_options.add_argument("--disable-logging")  # Suppress Chrome logs
            chrome_options.add_argument("--disable-dev-tools")
            chrome_options.add_argument("--silent")
            chrome_options.add_argument("--log-level=3")  # Only fatal errors
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-ipc-flooding-protection")
            # Additional options to suppress WebGL and GPU errors
            chrome_options.add_argument("--disable-webgl")
            chrome_options.add_argument("--disable-webgl2")
            chrome_options.add_argument("--disable-3d-apis")
            chrome_options.add_argument("--disable-accelerated-2d-canvas")
            chrome_options.add_argument("--disable-accelerated-jpeg-decoding")
            chrome_options.add_argument("--disable-accelerated-mjpeg-decode")
            chrome_options.add_argument("--disable-accelerated-video-decode")
            chrome_options.add_argument("--disable-gpu-sandbox")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--disable-background-networking")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--hide-scrollbars")
            chrome_options.add_argument("--metrics-recording-only")
            chrome_options.add_argument("--mute-audio")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--safebrowsing-disable-auto-update")
            chrome_options.add_argument("--disable-component-update")
            chrome_options.add_argument("--disable-domain-reliability")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_experimental_option("prefs", {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.images": 2
            })
            chrome_options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--remote-debugging-port=0")  # Let Chrome choose port

            # Suppress Chrome output by redirecting to null
            service_args = []
            if os.name == 'nt':  # Windows
                service_args.append('--log-path=nul')
            else:  # Unix/Linux
                service_args.append('--log-path=/dev/null')

            # Try multiple methods to create driver
            driver_created = False

            # Method 1: Auto-detection with suppressed output
            try:
                logger.debug("Attempting to create Chrome driver with auto-detection...")
                service = Service(log_path=os.devnull if os.name != 'nt' else 'nul')
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                driver_created = True
                logger.info("Chrome driver initialized successfully (auto-detection)")
            except WebDriverException as e:
                logger.warning(f"Auto-detection failed: {e}")

            # Method 2: Specific chromedriver.exe
            if not driver_created:
                try:
                    logger.debug("Attempting to create Chrome driver with chromedriver.exe...")
                    service = Service("chromedriver.exe", log_path=os.devnull if os.name != 'nt' else 'nul')
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    driver_created = True
                    logger.info("Chrome driver initialized successfully (chromedriver.exe)")
                except WebDriverException as e:
                    logger.error(f"chromedriver.exe failed: {e}")

            if not driver_created:
                logger.error("Failed to create Chrome driver with any method")
                return False

            # Configure wait
            self.wait = WebDriverWait(self.driver, 20)

            # Execute script to hide automation indicators
            try:
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            except Exception as e:
                logger.warning(f"Failed to hide automation indicators: {e}")

            # Test the driver with a simple navigation
            try:
                self.driver.get("about:blank")
                logger.debug("Driver test successful")
            except Exception as e:
                logger.error(f"Driver test failed: {e}")
                self.close()
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to setup Chrome driver: {e}")
            return False

    def extract_product_name(self, full_name: str) -> str:
        """Extract product name - for Target, usually return the full name as-is"""
        if not full_name:
            return "Unknown Product"

        logger.debug(f"Extracting product name from: {full_name}")

        # Check for error messages first
        error_patterns = [
            r"We're sorry, something went wrong",
            r"We\u2019re sorry, something went wrong",
            r"Were sorry, something went wrong",
            r"Page not found",
            r"Product not available",
            r"Item not found"
        ]

        for pattern in error_patterns:
            if re.search(pattern, full_name, re.IGNORECASE):
                logger.warning(f"Error page detected: {full_name}")
                return None  # Signal that we should use SKU name

        # For Target products, typically return the full name
        # You can add specific extraction rules here if needed
        logger.debug(f"Using full name: {full_name}")
        return full_name.strip()

    def scrape_product_info(self, sku: str) -> Tuple[Optional[str], Optional[str]]:
        """Scrape product name and thumbnail URL for a given SKU from Target"""
        if not self.driver:
            logger.error("Driver not initialized")
            return None, None

        # Use the correct Target URL format
        url = f"https://www.target.com/p/-/A-{sku}"

        try:
            logger.info(f"Scraping Target product info for SKU {sku}")

            # Navigate to the page
            start_time = time.time()
            self.driver.get(url)
            logger.debug(f"Page navigation took {time.time() - start_time:.2f} seconds")

            # Wait for page to load
            time.sleep(4)

            # Check if page loaded properly
            page_title = self.driver.title
            logger.debug(f"Page title: {page_title}")

            # Check for error pages
            if "error" in page_title.lower() or "not found" in page_title.lower() or "target" not in page_title.lower():
                logger.warning(f"Error page detected for SKU {sku}: {page_title}")
                return f"Unknown Product (SKU: {sku})", None

            product_name = None
            thumbnail_url = None

            # Try to get product name with Target-specific selectors
            try:
                logger.debug("Attempting to find Target product title...")

                title_selectors = [
                    # Primary Target product title selector
                    "//h1[@id='pdp-product-title-id']",
                    "//h1[@data-test='product-title']",
                    # Alternative selectors
                    "//h1[contains(@class, 'ndsHeading')]",
                    "//h1[contains(@class, 'product-title')]",
                    "//h1",
                    # Span selector as fallback
                    "//span[@data-test='product-title']"
                ]

                title_element = None
                for i, selector in enumerate(title_selectors):
                    try:
                        logger.debug(f"Trying Target title selector {i + 1}: {selector}")
                        title_element = self.driver.find_element(By.XPATH, selector)
                        if title_element and title_element.text.strip():
                            logger.debug(f"Found title with selector {i + 1}")
                            break
                    except NoSuchElementException:
                        logger.debug(f"Title selector {i + 1} failed")
                        continue

                if title_element:
                    full_name = title_element.text.strip()
                    logger.info(f"Raw Target product title found: {full_name}")

                    extracted_name = self.extract_product_name(full_name)
                    if extracted_name is None:
                        # Error page detected, use SKU name
                        product_name = f"Unknown Product (SKU: {sku})"
                        logger.warning(f"Error page detected, using SKU name: {product_name}")
                    else:
                        product_name = extracted_name
                        logger.info(f"Extracted product name: {product_name}")
                else:
                    logger.warning(f"Could not find product title element for SKU {sku}")
                    product_name = f"Unknown Product (SKU: {sku})"

            except Exception as e:
                logger.error(f"Error extracting product name for SKU {sku}: {e}")
                product_name = f"Unknown Product (SKU: {sku})"

            # Try to get thumbnail URL with Target-specific selectors
            try:
                logger.debug("Attempting to find Target product image...")

                img_selectors = [
                    # Primary Target image gallery selector
                    "//div[@id='PdpImageGallerySection']//img",
                    # Alternative Target image selectors
                    "//div[contains(@class, 'image-gallery')]//img",
                    "//div[contains(@class, 'product-image')]//img",
                    "//img[contains(@src, 'target.scene7.com')]",
                    "//img[contains(@src, 'Target/')]",
                    # General image selectors
                    "//img[contains(@alt, 'of')]",
                    "//main//img[1]",
                    "//img[contains(@class, 'product')]"
                ]

                img_element = None
                successful_selector = None

                for i, selector in enumerate(img_selectors):
                    try:
                        logger.debug(f"Trying Target image selector {i + 1}: {selector}")
                        img_element = self.driver.find_element(By.XPATH, selector)
                        if img_element:
                            successful_selector = selector
                            logger.debug(f"Found image with selector {i + 1}")
                            break
                    except NoSuchElementException:
                        logger.debug(f"Image selector {i + 1} failed")
                        continue

                if img_element:
                    logger.debug(f"Successful image selector: {successful_selector}")

                    # Try to get srcset first (higher quality)
                    srcset = img_element.get_attribute("srcset")
                    src = img_element.get_attribute("src")

                    logger.debug(f"Image srcset: {srcset}")
                    logger.debug(f"Image src: {src}")

                    if srcset:
                        # Extract URLs from srcset
                        urls = re.findall(r'(https://[^\s,]+)', srcset)
                        if urls:
                            # Take a medium quality URL (not the smallest, not the largest)
                            if len(urls) >= 3:
                                thumbnail_url = urls[2]  # Usually 600px version
                            else:
                                thumbnail_url = urls[-1]  # Highest available

                            # Convert WebP to PNG for better Discord compatibility
                            if 'fmt=webp' in thumbnail_url:
                                thumbnail_url = thumbnail_url.replace('fmt=webp', 'fmt=png')
                            elif 'fmt=pjpeg' in thumbnail_url:
                                thumbnail_url = thumbnail_url.replace('fmt=pjpeg', 'fmt=png')

                            logger.info(f"Extracted thumbnail from srcset: {thumbnail_url}")
                        else:
                            logger.warning("Srcset found but no URLs extracted")

                    if not thumbnail_url and src:
                        thumbnail_url = src
                        # Convert WebP to PNG for better Discord compatibility
                        if 'fmt=webp' in thumbnail_url:
                            thumbnail_url = thumbnail_url.replace('fmt=webp', 'fmt=png')
                        elif 'fmt=pjpeg' in thumbnail_url:
                            thumbnail_url = thumbnail_url.replace('fmt=pjpeg', 'fmt=png')
                        logger.info(f"Using src as thumbnail: {thumbnail_url}")

                    if not thumbnail_url:
                        logger.warning("Image element found but no src/srcset available")
                else:
                    logger.warning(f"Could not find product image for SKU {sku}")

            except Exception as e:
                logger.error(f"Error extracting thumbnail for SKU {sku}: {e}")

            logger.info(
                f"Target scraping complete for SKU {sku} - Name: {product_name}, Thumbnail: {bool(thumbnail_url)}")
            return product_name, thumbnail_url

        except TimeoutException:
            logger.error(f"Page load timeout for SKU {sku}")
            return f"Unknown Product (SKU: {sku})", None
        except Exception as e:
            logger.error(f"Error scraping SKU {sku}: {e}")
            return f"Unknown Product (SKU: {sku})", None

    def close(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser driver closed")
            except Exception as e:
                logger.error(f"Error closing driver: {e}")


class ProductInfoUpdater:
    def __init__(self, discord_handler):
        self.discord_handler = discord_handler
        self.scraper = None
        self.scraping_thread = None
        self.is_scraping = False

    def needs_scraping(self, sku: str, product_info: Dict) -> bool:
        """Check if a product needs to be scraped"""
        name = product_info.get('name', '')
        thumbnail_url = product_info.get('thumbnail_url', '')

        # Need scraping if name is unknown or missing thumbnail
        needs_name = 'Unknown Product' in name or not name
        needs_thumbnail = not thumbnail_url or thumbnail_url == ""

        return needs_name or needs_thumbnail

    def scrape_single_sku(self, sku: str, force_update: bool = False) -> Tuple[str, bool]:
        """Scrape a single SKU and return (result_message, success)"""
        try:
            if not self.scraper:
                logger.error("Scraper not initialized")
                return f"SKU {sku}: Scraper not initialized", False

            # Get current product info
            current_info = self.discord_handler._get_product_info(sku)
            current_name = current_info.get('name', f'Unknown Product (SKU: {sku})')
            current_thumbnail = current_info.get('thumbnail_url', '')
            current_send_initial = current_info.get('send_initial', True)

            # Check if scraping is needed
            if not force_update and not self.needs_scraping(sku, current_info):
                return f"SKU {sku}: No updates needed", True

            # Scrape new info
            scraped_name, scraped_thumbnail = self.scraper.scrape_product_info(sku)

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
                return f"SKU {sku}: Updated {', '.join(updated_fields)}", True
            else:
                return f"SKU {sku}: No updates needed", True

        except Exception as e:
            logger.error(f"Error scraping SKU {sku}: {e}")
            return f"SKU {sku}: Error - {str(e)}", False

    def update_products_from_web_background(self, sku_list: list = None, force_update: bool = False):
        """Update product information in background thread"""

        def background_scraper():
            try:
                self.is_scraping = True
                logger.info("Starting background Target product scraping...")

                # Load current products
                products = self.discord_handler._load_products()

                # Determine which SKUs to process
                if sku_list:
                    skus_to_process = sku_list
                else:
                    skus_to_process = []
                    for sku, product_info in products.items():
                        if force_update or self.needs_scraping(sku, product_info):
                            skus_to_process.append(sku)

                if not skus_to_process:
                    logger.info("No SKUs need scraping")
                    return

                logger.info(f"Background scraping {len(skus_to_process)} Target SKUs")

                # Initialize scraper
                self.scraper = TargetScraper()
                if not self.scraper.setup_driver():
                    logger.error("Failed to setup Target web scraper")
                    return

                # Process SKUs one by one
                updated_count = 0
                for i, sku in enumerate(skus_to_process):
                    if not self.is_scraping:  # Allow stopping
                        break

                    try:
                        result_message, success = self.scrape_single_sku(sku, force_update)
                        if success and "Updated" in result_message:
                            updated_count += 1

                        logger.info(f"[{i + 1}/{len(skus_to_process)}] {result_message}")

                        # Add delay between requests to be respectful to Target
                        time.sleep(3)

                    except Exception as e:
                        logger.error(f"Error processing Target SKU {sku}: {e}")
                        continue

                logger.info(f"Background Target scraping completed. Updated {updated_count} products.")

            except Exception as e:
                logger.error(f"Error in background Target scraping: {e}")
            finally:
                if self.scraper:
                    self.scraper.close()
                self.is_scraping = False

        # Start background thread
        if not self.is_scraping:
            self.scraping_thread = threading.Thread(target=background_scraper, daemon=True)
            self.scraping_thread.start()
            logger.info("Started background Target scraping thread")
        else:
            logger.warning("Target scraping already in progress")

    def update_products_from_web(self, sku_list: list = None, force_update: bool = False):
        """Update product information (non-blocking version)"""
        self.update_products_from_web_background(sku_list, force_update)
        return True  # Return immediately

    def stop_scraping(self):
        """Stop background scraping"""
        if self.is_scraping:
            logger.info("Stopping background Target scraping...")
            self.is_scraping = False
            if self.scraping_thread and self.scraping_thread.is_alive():
                self.scraping_thread.join(timeout=5)

    def update_single_sku(self, sku: str) -> bool:
        """Update a single SKU's information"""
        return self.update_products_from_web([sku], force_update=True)