import requests
from typing import Dict, List, Optional
import logging
import time
import random
import base64
import threading
import asyncio
from api.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


class TargetAPI:
    def __init__(self):
        self.proxy_manager = ProxyManager("proxies.txt")
        self.base_url = "https://api.snormax.com/stock/target"
        self.cache_busting_enabled = True  # Enable cache busting by default
        self.last_request_times = {}
        self.min_request_interval = 3.0  # Minimum 100ms between requests

        # Enhanced proxy validation
        self.validated_proxies = set()  # Store validated proxy URLs
        self.validation_lock = threading.Lock()
        self.validation_timeout = 15  # Timeout for proxy validation
        self.last_validation_check = {}  # Track when proxies were last validated
        self.validation_interval = 300  # Re-validate proxies every 5 minutes

        # Cache busting randomization
        self.cache_bust_probability = 0.5  # 50% chance to apply cache busting

        # Request spacing for same SKU-ZIP combinations
        self.request_history = {}  # Track last request times per SKU-ZIP
        self.min_same_request_interval = 30  # 30 seconds between same SKU-ZIP requests

        self.headers = {
            'authorization': 'Bearer null',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            # Remove Accept-Encoding to prevent compression issues
            # 'Accept-Encoding': 'gzip, deflate, br, zstd',
            'DNT': '1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Origin': 'https://www.snormax.com',
            'Referer': 'https://www.snormax.com/',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        }

        # Use session with connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Configure connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,  # Number of connection pools to cache
            pool_maxsize=20,  # Number of connections to save in the pool
            max_retries=3  # Number of retries
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def is_proxy_valid_for_snormax(self, proxy_dict: Dict) -> bool:
        """
        Validate that a proxy can successfully communicate with Snormax API.

        Args:
            proxy_dict: Proxy configuration dict with 'http' and 'https' keys

        Returns:
            bool: True if proxy is valid for Snormax API, False otherwise
        """
        if not proxy_dict:
            return False

        test_url = f"{self.base_url}?sku=94693225&zip=30313"

        try:
            logger.debug(f"Validating proxy: {proxy_dict.get('http', 'Unknown')}")

            # Make test request with short timeout
            response = self.session.get(
                test_url,
                proxies=proxy_dict,
                timeout=self.validation_timeout,
                allow_redirects=False
            )

            # Check HTTP status
            if response.status_code != 200:
                logger.warning(f"Proxy validation failed - HTTP {response.status_code}")
                return False

            # Attempt to parse JSON
            try:
                # Handle potential compression issues in validation too
                content = response.content

                # Check if response might be compressed but not auto-decompressed
                if (content[:2] == b'\x1f\x8b' or content[:2] == b'\x78\x9c' or content[:2] == b'\x78\x01'):
                    logger.debug("Validation response appears compressed, attempting decompression")

                    try:
                        if content[:2] == b'\x1f\x8b':  # GZIP
                            import gzip
                            content = gzip.decompress(content)
                        elif content[:2] in [b'\x78\x9c', b'\x78\x01']:  # DEFLATE
                            import zlib
                            content = zlib.decompress(content)
                    except Exception as decomp_error:
                        logger.warning(f"Validation decompression failed: {decomp_error}")
                        # Fall back to original response.json()
                        content = None

                # Try to parse JSON
                if content:
                    import json
                    if isinstance(content, bytes):
                        text_content = content.decode('utf-8', errors='ignore')
                    else:
                        text_content = str(content)
                    data = json.loads(text_content)
                else:
                    # Fall back to response.json()
                    data = response.json()

                # Check for expected JSON structure
                if isinstance(data, dict) and ('items' in data or 'locations' in data):
                    logger.debug("Proxy validation successful - valid JSON response")
                    return True
                else:
                    logger.warning("Proxy validation failed - missing expected JSON keys")
                    return False

            except ValueError as e:
                # JSON parsing failed - log detailed diagnostics
                logger.error(f"Proxy validation failed - JSON parsing error: {e}")
                self._log_response_diagnostics(response, proxy_dict.get('http', 'Unknown'))
                return False

        except requests.exceptions.Timeout:
            logger.warning(f"Proxy validation timeout for {proxy_dict.get('http', 'Unknown')}")
            return False
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Proxy validation connection error: {e}")
            return False
        except Exception as e:
            logger.error(f"Proxy validation unexpected error: {e}")
            return False

    def _log_response_diagnostics(self, response: requests.Response, proxy_ip: str):
        """
        Log detailed diagnostics when JSON parsing fails.

        Args:
            response: The failed response object
            proxy_ip: IP of the proxy used
        """
        try:
            content = response.content
            status_code = response.status_code
            content_type = response.headers.get('content-type', 'unknown')
            content_encoding = response.headers.get('content-encoding', 'none')

            logger.error(f"=== RESPONSE DIAGNOSTIC DATA ===")
            logger.error(f"Proxy IP: {proxy_ip}")
            logger.error(f"HTTP Status: {status_code}")
            logger.error(f"Content-Type: {content_type}")
            logger.error(f"Content-Encoding: {content_encoding}")
            logger.error(f"Content Length: {len(content)} bytes")

            # Check if content might be compressed
            is_gzip = content[:2] == b'\x1f\x8b'
            is_deflate = content[:2] == b'\x78\x9c' or content[:2] == b'\x78\x01'

            logger.error(f"Compression detected - GZIP: {is_gzip}, DEFLATE: {is_deflate}")

            # Try to decompress if it looks compressed
            decompressed_text = None
            if is_gzip:
                try:
                    import gzip
                    decompressed_content = gzip.decompress(content)
                    decompressed_text = decompressed_content.decode('utf-8', errors='ignore')
                    logger.error(f"GZIP Decompressed: {decompressed_text[:500]}")
                except Exception as e:
                    logger.error(f"GZIP decompression failed: {e}")

            elif is_deflate:
                try:
                    import zlib
                    decompressed_content = zlib.decompress(content)
                    decompressed_text = decompressed_content.decode('utf-8', errors='ignore')
                    logger.error(f"DEFLATE Decompressed: {decompressed_text[:500]}")
                except Exception as e:
                    logger.error(f"DEFLATE decompression failed: {e}")

            # Log first 500 characters of raw text
            try:
                text_preview = content.decode('utf-8', errors='ignore')[:500]
                logger.error(f"Raw Text Preview: {text_preview}")
            except:
                logger.error("Could not decode content as text")

            # Log HEX format (first 200 bytes)
            hex_content = content.hex()[:400]  # First 200 bytes in hex
            logger.error(f"HEX Content: {hex_content}")

            # Log BASE64 format (first ~300 bytes)
            b64_content = base64.b64encode(content).decode()[:400]
            logger.error(f"BASE64 Content: {b64_content}")

            # Log all response headers for debugging
            logger.error("Response Headers:")
            for header, value in response.headers.items():
                logger.error(f"  {header}: {value}")

            logger.error(f"=== END DIAGNOSTIC DATA ===")

        except Exception as e:
            logger.error(f"Error logging response diagnostics: {e}")

    def _get_validated_proxy(self) -> Optional[Dict]:
        """
        Get a validated proxy that works with Snormax API.

        Returns:
            Dict or None: Validated proxy configuration or None
        """
        with self.validation_lock:
            current_time = time.time()

            # Try to get a proxy that's already validated and recent
            for _ in range(10):  # Try up to 10 proxies
                proxy = self.proxy_manager.get_random_proxy()
                if not proxy:
                    break

                proxy_dict = self.proxy_manager.get_proxy_dict(proxy)
                if not proxy_dict:
                    continue

                proxy_url = proxy_dict.get('http', '')

                # Check if proxy was recently validated
                last_validated = self.last_validation_check.get(proxy_url, 0)
                is_recently_validated = (current_time - last_validated) < self.validation_interval

                if proxy_url in self.validated_proxies and is_recently_validated:
                    logger.debug(f"Using recently validated proxy: {proxy['ip']}:{proxy['port']}")
                    return proxy_dict

                # Need to validate this proxy
                if self.is_proxy_valid_for_snormax(proxy_dict):
                    self.validated_proxies.add(proxy_url)
                    self.last_validation_check[proxy_url] = current_time
                    logger.debug(f"Validated new proxy for Snormax: {proxy['ip']}:{proxy['port']}")
                    return proxy_dict
                else:
                    # Mark proxy as failed and remove from validated set
                    self.proxy_manager.mark_proxy_failed(proxy)
                    self.validated_proxies.discard(proxy_url)
                    self.last_validation_check.pop(proxy_url, None)
                    logger.warning(f"Proxy failed validation, marked as bad: {proxy['ip']}:{proxy['port']}")

            logger.warning("No valid proxies available for Snormax API")
            return None

    def _should_apply_cache_busting(self) -> bool:
        """
        Randomly decide whether to apply cache busting parameters.

        Returns:
            bool: True if cache busting should be applied
        """
        return random.random() < self.cache_bust_probability

    def _enforce_request_spacing(self, sku: str, zip_code: str):
        """Enforce request spacing to avoid rate limiting"""
        current_time = time.time()
        request_key = f"{sku}_{zip_code}"

        if request_key in self.last_request_times:
            time_since_last = current_time - self.last_request_times[request_key]
            if time_since_last < self.min_request_interval:
                sleep_time = self.min_request_interval - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s for {request_key}")

                # Check if we're in an async context
                try:
                    loop = asyncio.get_running_loop()
                    # We're in async context but this is a sync method
                    # Let the thread pool handle the sleep
                    time.sleep(sleep_time)
                except RuntimeError:
                    # No event loop running, safe to use blocking sleep
                    time.sleep(sleep_time)

        self.last_request_times[request_key] = current_time

    def set_cache_busting(self, enabled: bool):
        """Enable or disable cache busting"""
        self.cache_busting_enabled = enabled
        logger.info(f"Cache busting {'enabled' if enabled else 'disabled'}")

    def get_stock_data(self, sku: str, zip_code: str, force_fresh: bool = False) -> Optional[Dict]:
        """
        Get stock data for a specific SKU and ZIP code with enhanced proxy validation
        and error handling.

        Args:
            sku: Product SKU
            zip_code: ZIP code to check
            force_fresh: Force fresh data (add cache-busting parameters)

        Returns:
            API response data or None if failed
        """
        # Enforce request spacing for same SKU-ZIP combinations
        self._enforce_request_spacing(sku, zip_code)

        # Get a validated proxy for this request
        proxy_dict = self._get_validated_proxy()
        current_proxy = None

        if proxy_dict:
            # Extract proxy info for logging
            proxy_url = proxy_dict.get('http', '')
            if proxy_url:
                # Parse proxy URL to get IP/port for logging
                try:
                    if '@' in proxy_url:
                        # Format: http://user:pass@ip:port
                        proxy_host = proxy_url.split('@')[1]
                    else:
                        # Format: http://ip:port
                        proxy_host = proxy_url.replace('http://', '').replace('https://', '')

                    logger.debug(f"Using validated proxy {proxy_host} for SKU {sku}")
                except:
                    pass

        try:
            # Build URL with query parameters
            params = {
                'sku': sku,
                'zip': zip_code
            }

            # Apply cache-busting parameters based on randomization and settings
            apply_cache_bust = (self.cache_busting_enabled and self._should_apply_cache_busting()) or force_fresh

            if apply_cache_bust:
                params.update({
                    '_t': int(time.time() * 1000),  # Timestamp in milliseconds
                    '_r': random.randint(1000, 9999)  # Random number
                })
                logger.debug(f"Applying cache-busting for SKU {sku}")

            # Set additional cache-busting headers if needed
            request_headers = {}
            if apply_cache_bust:
                request_headers.update({
                    'Cache-Control': 'no-cache, no-store, must-revalidate',
                    'Pragma': 'no-cache',
                    'Expires': '0',
                    'If-Modified-Since': 'Thu, 01 Jan 1970 00:00:00 GMT'
                })

            # Make the request
            response = self.session.get(
                self.base_url,
                params=params,
                headers=request_headers,
                proxies=proxy_dict,
                timeout=30
            )

            if response.status_code == 200:
                try:
                    # Debug: Log response details
                    logger.debug(f"Response content-type: {response.headers.get('content-type', 'unknown')}")
                    logger.debug(f"Response content-encoding: {response.headers.get('content-encoding', 'none')}")
                    logger.debug(f"Response length: {len(response.content)} bytes")

                    # Handle potential compression issues
                    content = response.content

                    # Check if response might be compressed but not auto-decompressed
                    if (content[:2] == b'\x1f\x8b' or content[:2] == b'\x78\x9c' or content[:2] == b'\x78\x01'):
                        logger.warning(f"Response appears compressed but not decompressed for SKU {sku}")

                        # Try manual decompression
                        try:
                            if content[:2] == b'\x1f\x8b':  # GZIP
                                import gzip
                                content = gzip.decompress(content)
                                logger.debug("Successfully decompressed GZIP response")
                            elif content[:2] in [b'\x78\x9c', b'\x78\x01']:  # DEFLATE
                                import zlib
                                content = zlib.decompress(content)
                                logger.debug("Successfully decompressed DEFLATE response")
                        except Exception as decomp_error:
                            logger.error(f"Manual decompression failed for SKU {sku}: {decomp_error}")
                            # Fall back to original content
                            content = response.content

                    # Try to parse JSON from (potentially decompressed) content
                    if isinstance(content, bytes):
                        text_content = content.decode('utf-8', errors='ignore')
                    else:
                        text_content = str(content)

                    # Use response.json() first (handles encoding automatically)
                    try:
                        data = response.json()
                    except ValueError:
                        # If response.json() fails, try parsing the manually processed content
                        import json
                        data = json.loads(text_content)

                    # Validate response structure
                    if not isinstance(data, dict) or ('items' not in data and 'locations' not in data):
                        logger.warning(f"Invalid response structure for SKU {sku}")
                        return None

                    logger.debug(f"Successfully fetched data for SKU {sku}")
                    return data

                except ValueError as e:
                    # JSON parsing failed - log comprehensive diagnostics
                    logger.error(f"JSON parsing failed for SKU {sku}: {e}")

                    # Get proxy IP for logging (extract just the IP, not full URL)
                    proxy_ip = "No Proxy"
                    if proxy_dict:
                        proxy_url = proxy_dict.get('http', '')
                        try:
                            # Extract just the IP from various proxy URL formats
                            if '@' in proxy_url:
                                # Format: http://user:pass@ip:port -> get ip
                                host_part = proxy_url.split('@')[1]
                                proxy_ip = host_part.split(':')[0]
                            else:
                                # Format: http://ip:port -> get ip
                                host_part = proxy_url.replace('http://', '').replace('https://', '')
                                proxy_ip = host_part.split(':')[0]
                        except:
                            proxy_ip = "Parse Error"

                    self._log_response_diagnostics(response, proxy_ip)

                    # Mark proxy as potentially bad if we get non-JSON response
                    if proxy_dict and 'html' in response.headers.get('content-type', '').lower():
                        # Find the original proxy object to mark as failed
                        proxy_url = proxy_dict.get('http', '')
                        with self.validation_lock:
                            self.validated_proxies.discard(proxy_url)
                            self.last_validation_check.pop(proxy_url, None)

                        # Also mark in proxy manager
                        for proxy in self.proxy_manager.proxies:
                            if self.proxy_manager.get_proxy_dict(proxy) == proxy_dict:
                                self.proxy_manager.mark_proxy_failed(proxy)
                                logger.warning(
                                    f"Marked proxy as failed due to HTML response: {proxy['ip']}:{proxy['port']}")
                                break

                    return None

            else:
                # Log API request failures
                logger.error(f"API request failed for SKU {sku}: {response.status_code} - {response.text[:200]}")

                # Mark proxy as failed for certain status codes
                if response.status_code in [403, 429, 503] and proxy_dict:
                    proxy_url = proxy_dict.get('http', '')
                    with self.validation_lock:
                        self.validated_proxies.discard(proxy_url)
                        self.last_validation_check.pop(proxy_url, None)

                    # Mark in proxy manager
                    for proxy in self.proxy_manager.proxies:
                        if self.proxy_manager.get_proxy_dict(proxy) == proxy_dict:
                            self.proxy_manager.mark_proxy_failed(proxy)
                            logger.warning(
                                f"Marked proxy as failed due to status {response.status_code}: {proxy['ip']}:{proxy['port']}")
                            break

                return None

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching data for SKU {sku}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for SKU {sku}: {e}")

            # Mark proxy as failed on connection errors
            if proxy_dict:
                proxy_url = proxy_dict.get('http', '')
                with self.validation_lock:
                    self.validated_proxies.discard(proxy_url)
                    self.last_validation_check.pop(proxy_url, None)

                for proxy in self.proxy_manager.proxies:
                    if self.proxy_manager.get_proxy_dict(proxy) == proxy_dict:
                        self.proxy_manager.mark_proxy_failed(proxy)
                        break

            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for SKU {sku}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for SKU {sku}: {e}")
            return None

    def extract_stores_from_response(self, response_data: Dict) -> List[Dict]:
        """Extract store information from Target API response"""
        if not response_data:
            return []

        locations = response_data.get('locations', [])
        items = response_data.get('items', [])

        if not items or not locations:
            return []

        # Get the first item's location availability data
        item = items[0]
        item_locations = item.get('locations', [])

        stores = []
        for location in locations:
            store_id = location.get('id')

            # Find matching availability data for this store
            availability_data = None
            for item_location in item_locations:
                if item_location.get('locationId') == store_id:
                    availability_data = item_location
                    break

            # Extract quantities - Target has both pickup and in-store quantities
            pickup_quantity = 0
            instore_quantity = 0

            if availability_data:
                # Get pickup quantity
                availability = availability_data.get('availability', {})
                pickup_quantity = availability.get('availablePickupQuantity', 0)

                # Get in-store quantity
                instore_availability = availability_data.get('inStoreAvailability', {})
                instore_quantity = instore_availability.get('availableInStoreQuantity', 0)

            # Use the higher of the two quantities as the main quantity
            # You could also sum them or handle them separately based on your needs
            total_quantity = max(pickup_quantity, instore_quantity)

            # Create store data in the format expected by your existing code
            store_data = {
                'id': store_id,
                'name': location.get('name', ''),
                'address': location.get('address', ''),
                'city': location.get('city', ''),
                'state': location.get('state', ''),
                'zipCode': location.get('zipCode', ''),
                'phone': location.get('phone', ''),
                'distance': location.get('distance', 0),
                'quantity': total_quantity,  # Main quantity for compatibility
                'pickup_quantity': pickup_quantity,  # Target-specific: pickup orders
                'instore_quantity': instore_quantity,  # Target-specific: in-store shopping
                'availability_data': availability_data  # Raw availability data for debugging
            }

            stores.append(store_data)

        return stores

    def get_store_quantity(self, store_data: Dict) -> int:
        """Extract quantity from store data"""
        locations = store_data.get('locations', [])

        for location in locations:
            if not location:
                continue

            availability = location.get('availability', {})
            in_store = location.get('inStoreAvailability', {})

            # Check pickup quantity first
            pickup_qty = availability.get('availablePickupQuantity', 0)
            if pickup_qty and pickup_qty != 9999:
                return pickup_qty

            # Check in-store quantity
            instore_qty = in_store.get('availableInStoreQuantity', 0)
            if instore_qty and instore_qty != 9999:
                return instore_qty

            # If 9999, treat as high stock
            if pickup_qty == 9999 or instore_qty == 9999:
                return 999

        return 0

    def get_proxy_health_status(self) -> Dict:
        """
        Get health status of proxy pool.

        Returns:
            Dict with proxy health information
        """
        with self.validation_lock:
            total_proxies = len(self.proxy_manager.proxies)
            working_proxies = self.proxy_manager.get_working_proxy_count()
            validated_proxies = len(self.validated_proxies)
            failed_proxies = len(self.proxy_manager.failed_proxies)

            return {
                'total_proxies': total_proxies,
                'working_proxies': working_proxies,
                'validated_proxies': validated_proxies,
                'failed_proxies': failed_proxies,
                'health_percentage': (validated_proxies / max(1, total_proxies)) * 100,
                'last_validation_count': len(self.last_validation_check)
            }

    def cleanup_proxy_validation_cache(self):
        """Clean up old validation cache entries"""
        with self.validation_lock:
            current_time = time.time()
            expired_urls = []

            for proxy_url, last_check in self.last_validation_check.items():
                if current_time - last_check > self.validation_interval * 2:  # Double the interval
                    expired_urls.append(proxy_url)

            for url in expired_urls:
                self.last_validation_check.pop(url, None)
                self.validated_proxies.discard(url)

            if expired_urls:
                logger.info(f"Cleaned up {len(expired_urls)} expired proxy validation entries")

    def close(self):
        """Close the session"""
        try:
            self.session.close()
            logger.info("TargetAPI session closed")
        except Exception as e:
            logger.error(f"Error closing TargetAPI session: {e}")