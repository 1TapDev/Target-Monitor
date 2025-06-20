import requests
from typing import Dict, List, Optional
import logging
import time
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class EnhancedTargetAPI:
    """Enhanced Best Buy API client with rate limiting and error handling"""

    def __init__(self, proxy_manager=None):
        self.base_url = "https://api.snormax.com/stock/target"
        self.headers = {
            'authorization': 'Bearer null',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'DNT': '1'
        }

        self.proxy_manager = proxy_manager
        self.session = self._create_session()

        # Enhanced error tracking
        self.error_counts = {
            'timeouts': 0,
            'connection_errors': 0,
            'http_errors': 0,
            'parse_errors': 0,
            'rate_limit_errors': 0
        }

        self.last_request_time = 0
        self.consecutive_failures = 0
        self.last_success_time = time.time()

        # Request statistics
        self.total_requests = 0
        self.successful_requests = 0
        self.cache = {}
        self.cache_duration = 30  # seconds

    def _create_session(self):
        """Create optimized requests session"""
        session = requests.Session()
        session.headers.update(self.headers)

        # Configure connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0,  # We handle retries manually
            socket_options=[(1, 2, 1)]  # TCP keepalive
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        return session

    def get_stock_data(self, sku: str, zip_code: str, use_cache: bool = True) -> Optional[Dict]:
        """Enhanced stock data retrieval with caching and error handling"""

        # Check cache first
        cache_key = f"{sku}_{zip_code}"
        if use_cache and self._is_cache_valid(cache_key):
            logger.debug(f"Cache hit for SKU {sku}, ZIP {zip_code}")
            return self.cache[cache_key]['data']

        # Prepare request
        url = f"{self.base_url}?sku={sku}&zip={zip_code}"
        self.total_requests += 1

        # Add minimum delay between requests
        self._enforce_rate_limit()

        # Get proxy if available
        proxies = None
        current_proxy = None
        if self.proxy_manager:
            current_proxy = self.proxy_manager.get_next_proxy()
            if current_proxy:
                proxies = self.proxy_manager.get_proxy_dict(current_proxy)

        # Make request with retries
        max_retries = 3
        for attempt in range(max_retries):
            try:
                start_time = time.time()

                response = self.session.get(
                    url,
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=False
                )

                request_duration = time.time() - start_time
                self.last_request_time = time.time()

                # Handle response
                if response.status_code == 200:
                    try:
                        data = response.json()

                        # Validate response structure
                        if self._validate_response(data):
                            # Cache successful response
                            self.cache[cache_key] = {
                                'data': data,
                                'timestamp': time.time()
                            }

                            self.successful_requests += 1
                            self.consecutive_failures = 0
                            self.last_success_time = time.time()

                            logger.debug(f"API success: SKU {sku} in {request_duration:.2f}s")
                            return data
                        else:
                            logger.warning(f"Invalid response structure for SKU {sku}")
                            self.error_counts['parse_errors'] += 1

                    except ValueError as e:
                        logger.error(f"JSON decode error for SKU {sku}: {e}")
                        self.error_counts['parse_errors'] += 1

                elif response.status_code == 429:
                    # Rate limit hit
                    self.error_counts['rate_limit_errors'] += 1
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited for SKU {sku}, waiting {retry_after}s")

                    if attempt < max_retries - 1:
                        time.sleep(retry_after)
                        continue

                elif response.status_code in [500, 502, 503, 504]:
                    # Server errors - retry with backoff
                    self.error_counts['http_errors'] += 1
                    if attempt < max_retries - 1:
                        wait_time = min(60, 2 ** attempt + random.uniform(0, 1))
                        logger.warning(f"Server error {response.status_code} for SKU {sku}, "
                                       f"retrying in {wait_time:.1f}s")
                        time.sleep(wait_time)
                        continue

                else:
                    # Other HTTP errors
                    self.error_counts['http_errors'] += 1
                    logger.error(f"HTTP {response.status_code} for SKU {sku}: {response.text[:200]}")

                # If we get here, request failed
                self._handle_request_failure(current_proxy)
                break

            except requests.exceptions.Timeout:
                self.error_counts['timeouts'] += 1
                logger.warning(f"Timeout for SKU {sku} (attempt {attempt + 1})")

                if attempt < max_retries - 1:
                    wait_time = min(30, 5 * (attempt + 1))
                    time.sleep(wait_time)
                    continue

            except requests.exceptions.ConnectionError as e:
                self.error_counts['connection_errors'] += 1
                logger.warning(f"Connection error for SKU {sku}: {e}")

                # Mark proxy as failed if using one
                if current_proxy and self.proxy_manager:
                    self.proxy_manager.mark_proxy_failed(current_proxy)
                    # Get new proxy for retry
                    current_proxy = self.proxy_manager.get_next_proxy()
                    if current_proxy:
                        proxies = self.proxy_manager.get_proxy_dict(current_proxy)

                if attempt < max_retries - 1:
                    wait_time = min(30, 10 * (attempt + 1))
                    time.sleep(wait_time)
                    continue

            except Exception as e:
                logger.error(f"Unexpected error for SKU {sku}: {e}")
                break

        # All retries failed
        self._handle_request_failure(current_proxy)
        return None

    def _validate_response(self, data: Dict) -> bool:
        """Validate API response structure"""
        if not isinstance(data, dict):
            return False

        # Check for required fields
        if 'locations' not in data and 'items' not in data:
            return False

        # Check if we have actual data
        locations = data.get('locations', [])
        items = data.get('items', [])

        if not locations and not items:
            return False

        return True

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self.cache:
            return False

        cached_time = self.cache[cache_key]['timestamp']
        return time.time() - cached_time < self.cache_duration

    def _enforce_rate_limit(self):
        """Enforce minimum delay between requests"""
        min_delay = 1.0  # Minimum 1 second between requests

        if self.last_request_time > 0:
            elapsed = time.time() - self.last_request_time
            if elapsed < min_delay:
                sleep_time = min_delay - elapsed
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

    def _handle_request_failure(self, proxy: Optional[Dict] = None):
        """Handle failed request"""
        self.consecutive_failures += 1

        # Mark proxy as failed if using one
        if proxy and self.proxy_manager:
            self.proxy_manager.mark_proxy_failed(proxy)

        # Implement exponential backoff for consecutive failures
        if self.consecutive_failures >= 3:
            backoff_time = min(300, 30 * (2 ** (self.consecutive_failures - 3)))
            logger.warning(f"High failure rate, implementing {backoff_time}s backoff")
            time.sleep(backoff_time)

    def extract_stores_from_response(self, response_data: Dict) -> List[Dict]:
        """Extract store information from API response"""
        if not response_data:
            return []

        locations = response_data.get('locations', [])
        items = response_data.get('items', [])

        if not items:
            return []

        # Get the first item's location data
        item = items[0]
        item_locations = item.get('locations', [])

        stores = []
        for location in locations:
            store_id = location.get('id')

            # Find matching item location data
            item_location = None
            for il in item_locations:
                if il.get('locationId') == store_id:
                    item_location = il
                    break

            # Create store data combining location and availability info
            store_data = {
                'id': store_id,
                'name': location.get('name', ''),
                'address': location.get('address', ''),
                'city': location.get('city', ''),
                'state': location.get('state', ''),
                'zipCode': location.get('zipCode', ''),
                'phone': location.get('phone', ''),
                'distance': location.get('distance', 0),
                'locationFormat': location.get('locationFormat', ''),
                'locations': [item_location] if item_location else []
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

    def get_health_status(self) -> Dict:
        """Get API health status and statistics"""
        current_time = time.time()

        # Calculate success rate
        success_rate = 0.0
        if self.total_requests > 0:
            success_rate = self.successful_requests / self.total_requests

        # Time since last success
        time_since_success = current_time - self.last_success_time

        # Health assessment
        health_status = "HEALTHY"
        if success_rate < 0.5:
            health_status = "CRITICAL"
        elif success_rate < 0.8 or self.consecutive_failures >= 5:
            health_status = "DEGRADED"
        elif time_since_success > 600:  # 10 minutes
            health_status = "STALE"

        return {
            'status': health_status,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'success_rate': success_rate,
            'consecutive_failures': self.consecutive_failures,
            'time_since_last_success': time_since_success,
            'error_counts': self.error_counts.copy(),
            'cache_size': len(self.cache),
            'proxy_status': self._get_proxy_status()
        }

    def _get_proxy_status(self) -> Dict:
        """Get proxy manager status if available"""
        if not self.proxy_manager:
            return {'enabled': False}

        return {
            'enabled': True,
            'working_proxies': self.proxy_manager.get_working_proxy_count(),
            'total_proxies': len(self.proxy_manager.proxies),
            'failed_proxies': len(self.proxy_manager.failed_proxies)
        }

    def reset_error_counts(self):
        """Reset error counters (useful for health monitoring)"""
        self.error_counts = {key: 0 for key in self.error_counts}
        self.consecutive_failures = 0
        logger.info("API error counts reset")

    def clear_cache(self):
        """Clear the response cache"""
        cache_size = len(self.cache)
        self.cache.clear()
        logger.info(f"Cleared {cache_size} cached responses")

    def cleanup_old_cache(self):
        """Remove expired cache entries"""
        current_time = time.time()
        expired_keys = []

        for key, entry in self.cache.items():
            if current_time - entry['timestamp'] > self.cache_duration:
                expired_keys.append(key)

        for key in expired_keys:
            del self.cache[key]

        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

    def force_proxy_rotation(self):
        """Force proxy rotation if proxy manager is available"""
        if self.proxy_manager:
            new_proxy = self.proxy_manager.force_new_proxy()
            if new_proxy:
                logger.info(f"Forced proxy rotation to {new_proxy['ip']}:{new_proxy['port']}")
                return True
        return False

    def close(self):
        """Close the session and cleanup resources"""
        try:
            if self.session:
                self.session.close()

            # Log final statistics
            health = self.get_health_status()
            logger.info(f"API client closing - Final stats:")
            logger.info(f"  Total requests: {health['total_requests']}")
            logger.info(f"  Success rate: {health['success_rate']:.1%}")
            logger.info(f"  Cache entries: {health['cache_size']}")

        except Exception as e:
            logger.error(f"Error closing API client: {e}")


# Backwards compatibility wrapper
class TargetAPI(EnhancedTargetAPI):
    """Backwards compatible wrapper for existing code"""

    def __init__(self, proxy_manager=None):
        super().__init__(proxy_manager)
        logger.info("Using enhanced Best Buy API client")