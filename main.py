#!/usr/bin/env python3

import time
import signal
import sys
import asyncio
import os
from typing import Dict, List
import threading
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import warnings
import concurrent.futures
from threading import Lock

# Fix import path conflicts
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import modules with proper error handling
try:
    from utils.logger import setup_logger, setup_inventory_logger, log_inventory_change
    from utils.config import ConfigLoader
    from data.database import DatabaseManager
    from api.target_api import TargetAPI
    from integrations.discord_handler import DiscordHandler
    from commands.command_handler import CommandHandler
    from integrations.discord_bot import run_discord_bot, stop_discord_bot
    from integrations.scraper.target_scraper import ProductInfoUpdater
    from core.rate_limiter import EnhancedRateLimiter, RateLimitConfig
    from core.request_queue import SmartRequestQueue, RequestPriority
except ImportError as e:
    print(f"ImportError: {e}")
    print("Please ensure all modules are in the correct directories")
    sys.exit(1)

# Try enhanced scraper
try:
    from integrations.scraper.fallback_scraper import EnhancedProductInfoUpdater

    USE_ENHANCED_SCRAPER = True
except ImportError:
    USE_ENHANCED_SCRAPER = False


class EnhancedTargetMonitor:
    """Enhanced Target monitor with intelligent rate limiting and error handling"""

    def __init__(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{timestamp}.log"
        self.logger = setup_logger(log_filename=log_filename)
        self.running = False
        self.shutdown_requested = False

        # Configuration
        self.config = None
        self.config_loader = None
        self.config_last_modified = 0

        # Core components
        self.db_manager = None
        self.api = None
        self.discord_handler = None
        self.command_handler = None
        self.product_updater = None

        # Enhanced rate limiting and request management
        self.rate_limiter = None
        self.request_queue = None

        # Monitoring state
        self.startup_complete = False
        self.cycle_count = 0
        self.last_cycle_time = None
        self.next_cycle_time = None
        self.test_mode = False
        self.max_skus = None

        # Statistics and health tracking
        self.stats = {
            'total_cycles': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'stock_changes_detected': 0,
            'alerts_sent': 0,
            'start_time': None,
            'last_error': None,
            'error_count': 0
        }

        # Threading
        self.bot_task = None
        self.loop = None
        self.initial_stock_sent = set()
        self.new_product_timeouts = {}
        self.api_cache = {}
        self.cache_ttl = 30

        # Thread safety
        self.db_lock = Lock()

        # Force fresh data from API
        self.force_fresh_data = True

    def initialize(self):
        """Initialize all components with enhanced rate limiting"""
        try:
            # Load configuration
            self.config_loader = ConfigLoader()
            self.config = self.config_loader.load_config()
            self.config_last_modified = os.path.getmtime('config.json') if os.path.exists('config.json') else 0

            # Apply test mode limitations
            if self.test_mode and self.max_skus:
                original_count = len(self.config.skus)
                self.config.skus = self.config.skus[:self.max_skus]
                self.logger.info(f"TEST MODE: Limited to {len(self.config.skus)} SKUs (was {original_count})")

            # Validate configuration
            if not self._validate_config():
                return False

            # Setup inventory logger
            setup_inventory_logger()

            # Initialize database
            self.db_manager = DatabaseManager(self.config.database)
            self.db_manager.connect()
            self.db_manager.create_tables()

            # Initialize API client
            self.api = TargetAPI()

            # Set cache-busting if available
            if hasattr(self.api, 'set_cache_busting'):
                self.api.set_cache_busting(True)

            # Initialize Discord handler
            self.discord_handler = DiscordHandler(self.config.discord_webhook_url)

            # Initialize command handler
            self.command_handler = CommandHandler(self.discord_handler)

            # Initialize product updater
            if USE_ENHANCED_SCRAPER:
                self.product_updater = EnhancedProductInfoUpdater(self.discord_handler)
                self.logger.info("Using enhanced scraper with fallback support")
            else:
                self.product_updater = ProductInfoUpdater(self.discord_handler)
                self.logger.info("Using basic scraper")

            # Setup enhanced rate limiting
            self._setup_rate_limiting()

            # Calculate and log monitoring plan
            self._log_monitoring_plan()

            self.logger.info("Enhanced Target monitor initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Target monitor: {e}")
            return False

    def _validate_config(self) -> bool:
        """Validate configuration and calculate request load"""
        if not self.config.discord_webhook_url or "YOUR_WEBHOOK_URL" in self.config.discord_webhook_url:
            self.logger.error("Invalid Discord webhook URL in config.json")
            return False

        if not self.config.skus:
            self.logger.error("No SKUs configured for monitoring")
            return False

        if not self.config.zip_codes:
            self.logger.error("No ZIP codes configured for monitoring")
            return False

        # Calculate request load
        requests_per_cycle = len(self.config.skus) * len(self.config.zip_codes)
        cycles_per_day = 86400 / self.config.monitoring_interval
        daily_requests = requests_per_cycle * cycles_per_day

        # Warn if approaching limits
        if daily_requests > 40000:
            self.logger.warning(f"High request load: {daily_requests:.0f} requests/day")
            self.logger.warning("Consider increasing monitoring_interval to reduce load")

        return True

    def _setup_rate_limiting(self):
        """Setup enhanced rate limiting system"""
        # Calculate safe limits based on configuration
        requests_per_cycle = len(self.config.skus) * len(self.config.zip_codes)

        # Conservative rate limiting configuration
        rate_config = RateLimitConfig(
            max_requests_per_minute=min(20, requests_per_cycle + 5),
            max_requests_per_hour=1200,  # 20 per minute average
            max_requests_per_day=25000,  # Well under 50k limit
            burst_allowance=5,
            cooldown_period=60
        )

        self.rate_limiter = EnhancedRateLimiter(rate_config)

        self.request_queue = SmartRequestQueue(
            rate_limiter=self.rate_limiter,
            batch_size=3,
            max_concurrent=2
        )

        self.request_queue.start()

        self.logger.info(f"Enhanced rate limiting configured")
        self.logger.info(f"  - Max 20 requests/minute, 1200/hour, 25000/day")
        self.logger.info(f"  - Processing {requests_per_cycle} requests per cycle")

    def _log_monitoring_plan(self):
        """Log the monitoring plan and request calculations"""
        requests_per_cycle = len(self.config.skus) * len(self.config.zip_codes)
        cycles_per_day = 86400 / self.config.monitoring_interval
        daily_requests = requests_per_cycle * cycles_per_day

        self.logger.info("Enhanced Target Monitoring Plan:")
        self.logger.info(f"  - SKUs: {len(self.config.skus)}")
        self.logger.info(f"  - ZIP codes: {len(self.config.zip_codes)}")
        self.logger.info(f"  - Requests per cycle: {requests_per_cycle}")
        self.logger.info(f"  - Check interval: {self.config.monitoring_interval} seconds")
        self.logger.info(f"  - Cycles per day: {cycles_per_day:.1f}")
        self.logger.info(f"  - Daily requests: {daily_requests:.0f}")

        # Safety assessment
        safety_percentage = (daily_requests / 50000) * 100
        if safety_percentage < 60:
            safety_level = "SAFE"
        elif safety_percentage < 80:
            safety_level = "CAUTION"
        else:
            safety_level = "HIGH RISK"

        self.logger.info(f"  - API Usage: {safety_percentage:.1f}% of 50k limit ({safety_level})")

    def monitor_loop(self):
        """Enhanced monitoring loop with intelligent request queuing"""
        self.logger.info("Starting enhanced Target monitoring loop with intelligent rate limiting")
        self.stats['start_time'] = datetime.now()

        # If test mode, run fewer cycles
        max_cycles = 3 if self.test_mode else float('inf')

        # Run initial product info update on first start (non-blocking)
        if not self.startup_complete:
            self.logger.info("Starting initial product info update in background...")
            try:
                # Update products that need scraping in background
                self.product_updater.update_products_from_web()
            except Exception as e:
                self.logger.error(f"Error during initial product update: {e}")

        while self.running and not self.shutdown_requested and self.cycle_count < max_cycles:
            try:
                cycle_start = time.time()
                self.cycle_count += 1
                self.stats['total_cycles'] += 1

                self.logger.info(f"Starting enhanced cycle {self.cycle_count}")

                # Check for config updates
                self._check_config_updates()

                # Process all SKU-ZIP combinations with intelligent queuing
                self._queue_monitoring_cycle()

                # Wait for current cycle to complete
                self._wait_for_cycle_completion()

                # Calculate cycle timing
                cycle_duration = time.time() - cycle_start
                self.last_cycle_time = cycle_duration

                # Mark startup as complete after first cycle
                if not self.startup_complete:
                    self.startup_complete = True
                    self.logger.info(
                        "Initial startup cycle complete - subsequent cycles will only send alerts for changes")

                # Log cycle summary
                self._log_cycle_summary()

                if self.test_mode:
                    self.logger.info(f"TEST MODE: Completed cycle {self.cycle_count}/{max_cycles}")
                    if self.cycle_count >= max_cycles:
                        self.logger.info("TEST MODE: Maximum cycles reached, stopping")
                        break

                # Calculate next cycle time
                sleep_time = max(0, self.config.monitoring_interval - cycle_duration)
                self.next_cycle_time = time.time() + sleep_time

                # Sleep with responsive shutdown checking
                if not self.test_mode or self.cycle_count < max_cycles:
                    self.logger.info(f"Sleeping {sleep_time:.1f}s until next cycle")
                    self._smart_sleep(sleep_time)

            except KeyboardInterrupt:
                self.logger.info("Received Ctrl+C, stopping monitor...")
                self.shutdown_requested = True
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                self.stats['error_count'] += 1
                self.stats['last_error'] = str(e)
                if self.running and not self.shutdown_requested:
                    self._smart_sleep(60)  # Error recovery sleep

    def _queue_monitoring_cycle(self):
        """Queue all requests for current monitoring cycle with intelligent batching"""
        try:
            # Create list of all SKU-ZIP combinations
            requests = []
            for sku in self.config.skus:
                for zip_code in self.config.zip_codes:
                    requests.append((sku, zip_code))

            self.logger.info(f"Queuing {len(requests)} SKU-ZIP combinations for intelligent processing")

            # Process requests with enhanced rate limiting and batching
            request_ids = self.request_queue.add_batch_requests(
                requests=requests,
                callback=self._process_sku_zip_request,
                priority=RequestPriority.NORMAL,
                spread_delay=True
            )

            self.logger.info(f"Queued {len(request_ids)} requests for cycle {self.cycle_count}")

        except Exception as e:
            self.logger.error(f"Error queuing monitoring cycle: {e}")

    def _process_sku_zip_request(self, sku: str, zip_code: str) -> Dict:
        """Process a single SKU-ZIP request with enhanced error handling"""
        try:
            start_time = time.time()
            self.logger.debug(f"Processing SKU {sku}, ZIP {zip_code}")

            # Check for shutdown at the start
            if self.shutdown_requested:
                return {'success': False, 'error': 'Shutdown requested'}

            # Check if this is a new product in timeout period
            is_new_product_timeout = self._is_product_in_timeout(sku)

            if is_new_product_timeout:
                self._handle_new_product_during_timeout(sku)
                self.logger.debug(f"SKU {sku} in timeout period, processing data but suppressing alerts")

            # Get current stock data with caching and cache-busting
            response_data = self._get_cached_stock_data(sku, zip_code)

            if not response_data:
                self.stats['failed_requests'] += 1
                return {'success': False, 'error': 'No data received'}

            if self.shutdown_requested:
                return {'success': False, 'error': 'Shutdown requested'}

            # Extract stores from response
            stores = self.api.extract_stores_from_response(response_data)
            if not stores:
                self.stats['failed_requests'] += 1
                return {'success': False, 'error': 'No stores found'}

            # Process each store with database locking for thread safety
            changes_detected = 0
            stores_with_stock = []

            for store in stores:
                if self.shutdown_requested:
                    break

                try:
                    # Use database lock for thread safety
                    with self.db_lock:
                        changed, previous_qty = self.db_manager.update_stock(sku, store, 'target')

                    current_qty = self.api.get_store_quantity(store)

                    # Track stores with current stock for initial report
                    if current_qty > 0:
                        stores_with_stock.append({**store, 'quantity': current_qty})

                    if changed and previous_qty is not None:
                        changes_detected += 1
                        self.stats['stock_changes_detected'] += 1

                        # Send stock alert (only if not in test mode or timeout)
                        if not self.shutdown_requested and not self.test_mode and not is_new_product_timeout:
                            try:
                                self.discord_handler.send_stock_alert(sku, store, previous_qty, current_qty)
                                self.stats['alerts_sent'] += 1
                            except Exception as e:
                                self.logger.error(f"Failed to send Discord alert: {e}")
                        elif self.test_mode:
                            self.logger.info(
                                f"TEST MODE: Would send alert for {store.get('name')} - {previous_qty} -> {current_qty}")

                except Exception as e:
                    self.logger.error(f"Error processing store {store.get('id', 'unknown')}: {e}")

            # Handle initial stock reports (with locking)
            if stores_with_stock and not is_new_product_timeout:
                with self.db_lock:
                    self._handle_initial_stock_report(sku, zip_code, stores_with_stock)

            # Log if significant changes detected
            if changes_detected > 0:
                self.logger.info(f"SKU {sku}, ZIP {zip_code}: {changes_detected} changes detected")

            request_time = time.time() - start_time
            self.stats['successful_requests'] += 1

            return {
                'success': True,
                'sku': sku,
                'zip_code': zip_code,
                'stores_found': len(stores),
                'stores_with_stock': len(stores_with_stock),
                'changes_detected': changes_detected,
                'request_time': request_time
            }

        except Exception as e:
            self.logger.error(f"Error processing SKU {sku}, ZIP {zip_code}: {e}")
            self.stats['failed_requests'] += 1
            return {'success': False, 'error': str(e)}

    def _get_cached_stock_data(self, sku: str, zip_code: str):
        """Get stock data with caching to reduce API calls"""
        cache_key = f"{sku}_{zip_code}"
        current_time = time.time()

        # Check if we have cached data that's still valid (only if not forcing fresh data)
        if not self.force_fresh_data and cache_key in self.api_cache:
            cached_data, timestamp = self.api_cache[cache_key]
            if current_time - timestamp < self.cache_ttl:
                return cached_data

        # Get fresh data from API with cache-busting
        response_data = self.api.get_stock_data(sku, zip_code, force_fresh=True)

        # Cache the response
        if response_data:
            self.api_cache[cache_key] = (response_data, current_time)

            # Clean old cache entries
            if len(self.api_cache) > 100:
                oldest_keys = sorted(self.api_cache.keys(),
                                     key=lambda k: self.api_cache[k][1])[:50]
                for key in oldest_keys:
                    del self.api_cache[key]

        return response_data

    def _handle_initial_stock_report(self, sku: str, zip_code: str, stores_with_stock: List[Dict]):
        """Handle initial stock reporting logic"""
        try:
            # Track if this is the first time we're seeing this SKU-ZIP combination
            sku_zip_key = f"{sku}_{zip_code}"

            # Check if this SKU has EVER been seen in the database
            is_completely_new_product = not self.db_manager.has_sku_been_seen(sku, 'target')

            # Check if this specific SKU-ZIP combination has stores in database
            existing_stores_for_this_zip = self.db_manager.get_stores_for_sku_zip(sku, zip_code, 'target')
            is_new_sku_zip_combination = len(existing_stores_for_this_zip) == 0

            # Check if initial stock report should be sent
            should_send_initial = False

            # Check products.json flag first
            product_allows_initial = self.discord_handler.should_send_initial_report(sku)

            # Check database flag
            initial_already_sent = self.db_manager.has_initial_report_been_sent(sku, zip_code, 'target')

            if not product_allows_initial:
                self.logger.debug(f"Initial reports disabled in products.json for SKU {sku}")
            elif initial_already_sent:
                self.logger.debug(f"Initial report already sent for SKU {sku}, ZIP {zip_code}")
            elif is_completely_new_product and stores_with_stock:
                should_send_initial = True
                self.logger.info(f"New product detected - will send initial stock report for SKU {sku}")
            elif is_new_sku_zip_combination and stores_with_stock:
                should_send_initial = True
                self.logger.info(f"New ZIP code for SKU - will send initial stock report for SKU {sku}, ZIP {zip_code}")

            if (should_send_initial and self.config.send_initial_stock_report and not self.shutdown_requested):
                self.logger.info(
                    f"INITIAL STOCK REPORT: SKU {sku}, ZIP {zip_code} - {len(stores_with_stock)} stores with stock")
                self.discord_handler.send_initial_stock_report(sku, zip_code, stores_with_stock)

                # Mark as sent in database to prevent future sends
                self.db_manager.mark_initial_report_sent(sku, zip_code, 'target')
                self.initial_stock_sent.add(sku_zip_key)
            elif existing_stores_for_this_zip and stores_with_stock and not initial_already_sent:
                # First time running but data exists - mark as sent to prevent future sends
                self.db_manager.mark_initial_report_sent(sku, zip_code, 'target')
                self.logger.debug(f"Marked existing SKU {sku}, ZIP {zip_code} as having initial report sent")

        except Exception as e:
            self.logger.error(f"Error in _handle_initial_stock_report for SKU {sku}, ZIP {zip_code}: {e}")

    def _wait_for_cycle_completion(self, timeout: float = 300.0):
        """Wait for current cycle requests to complete"""
        try:
            completed = self.request_queue.force_process_all(timeout=timeout)
            if not completed:
                self.logger.warning(f"Cycle {self.cycle_count} did not complete within {timeout}s")
        except Exception as e:
            self.logger.error(f"Error waiting for cycle completion: {e}")

    def _smart_sleep(self, duration: float):
        """Sleep with responsive shutdown checking"""
        if duration <= 0:
            return

        sleep_chunks = max(1, int(duration))
        remainder = duration % 1

        # Sleep in 1-second chunks for responsive shutdown
        for i in range(sleep_chunks):
            if self.shutdown_requested or not self.running:
                break
            if i % 10 == 0 and i > 0:  # Log every 10 seconds
                self.logger.debug(f"Sleeping... {sleep_chunks - i}s remaining")
            time.sleep(1)

        # Sleep remainder if still running
        if not self.shutdown_requested and self.running and remainder > 0:
            time.sleep(remainder)

    def _check_config_updates(self):
        """Check for configuration file updates"""
        try:
            if not os.path.exists('config.json'):
                return

            current_modified = os.path.getmtime('config.json')
            if current_modified != self.config_last_modified:
                self.logger.info("Configuration file updated, reloading...")

                old_sku_count = len(self.config.skus)
                self.config = self.config_loader.load_config()
                self.config_last_modified = current_modified

                new_sku_count = len(self.config.skus)
                if new_sku_count != old_sku_count:
                    self.logger.info(f"SKU count changed: {old_sku_count} -> {new_sku_count}")
                    self._log_monitoring_plan()

                    # Initialize timeout tracking for new SKUs
                    new_skus = set(self.config.skus) - set(self.initial_stock_sent)
                    for sku in new_skus:
                        self._initialize_new_product_timeout(sku)

        except Exception as e:
            self.logger.error(f"Error checking config updates: {e}")

    def _initialize_new_product_timeout(self, sku: str):
        """Initialize timeout tracking for a new product"""
        product_info = self.discord_handler._get_product_info(sku)
        if "Unknown Product" in product_info['name']:
            # Start 1-minute timeout for new products
            self.new_product_timeouts[sku] = {
                'start_time': time.time(),
                'timeout_duration': 60,  # 1 minute
                'scraping_initiated': False
            }
            self.logger.info(f"Initialized 1-minute timeout for new product SKU {sku}")

    def _is_product_in_timeout(self, sku: str) -> bool:
        """Check if a product is still in its timeout period"""
        if sku not in self.new_product_timeouts:
            return False

        timeout_info = self.new_product_timeouts[sku]
        elapsed_time = time.time() - timeout_info['start_time']

        if elapsed_time >= timeout_info['timeout_duration']:
            # Timeout expired, remove from tracking
            del self.new_product_timeouts[sku]
            self.logger.info(f"Timeout expired for SKU {sku}, proceeding with stock alerts")
            return False

        return True

    def _handle_new_product_during_timeout(self, sku: str):
        """Handle scraping for new products during timeout period"""
        if sku in self.new_product_timeouts:
            timeout_info = self.new_product_timeouts[sku]

            # Only initiate scraping once
            if not timeout_info['scraping_initiated']:
                self.logger.info(f"Initiating product info scraping for new SKU {sku}")
                try:
                    # Trigger background scraping for this specific SKU
                    self.product_updater.update_products_from_web([sku], force_update=True)
                    timeout_info['scraping_initiated'] = True
                except Exception as e:
                    self.logger.error(f"Error initiating scraping for SKU {sku}: {e}")

    def _log_cycle_summary(self):
        """Log enhanced monitoring cycle summary"""
        try:
            queue_stats = self.request_queue.get_stats()
            rate_stats = self.rate_limiter.get_stats()

            if self.stats['start_time']:
                uptime = datetime.now() - self.stats['start_time']
            else:
                uptime = timedelta(0)

            success_rate = (
                                   self.stats['successful_requests'] /
                                   max(1, self.stats['successful_requests'] + self.stats['failed_requests'])
                           ) * 100

            self.logger.info(f"Enhanced Cycle {self.cycle_count} Summary:")
            self.logger.info(f"  - Uptime: {uptime}")
            self.logger.info(f"  - Success rate: {success_rate:.1f}%")
            self.logger.info(f"  - Stock changes: {self.stats['stock_changes_detected']}")
            self.logger.info(f"  - Alerts sent: {self.stats['alerts_sent']}")
            self.logger.info(f"  - Requests processed: {queue_stats['total_processed']}")
            self.logger.info(
                f"  - Queue stats: {queue_stats['queue_size']} pending, {queue_stats['active_requests']} active")
            self.logger.info(
                f"  - Rate limit - Today: {rate_stats['requests_today']}, Last minute: {rate_stats['requests_last_minute']}")

        except Exception as e:
            self.logger.error(f"Error logging cycle summary: {e}")

    def start(self, test_mode=False, max_skus=None):
        """Start the enhanced monitor"""
        self.test_mode = test_mode
        self.max_skus = max_skus

        if not self.initialize():
            self.logger.error("Failed to initialize enhanced monitor")
            return False

        self.running = True
        self.shutdown_requested = False

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        mode_text = "TEST MODE" if test_mode else "PRODUCTION MODE"
        print(f"✅ Enhanced Target Monitor is running in {mode_text}...")
        print("📊 Enhanced Target monitoring configuration:")
        print(f"   • SKUs: {len(self.config.skus)}")
        print(f"   • ZIP Codes: {', '.join(self.config.zip_codes)}")
        print(f"   • Check interval: {self.config.monitoring_interval} seconds")
        print(
            f"   • Daily requests: ~{(len(self.config.skus) * len(self.config.zip_codes) * 86400 / self.config.monitoring_interval):.0f}")
        print(f"   • Rate limiting: 20/min, 1200/hour, 25k/day")
        print(f"   • Request batching: {self.request_queue.batch_size} per batch")

        if test_mode:
            print(f"🧪 TEST MODE: Limited to {max_skus or 'all'} SKUs, max 3 cycles")

        print("🔄 Starting enhanced monitoring loop...")
        print("💡 Press Ctrl+C to stop")

        # Start Discord bot if token provided
        if self.config.discord_bot_token and self.config.discord_bot_token.strip():
            print("🤖 Starting Discord bot...")
            self.loop = asyncio.new_event_loop()

            def run_bot():
                asyncio.set_event_loop(self.loop)
                self.loop.run_until_complete(
                    run_discord_bot(self.config.discord_bot_token, self.config.discord_webhook_url, self.db_manager)
                )

            bot_thread = threading.Thread(target=run_bot, daemon=True)
            bot_thread.start()
            print("✅ Discord bot started")

        try:
            self.monitor_loop()
        except KeyboardInterrupt:
            print("\n🛑 Keyboard interrupt received, shutting down...")
            self.shutdown_requested = True
        finally:
            self.cleanup()

        return True

    def stop(self):
        """Stop the enhanced monitor"""
        self.logger.info("Stopping enhanced Target monitor...")
        self.running = False
        self.shutdown_requested = True

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        signal_names = {2: 'SIGINT (Ctrl+C)', 15: 'SIGTERM'}
        signal_name = signal_names.get(signum, f'Signal {signum}')

        if self.shutdown_requested:
            return

        print(f"\n🛑 Received {signal_name}, shutting down gracefully...")
        self.shutdown_requested = True
        self.running = False

    def cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up enhanced Target monitor resources...")

        try:
            # Stop request queue
            if self.request_queue:
                self.request_queue.stop()

            # Stop background scraping
            if self.product_updater:
                try:
                    if hasattr(self.product_updater, 'stop_scraping'):
                        self.product_updater.stop_scraping()
                    else:
                        # For EnhancedProductInfoUpdater, manually close scrapers
                        if hasattr(self.product_updater, 'scraper') and self.product_updater.scraper:
                            self.product_updater.scraper.close()
                        if hasattr(self.product_updater, 'fallback_scraper') and self.product_updater.fallback_scraper:
                            self.product_updater.fallback_scraper.close()
                except Exception as e:
                    self.logger.error(f"Error stopping product updater: {e}")

            # Stop Discord bot
            if self.loop:
                stop_discord_bot()
                if not self.loop.is_closed():
                    self.loop.call_soon_threadsafe(self.loop.stop)

            # Close API connection
            if self.api:
                self.api.close()

            # Close command handler
            if self.command_handler:
                self.command_handler.close()

            # Close database
            if self.db_manager:
                self.db_manager.close()

            # Final statistics
            self._log_final_stats()

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

        self.logger.info("Enhanced Target monitor stopped")

    def _log_final_stats(self):
        """Log final enhanced monitoring statistics"""
        try:
            if self.stats['start_time']:
                uptime = datetime.now() - self.stats['start_time']
                total_requests = self.stats['successful_requests'] + self.stats['failed_requests']
                success_rate = (self.stats['successful_requests'] / max(1, total_requests)) * 100

                self.logger.info("Final Enhanced Statistics:")
                self.logger.info(f"  - Total uptime: {uptime}")
                self.logger.info(f"  - Cycles completed: {self.stats['total_cycles']}")
                self.logger.info(f"  - Total requests: {total_requests}")
                self.logger.info(f"  - Success rate: {success_rate:.1f}%")
                self.logger.info(f"  - Stock changes detected: {self.stats['stock_changes_detected']}")
                self.logger.info(f"  - Alerts sent: {self.stats['alerts_sent']}")
                self.logger.info(f"  - Error count: {self.stats['error_count']}")

                if self.rate_limiter:
                    rate_stats = self.rate_limiter.get_stats()
                    self.logger.info(f"  - Rate limiting - Final usage: {rate_stats['requests_today']} requests today")

        except Exception as e:
            self.logger.error(f"Error logging final stats: {e}")

    def run_command(self, sku: str, zip_code: str):
        """Run a one-time command check with enhanced rate limiting"""
        if not self.initialize():
            print("Failed to initialize enhanced Target monitor")
            return

        try:
            print(f"Running enhanced Target command check for SKU {sku}, ZIP {zip_code}")

            # Process single request with rate limiting
            result = self._process_sku_zip_request(sku, zip_code)

            if result['success']:
                print(f"✅ Success: Found {result['stores_found']} stores")
                print(f"📦 Stores with stock: {result['stores_with_stock']}")
                print(f"🔄 Changes detected: {result['changes_detected']}")
                print(f"⏱️ Request time: {result['request_time']:.2f}s")

                # Send Discord notification
                try:
                    command_result = self.command_handler.target_command(sku, zip_code, send_webhook=True)
                    print(f"📢 Discord notification: {command_result['message']}")
                except Exception as e:
                    print(f"❌ Discord notification failed: {e}")
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")

        except Exception as e:
            print(f"❌ Command error: {e}")
            self.logger.error(f"Enhanced command error: {e}")
        finally:
            self.cleanup()

    def get_health_status(self) -> Dict:
        """Get comprehensive health status"""
        try:
            health = {
                'status': 'HEALTHY',
                'uptime': None,
                'cycles_completed': self.stats['total_cycles'],
                'success_rate': 0,
                'error_count': self.stats['error_count'],
                'last_error': self.stats.get('last_error'),
                'rate_limiting': {},
                'queue_status': {},
                'database_connected': bool(self.db_manager and self.db_manager.connection),
                'api_available': bool(self.api)
            }

            # Calculate uptime
            if self.stats['start_time']:
                uptime = datetime.now() - self.stats['start_time']
                health['uptime'] = str(uptime)

            # Calculate success rate
            total_requests = self.stats['successful_requests'] + self.stats['failed_requests']
            if total_requests > 0:
                health['success_rate'] = (self.stats['successful_requests'] / total_requests) * 100

            # Get rate limiting status
            if self.rate_limiter:
                health['rate_limiting'] = self.rate_limiter.get_stats()

            # Get queue status
            if self.request_queue:
                health['queue_status'] = self.request_queue.get_stats()

            # Determine overall health status
            if health['error_count'] > 10:
                health['status'] = 'DEGRADED'
            elif health['success_rate'] < 80:
                health['status'] = 'CRITICAL'
            elif not health['database_connected']:
                health['status'] = 'CRITICAL'

            return health

        except Exception as e:
            self.logger.error(f"Error getting health status: {e}")
            return {'status': 'ERROR', 'error': str(e)}


def main():
    """Enhanced main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Enhanced Target Stock Monitor')
    parser.add_argument('--command', action='store_true', help='Run in command mode')
    parser.add_argument('--sku', type=str, help='SKU to check (command mode only)')
    parser.add_argument('--zip', type=str, help='ZIP code to check (command mode only)')
    parser.add_argument('--test-mode', action='store_true', help='Run in test mode (limited cycles)')
    parser.add_argument('--max-skus', type=int, help='Maximum SKUs to test (test mode only)')
    parser.add_argument('--health', action='store_true', help='Check health status and exit')

    args = parser.parse_args()

    monitor = EnhancedTargetMonitor()

    if args.health:
        # Quick health check
        if monitor.initialize():
            health = monitor.get_health_status()
            print(f"Health Status: {health['status']}")
            print(f"Database Connected: {health['database_connected']}")
            print(f"API Available: {health['api_available']}")
            monitor.cleanup()
        else:
            print("Health Status: CRITICAL - Initialization failed")
        sys.exit(0)

    if args.command:
        if not args.sku or not args.zip:
            print("Command mode requires --sku and --zip arguments")
            sys.exit(1)

        monitor.run_command(args.sku, args.zip)
    else:
        success = monitor.start(test_mode=args.test_mode, max_skus=args.max_skus)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()