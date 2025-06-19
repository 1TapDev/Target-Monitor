import random
import logging
import time
import threading
from typing import List, Optional, Dict, Set
import requests

logger = logging.getLogger(__name__)


class ProxyManager:
    def __init__(self, proxy_file: str = "proxies.txt"):
        self.proxy_file = proxy_file
        self.proxies = []
        self.failed_proxies = set()  # Store failed proxy URLs
        self.banned_proxies = set()  # Store permanently banned proxy URLs
        self.last_proxy_index = 0
        self.proxy_test_timeout = 10
        self.lock = threading.Lock()

        # Enhanced health monitoring
        self.proxy_health = {}  # Track health metrics per proxy
        self.health_check_interval = 300  # 5 minutes
        self.last_health_check = {}
        self.consecutive_failures_threshold = 3
        self.ban_duration = 3600  # 1 hour temporary ban

        # Performance tracking
        self.proxy_performance = {}  # Track response times and success rates
        self.performance_window = 100  # Track last 100 requests per proxy

        self.load_proxies()

    def load_proxies(self):
        """Load proxies from proxies.txt file with enhanced validation"""
        try:
            with open(self.proxy_file, 'r') as f:
                lines = f.readlines()

            self.proxies = []
            loaded_count = 0

            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if line and not line.startswith('#'):
                    proxy_data = self.parse_proxy_line(line)
                    if proxy_data:
                        self.proxies.append(proxy_data)
                        loaded_count += 1

                        # Initialize health tracking
                        proxy_url = proxy_data['url']
                        self.proxy_health[proxy_url] = {
                            'consecutive_failures': 0,
                            'total_requests': 0,
                            'successful_requests': 0,
                            'last_success': None,
                            'last_failure': None,
                            'banned_until': None
                        }

                        self.proxy_performance[proxy_url] = {
                            'response_times': [],
                            'recent_results': []  # True for success, False for failure
                        }
                    else:
                        logger.warning(f"Invalid proxy format on line {line_num}: {line}")

            logger.info(f"Loaded {loaded_count} proxies from {self.proxy_file}")

            if not self.proxies:
                logger.warning(f"No valid proxies found in {self.proxy_file}")

        except FileNotFoundError:
            logger.warning(f"Proxy file {self.proxy_file} not found. Running without proxies.")
            self.proxies = []
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            self.proxies = []

    def parse_proxy_line(self, line: str) -> Optional[Dict]:
        """Parse proxy line in format ip:port:user:pass"""
        try:
            parts = line.split(':')
            if len(parts) == 4:
                ip, port, username, password = parts
                return {
                    'ip': ip.strip(),
                    'port': int(port.strip()),
                    'username': username.strip(),
                    'password': password.strip(),
                    'url': f"http://{username.strip()}:{password.strip()}@{ip.strip()}:{port.strip()}"
                }
            elif len(parts) == 2:
                # Support for ip:port format without auth
                ip, port = parts
                return {
                    'ip': ip.strip(),
                    'port': int(port.strip()),
                    'username': None,
                    'password': None,
                    'url': f"http://{ip.strip()}:{port.strip()}"
                }
            else:
                return None
        except Exception as e:
            logger.warning(f"Error parsing proxy line '{line}': {e}")
            return None

    def _is_proxy_temporarily_banned(self, proxy_url: str) -> bool:
        """Check if proxy is temporarily banned"""
        health = self.proxy_health.get(proxy_url, {})
        banned_until = health.get('banned_until')

        if banned_until and time.time() < banned_until:
            return True
        elif banned_until:
            # Ban expired, clear it
            health['banned_until'] = None
            health['consecutive_failures'] = 0
            logger.info(f"Temporary ban expired for proxy: {proxy_url}")

        return False

    def _get_healthy_proxies(self) -> List[Dict]:
        """Get list of healthy proxies that are not failed or banned"""
        with self.lock:
            healthy_proxies = []
            current_time = time.time()

            for proxy in self.proxies:
                proxy_url = proxy['url']

                # Skip permanently failed proxies
                if proxy_url in self.failed_proxies:
                    continue

                # Skip permanently banned proxies
                if proxy_url in self.banned_proxies:
                    continue

                # Skip temporarily banned proxies
                if self._is_proxy_temporarily_banned(proxy_url):
                    continue

                healthy_proxies.append(proxy)

            return healthy_proxies

    def get_random_proxy(self) -> Optional[Dict]:
        """Get a random healthy proxy with performance-based selection"""
        healthy_proxies = self._get_healthy_proxies()

        if not healthy_proxies:
            # Try to reset some failed proxies if all are failed
            self._attempt_proxy_recovery()
            healthy_proxies = self._get_healthy_proxies()

        if not healthy_proxies:
            logger.warning("No healthy proxies available")
            return None

        # Use performance-based selection - prefer proxies with better success rates
        if len(healthy_proxies) > 1:
            weighted_proxies = []

            for proxy in healthy_proxies:
                proxy_url = proxy['url']
                perf = self.proxy_performance.get(proxy_url, {})
                recent_results = perf.get('recent_results', [])

                if recent_results:
                    success_rate = sum(recent_results) / len(recent_results)
                    weight = max(1, int(success_rate * 10))  # Weight based on success rate
                else:
                    weight = 5  # Default weight for untested proxies

                weighted_proxies.extend([proxy] * weight)

            if weighted_proxies:
                return random.choice(weighted_proxies)

        return random.choice(healthy_proxies)

    def get_next_proxy(self) -> Optional[Dict]:
        """Get next proxy in rotation"""
        healthy_proxies = self._get_healthy_proxies()

        if not healthy_proxies:
            return None

        if healthy_proxies:
            proxy = healthy_proxies[self.last_proxy_index % len(healthy_proxies)]
            self.last_proxy_index += 1
            return proxy

        return None

    def record_proxy_result(self, proxy: Dict, success: bool, response_time: float = None):
        """
        Record the result of using a proxy for enhanced health tracking.

        Args:
            proxy: Proxy configuration dict
            success: Whether the request was successful
            response_time: Time taken for the request in seconds
        """
        if not proxy:
            return

        proxy_url = proxy['url']
        current_time = time.time()

        with self.lock:
            # Update health tracking
            health = self.proxy_health.get(proxy_url, {})
            health['total_requests'] = health.get('total_requests', 0) + 1

            if success:
                health['successful_requests'] = health.get('successful_requests', 0) + 1
                health['last_success'] = current_time
                health['consecutive_failures'] = 0

                # Remove from failed set if it was there
                self.failed_proxies.discard(proxy_url)

            else:
                health['last_failure'] = current_time
                health['consecutive_failures'] = health.get('consecutive_failures', 0) + 1

                # Check if proxy should be temporarily banned
                if health['consecutive_failures'] >= self.consecutive_failures_threshold:
                    health['banned_until'] = current_time + self.ban_duration
                    logger.warning(f"Temporarily banned proxy for {self.ban_duration}s: {proxy['ip']}:{proxy['port']}")

            # Update performance tracking
            perf = self.proxy_performance.get(proxy_url, {})

            # Track response time
            if response_time is not None:
                response_times = perf.get('response_times', [])
                response_times.append(response_time)

                # Keep only last 50 response times
                if len(response_times) > 50:
                    response_times = response_times[-50:]
                perf['response_times'] = response_times

            # Track recent results
            recent_results = perf.get('recent_results', [])
            recent_results.append(success)

            # Keep only last 100 results
            if len(recent_results) > self.performance_window:
                recent_results = recent_results[-self.performance_window:]
            perf['recent_results'] = recent_results

    def mark_proxy_failed(self, proxy: Dict):
        """Mark a proxy as failed (soft failure, can be recovered)"""
        if proxy:
            proxy_url = proxy['url']
            self.failed_proxies.add(proxy_url)
            self.record_proxy_result(proxy, False)
            logger.warning(f"Marked proxy as failed: {proxy['ip']}:{proxy['port']}")

    def mark_proxy_banned(self, proxy: Dict):
        """Mark a proxy as permanently banned"""
        if proxy:
            proxy_url = proxy['url']
            self.banned_proxies.add(proxy_url)
            self.failed_proxies.add(proxy_url)
            logger.error(f"Marked proxy as PERMANENTLY BANNED: {proxy['ip']}:{proxy['port']}")

    def _attempt_proxy_recovery(self):
        """Attempt to recover some failed proxies if all proxies are failed"""
        with self.lock:
            if len(self.failed_proxies) >= len(self.proxies) * 0.8:  # If 80% or more are failed
                # Find failed proxies that haven't failed recently
                current_time = time.time()
                recovery_candidates = []

                for proxy in self.proxies:
                    proxy_url = proxy['url']
                    if proxy_url in self.failed_proxies:
                        health = self.proxy_health.get(proxy_url, {})
                        last_failure = health.get('last_failure', 0)

                        # Consider recovery if failed more than 10 minutes ago
                        if current_time - last_failure > 600:
                            recovery_candidates.append(proxy_url)

                # Recover up to 25% of failed proxies
                recovery_count = min(len(recovery_candidates), len(self.proxies) // 4)
                if recovery_count > 0:
                    recovered = random.sample(recovery_candidates, recovery_count)
                    for proxy_url in recovered:
                        self.failed_proxies.discard(proxy_url)
                        # Reset health stats
                        health = self.proxy_health.get(proxy_url, {})
                        health['consecutive_failures'] = 0
                        health['banned_until'] = None

                    logger.info(f"Recovered {len(recovered)} failed proxies for retry")

    def test_proxy(self, proxy: Dict) -> bool:
        """Test if a proxy is working with basic connectivity"""
        try:
            proxies = {
                'http': proxy['url'],
                'https': proxy['url']
            }

            start_time = time.time()

            # Test with a simple request
            response = requests.get(
                'http://httpbin.org/ip',
                proxies=proxies,
                timeout=self.proxy_test_timeout
            )

            response_time = time.time() - start_time

            if response.status_code == 200:
                self.record_proxy_result(proxy, True, response_time)
                logger.debug(f"Proxy test successful: {proxy['ip']}:{proxy['port']} ({response_time:.2f}s)")
                return True
            else:
                self.record_proxy_result(proxy, False, response_time)
                logger.warning(f"Proxy test failed: {proxy['ip']}:{proxy['port']} - Status: {response.status_code}")
                return False

        except Exception as e:
            self.record_proxy_result(proxy, False)
            logger.warning(f"Proxy test failed: {proxy['ip']}:{proxy['port']} - Error: {e}")
            return False

    def get_proxy_dict(self, proxy: Dict) -> Optional[Dict]:
        """Convert proxy to requests-compatible format"""
        if not proxy:
            return None

        return {
            'http': proxy['url'],
            'https': proxy['url']
        }

    def get_working_proxy_count(self) -> int:
        """Get count of working proxies"""
        return len(self._get_healthy_proxies())

    def get_proxy_stats(self) -> Dict:
        """Get comprehensive proxy statistics"""
        with self.lock:
            total_proxies = len(self.proxies)
            failed_proxies = len(self.failed_proxies)
            banned_proxies = len(self.banned_proxies)
            healthy_proxies = len(self._get_healthy_proxies())

            # Calculate temporarily banned proxies
            temp_banned = 0
            for proxy_url, health in self.proxy_health.items():
                if self._is_proxy_temporarily_banned(proxy_url):
                    temp_banned += 1

            # Calculate average performance metrics
            total_requests = sum(h.get('total_requests', 0) for h in self.proxy_health.values())
            total_successful = sum(h.get('successful_requests', 0) for h in self.proxy_health.values())
            overall_success_rate = (total_successful / max(1, total_requests)) * 100

            # Calculate average response time
            all_response_times = []
            for perf in self.proxy_performance.values():
                all_response_times.extend(perf.get('response_times', []))

            avg_response_time = sum(all_response_times) / len(all_response_times) if all_response_times else 0

            return {
                'total_proxies': total_proxies,
                'healthy_proxies': healthy_proxies,
                'failed_proxies': failed_proxies,
                'banned_proxies': banned_proxies,
                'temporarily_banned': temp_banned,
                'health_percentage': (healthy_proxies / max(1, total_proxies)) * 100,
                'total_requests': total_requests,
                'overall_success_rate': overall_success_rate,
                'average_response_time': avg_response_time,
                'performance_tracked_proxies': len(self.proxy_performance)
            }

    def get_top_performing_proxies(self, limit: int = 5) -> List[Dict]:
        """Get top performing proxies based on success rate and response time"""
        healthy_proxies = self._get_healthy_proxies()

        if not healthy_proxies:
            return []

        # Score proxies based on success rate and response time
        proxy_scores = []

        for proxy in healthy_proxies:
            proxy_url = proxy['url']
            perf = self.proxy_performance.get(proxy_url, {})

            recent_results = perf.get('recent_results', [])
            response_times = perf.get('response_times', [])

            if recent_results:
                success_rate = sum(recent_results) / len(recent_results)
                avg_response_time = sum(response_times) / len(response_times) if response_times else 999

                # Score: higher success rate is better, lower response time is better
                score = success_rate * 100 - min(avg_response_time, 10)  # Cap response time impact

                proxy_scores.append((proxy, score, success_rate, avg_response_time))

        # Sort by score (descending)
        proxy_scores.sort(key=lambda x: x[1], reverse=True)

        return [
            {
                'proxy': proxy,
                'score': score,
                'success_rate': success_rate,
                'avg_response_time': avg_response_time
            }
            for proxy, score, success_rate, avg_response_time in proxy_scores[:limit]
        ]

    def cleanup_old_performance_data(self):
        """Clean up old performance data to prevent memory bloat"""
        with self.lock:
            current_time = time.time()
            cleaned_count = 0

            for proxy_url in list(self.proxy_health.keys()):
                health = self.proxy_health[proxy_url]
                last_activity = max(
                    health.get('last_success', 0),
                    health.get('last_failure', 0)
                )

                # Remove data for proxies not used in last 24 hours
                if current_time - last_activity > 86400:  # 24 hours
                    self.proxy_health.pop(proxy_url, None)
                    self.proxy_performance.pop(proxy_url, None)
                    cleaned_count += 1

            if cleaned_count > 0:
                logger.info(f"Cleaned up performance data for {cleaned_count} inactive proxies")

    def force_new_proxy(self) -> Optional[Dict]:
        """Force get a new proxy, different from the last used one"""
        healthy_proxies = self._get_healthy_proxies()

        if not healthy_proxies:
            return None

        if len(healthy_proxies) > 1:
            # Try to get a different proxy than the current one
            return random.choice(healthy_proxies)

        # Fallback to any healthy proxy
        if healthy_proxies:
            return healthy_proxies[0]

        return None

    def reset_failed_proxies(self):
        """Reset the failed proxies list"""
        with self.lock:
            failed_count = len(self.failed_proxies)
            self.failed_proxies.clear()

            # Reset health stats but keep performance data
            for health in self.proxy_health.values():
                health['consecutive_failures'] = 0
                health['banned_until'] = None

            logger.info(f"Reset {failed_count} failed proxies")

    def reset_banned_proxies(self):
        """Reset permanently banned proxies (use with caution)"""
        with self.lock:
            banned_count = len(self.banned_proxies)
            self.banned_proxies.clear()
            logger.info(f"Reset {banned_count} permanently banned proxies")

    def export_proxy_health_report(self) -> Dict:
        """Export detailed health report for analysis"""
        with self.lock:
            report = {
                'timestamp': time.time(),
                'summary': self.get_proxy_stats(),
                'top_performers': self.get_top_performing_proxies(10),
                'detailed_health': {}
            }

            for proxy in self.proxies:
                proxy_url = proxy['url']
                health = self.proxy_health.get(proxy_url, {})
                perf = self.proxy_performance.get(proxy_url, {})

                recent_results = perf.get('recent_results', [])
                response_times = perf.get('response_times', [])

                report['detailed_health'][f"{proxy['ip']}:{proxy['port']}"] = {
                    'total_requests': health.get('total_requests', 0),
                    'successful_requests': health.get('successful_requests', 0),
                    'consecutive_failures': health.get('consecutive_failures', 0),
                    'is_failed': proxy_url in self.failed_proxies,
                    'is_banned': proxy_url in self.banned_proxies,
                    'is_temp_banned': self._is_proxy_temporarily_banned(proxy_url),
                    'recent_success_rate': (sum(recent_results) / len(recent_results)) if recent_results else 0,
                    'avg_response_time': (sum(response_times) / len(response_times)) if response_times else 0,
                    'last_success': health.get('last_success'),
                    'last_failure': health.get('last_failure')
                }

            return report