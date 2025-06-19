import time
import threading
import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
from collections import deque
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    max_requests_per_minute: int = 25  # Conservative limit
    max_requests_per_hour: int = 1200  # 20 per minute average
    max_requests_per_day: int = 25000  # Well under 50k limit
    burst_allowance: int = 5  # Allow small bursts
    cooldown_period: int = 60  # Seconds to wait after hitting limit


class TokenBucket:
    """Token bucket algorithm for rate limiting"""

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if successful."""
        with self.lock:
            now = time.time()
            # Add tokens based on time elapsed
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last_refill = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_time(self, tokens: int = 1) -> float:
        """Calculate wait time needed for tokens"""
        with self.lock:
            if self.tokens >= tokens:
                return 0
            needed_tokens = tokens - self.tokens
            return needed_tokens / self.refill_rate


class RequestTracker:
    """Track requests over different time windows"""

    def __init__(self):
        self.requests = deque()  # (timestamp, success)
        self.lock = threading.Lock()

    def add_request(self, success: bool = True):
        """Record a request"""
        with self.lock:
            now = datetime.now()
            self.requests.append((now, success))
            # Clean old entries (keep 24 hours)
            cutoff = now - timedelta(hours=24)
            while self.requests and self.requests[0][0] < cutoff:
                self.requests.popleft()

    def count_requests(self, window_minutes: int) -> int:
        """Count requests in the last N minutes"""
        with self.lock:
            cutoff = datetime.now() - timedelta(minutes=window_minutes)
            return sum(1 for timestamp, _ in self.requests if timestamp >= cutoff)

    def get_success_rate(self, window_minutes: int = 60) -> float:
        """Get success rate in the last N minutes"""
        with self.lock:
            cutoff = datetime.now() - timedelta(minutes=window_minutes)
            recent_requests = [(t, s) for t, s in self.requests if t >= cutoff]
            if not recent_requests:
                return 1.0
            successful = sum(1 for _, success in recent_requests if success)
            return successful / len(recent_requests)


class EnhancedRateLimiter:
    """Enhanced rate limiter with multiple time windows and health monitoring"""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.tracker = RequestTracker()

        # Token buckets for different time windows
        self.minute_bucket = TokenBucket(
            capacity=config.max_requests_per_minute + config.burst_allowance,
            refill_rate=config.max_requests_per_minute / 60.0
        )

        self.hour_bucket = TokenBucket(
            capacity=config.max_requests_per_hour,
            refill_rate=config.max_requests_per_hour / 3600.0
        )

        self.day_bucket = TokenBucket(
            capacity=config.max_requests_per_day,
            refill_rate=config.max_requests_per_day / 86400.0
        )

        self.circuit_breaker = CircuitBreaker()
        self.lock = threading.Lock()
        self.last_violation = None

    def can_make_request(self) -> tuple[bool, float]:
        """Check if request can be made. Returns (allowed, wait_time)"""
        if self.circuit_breaker.is_open():
            return False, self.circuit_breaker.wait_time()

        # Check all time windows
        buckets = [
            ("minute", self.minute_bucket),
            ("hour", self.hour_bucket),
            ("day", self.day_bucket)
        ]

        max_wait = 0
        can_proceed = True

        for name, bucket in buckets:
            if not bucket.consume(1):
                can_proceed = False
                wait_time = bucket.wait_time(1)
                max_wait = max(max_wait, wait_time)
                logger.warning(f"Rate limit hit for {name} window, need to wait {wait_time:.2f}s")

        if not can_proceed:
            # Return tokens since we're not proceeding
            for _, bucket in buckets:
                if bucket.tokens < bucket.capacity:
                    bucket.tokens += 1

        return can_proceed, max_wait

    def record_request(self, success: bool):
        """Record the result of a request"""
        self.tracker.add_request(success)

        if not success:
            self.circuit_breaker.record_failure()
        else:
            self.circuit_breaker.record_success()

    def get_stats(self) -> Dict:
        """Get current rate limiting statistics"""
        return {
            "requests_last_minute": self.tracker.count_requests(1),
            "requests_last_hour": self.tracker.count_requests(60),
            "requests_today": self.tracker.count_requests(1440),
            "success_rate_hour": self.tracker.get_success_rate(60),
            "circuit_breaker_state": self.circuit_breaker.state,
            "tokens_available": {
                "minute": int(self.minute_bucket.tokens),
                "hour": int(self.hour_bucket.tokens),
                "day": int(self.day_bucket.tokens)
            }
        }

    def force_cooldown(self, duration: int = None):
        """Force a cooldown period"""
        duration = duration or self.config.cooldown_period
        logger.warning(f"Forcing {duration}s cooldown due to rate limit violation")
        self.last_violation = time.time()
        time.sleep(duration)


class CircuitBreaker:
    """Circuit breaker to prevent cascade failures"""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.lock = threading.Lock()

    def record_failure(self):
        """Record a failed request"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker OPENED after {self.failure_count} failures")

    def record_success(self):
        """Record a successful request"""
        with self.lock:
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info("Circuit breaker CLOSED after successful request")
            elif self.state == "CLOSED":
                # Gradually reduce failure count on success
                self.failure_count = max(0, self.failure_count - 1)

    def is_open(self) -> bool:
        """Check if circuit breaker is open"""
        with self.lock:
            if self.state == "CLOSED":
                return False

            if self.state == "OPEN":
                # Check if we should try recovery
                if (self.last_failure_time and
                        time.time() - self.last_failure_time >= self.recovery_timeout):
                    self.state = "HALF_OPEN"
                    logger.info("Circuit breaker moved to HALF_OPEN for recovery test")
                    return False
                return True

            # HALF_OPEN state
            return False

    def wait_time(self) -> float:
        """Get wait time until circuit breaker can attempt recovery"""
        if self.last_failure_time:
            elapsed = time.time() - self.last_failure_time
            remaining = max(0, self.recovery_timeout - elapsed)
            return remaining
        return 0