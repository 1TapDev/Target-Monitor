"""
Circuit breaker pattern implementation for preventing cascade failures
"""

import time
import threading
import logging
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN" 
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Circuit breaker to prevent cascade failures"""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300,
                 success_threshold: int = 2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        """Call function through circuit breaker"""
        if not self.can_execute():
            raise Exception(f"Circuit breaker is {self.state.value}")

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise e

    def can_execute(self) -> bool:
        """Check if execution is allowed"""
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                # Check if we should attempt recovery
                if (self.last_failure_time and 
                    time.time() - self.last_failure_time >= self.recovery_timeout):
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info("Circuit breaker moved to HALF_OPEN for recovery")
                    return True
                return False
            else:  # HALF_OPEN
                return True

    def record_success(self):
        """Record successful execution"""
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker CLOSED after successful recovery")
            elif self.state == CircuitState.CLOSED:
                # Gradually reduce failure count on success
                self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self):
        """Record failed execution"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state in [CircuitState.CLOSED, CircuitState.HALF_OPEN]:
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.error(f"Circuit breaker OPENED after {self.failure_count} failures")

    def get_state(self) -> CircuitState:
        """Get current circuit breaker state"""
        return self.state

    def reset(self):
        """Reset circuit breaker to closed state"""
        with self.lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset to CLOSED")
