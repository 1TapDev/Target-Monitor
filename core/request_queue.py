import asyncio
import time
import threading
from queue import PriorityQueue, Queue, Empty
from typing import Optional, Dict, List, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
import random

logger = logging.getLogger(__name__)


class RequestPriority(Enum):
    """Request priority levels"""
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass
class QueuedRequest:
    """A request in the queue"""
    priority: RequestPriority
    sku: str
    zip_code: str
    callback: Callable
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    scheduled_at: float = field(default_factory=time.time)

    def __lt__(self, other):
        """For priority queue ordering"""
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        return self.scheduled_at < other.scheduled_at


class RequestBatcher:
    """Batch requests for efficient processing"""

    def __init__(self, batch_size: int = 3, batch_timeout: float = 10.0):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_requests = []
        self.last_batch_time = time.time()
        self.lock = threading.Lock()

    def add_request(self, request: QueuedRequest) -> Optional[List[QueuedRequest]]:
        """Add request to batch. Returns batch if ready."""
        with self.lock:
            self.pending_requests.append(request)

            # Check if batch is ready
            if (len(self.pending_requests) >= self.batch_size or
                    time.time() - self.last_batch_time >= self.batch_timeout):
                batch = self.pending_requests[:self.batch_size]
                self.pending_requests = self.pending_requests[self.batch_size:]
                self.last_batch_time = time.time()

                logger.debug(f"Created batch of {len(batch)} requests")
                return batch

        return None

    def flush_pending(self) -> List[QueuedRequest]:
        """Get all pending requests"""
        with self.lock:
            if self.pending_requests:
                batch = self.pending_requests.copy()
                self.pending_requests.clear()
                self.last_batch_time = time.time()
                logger.debug(f"Flushed {len(batch)} pending requests")
                return batch
        return []


class SmartRequestQueue:
    """Intelligent request queue with batching, prioritization, and retry logic"""

    def __init__(self, rate_limiter, batch_size: int = 3, max_concurrent: int = 2):
        self.rate_limiter = rate_limiter
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent

        self.request_queue = PriorityQueue()
        self.batcher = RequestBatcher(batch_size=batch_size)
        self.retry_queue = Queue()

        self.active_requests = 0
        self.total_processed = 0
        self.failed_requests = 0

        self.running = False
        self.worker_threads = []
        self.stats_lock = threading.Lock()

        # Adaptive timing
        self.base_delay = 2.0  # Base delay between batches
        self.adaptive_delay = self.base_delay
        self.last_success_rate = 1.0

    def start(self):
        """Start the queue processor"""
        self.running = True

        # Start worker threads
        for i in range(self.max_concurrent):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"RequestWorker-{i}",
                daemon=True
            )
            worker.start()
            self.worker_threads.append(worker)

        # Start retry processor
        retry_processor = threading.Thread(
            target=self._retry_processor,
            name="RetryProcessor",
            daemon=True
        )
        retry_processor.start()
        self.worker_threads.append(retry_processor)

        logger.info(f"Started request queue with {self.max_concurrent} workers")

    def stop(self):
        """Stop the queue processor"""
        self.running = False

        # Add sentinel values to wake up workers
        for _ in range(self.max_concurrent):
            self.request_queue.put((0, None))

        # Wait for workers to finish
        for worker in self.worker_threads:
            worker.join(timeout=5.0)

        logger.info("Request queue stopped")

    def add_request(self, sku: str, zip_code: str, callback: Callable,
                    priority: RequestPriority = RequestPriority.NORMAL,
                    delay: float = 0) -> str:
        """Add a request to the queue"""

        scheduled_at = time.time() + delay
        request = QueuedRequest(
            priority=priority,
            sku=sku,
            zip_code=zip_code,
            callback=callback,
            scheduled_at=scheduled_at
        )

        # Use priority value and scheduled time for queue ordering
        self.request_queue.put((request.priority.value, request))

        logger.debug(f"Queued request for SKU {sku}, ZIP {zip_code} "
                     f"(priority: {priority.name}, delay: {delay}s)")

        return f"{sku}_{zip_code}_{request.created_at}"

    def add_batch_requests(self, requests: List[Tuple[str, str]],
                           callback: Callable,
                           priority: RequestPriority = RequestPriority.NORMAL,
                           spread_delay: bool = True) -> List[str]:
        """Add multiple requests with optional delay spreading"""

        request_ids = []

        for i, (sku, zip_code) in enumerate(requests):
            # Spread requests over time to avoid bursts
            delay = 0
            if spread_delay and len(requests) > 1:
                delay = i * (self.adaptive_delay / len(requests))

            request_id = self.add_request(sku, zip_code, callback, priority, delay)
            request_ids.append(request_id)

        logger.info(f"Queued batch of {len(requests)} requests with spread delay")
        return request_ids

    def _worker_loop(self):
        """Main worker loop"""
        thread_name = threading.current_thread().name
        logger.debug(f"{thread_name} started")

        while self.running:
            try:
                # Get request from queue with timeout
                try:
                    priority, request = self.request_queue.get(timeout=1.0)
                except Empty:
                    continue

                # Check for sentinel value
                if request is None:
                    break

                # Wait if request is scheduled for future
                wait_time = request.scheduled_at - time.time()
                if wait_time > 0:
                    time.sleep(min(wait_time, 1.0))

                # Process the request
                self._process_request(request)

            except Exception as e:
                logger.error(f"Error in worker {thread_name}: {e}")

        logger.debug(f"{thread_name} stopped")

    def _process_request(self, request: QueuedRequest):
        """Process a single request"""

        # Check rate limiter
        can_proceed, wait_time = self.rate_limiter.can_make_request()

        if not can_proceed:
            logger.debug(f"Rate limited, waiting {wait_time:.2f}s for {request.sku}")
            time.sleep(wait_time)

            # Check again after waiting
            can_proceed, _ = self.rate_limiter.can_make_request()
            if not can_proceed:
                # Still can't proceed, requeue with delay
                self._requeue_request(request, delay=wait_time)
                return

        # Update active request count
        with self.stats_lock:
            self.active_requests += 1

        try:
            # Add small random delay to avoid synchronized requests
            jitter = random.uniform(0.1, 0.5)
            time.sleep(jitter)

            # Execute the request
            logger.debug(f"Processing request: SKU {request.sku}, ZIP {request.zip_code}")

            success = False
            try:
                result = request.callback(request.sku, request.zip_code)
                success = result is not None and result.get('success', False)
            except Exception as e:
                logger.error(f"Request callback failed for {request.sku}: {e}")
                success = False

            # Record result
            self.rate_limiter.record_request(success)

            with self.stats_lock:
                self.total_processed += 1
                if not success:
                    self.failed_requests += 1

            if not success:
                self._handle_failed_request(request)
            else:
                logger.debug(f"Successfully processed {request.sku}")

        except Exception as e:
            logger.error(f"Error processing request {request.sku}: {e}")
            self.rate_limiter.record_request(False)
            self._handle_failed_request(request)

        finally:
            with self.stats_lock:
                self.active_requests -= 1

            # Adaptive delay based on success rate
            self._update_adaptive_delay()

    def _handle_failed_request(self, request: QueuedRequest):
        """Handle a failed request with retry logic"""

        if request.retry_count < request.max_retries:
            request.retry_count += 1

            # Exponential backoff for retries
            retry_delay = min(300, 30 * (2 ** request.retry_count))  # Max 5 minutes

            logger.warning(f"Retrying request for {request.sku} "
                           f"(attempt {request.retry_count}/{request.max_retries}) "
                           f"in {retry_delay}s")

            # Schedule retry
            request.scheduled_at = time.time() + retry_delay
            self.retry_queue.put(request)

        else:
            logger.error(f"Request for {request.sku} failed after {request.max_retries} retries")

    def _requeue_request(self, request: QueuedRequest, delay: float = 0):
        """Requeue a request with delay"""
        request.scheduled_at = time.time() + delay
        self.request_queue.put((request.priority.value, request))

    def _retry_processor(self):
        """Process retry queue"""
        logger.debug("Retry processor started")

        while self.running:
            try:
                request = self.retry_queue.get(timeout=1.0)

                # Wait until scheduled time
                wait_time = request.scheduled_at - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)

                # Requeue for processing
                self.request_queue.put((request.priority.value, request))

            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in retry processor: {e}")

        logger.debug("Retry processor stopped")

    def _update_adaptive_delay(self):
        """Update adaptive delay based on success rate"""
        stats = self.rate_limiter.get_stats()
        current_success_rate = stats.get('success_rate_hour', 1.0)

        # Adjust delay based on success rate
        if current_success_rate < 0.8:
            # Low success rate, increase delay
            self.adaptive_delay = min(10.0, self.adaptive_delay * 1.2)
        elif current_success_rate > 0.95:
            # High success rate, can decrease delay
            self.adaptive_delay = max(1.0, self.adaptive_delay * 0.9)

        self.last_success_rate = current_success_rate

        logger.debug(f"Adaptive delay updated to {self.adaptive_delay:.2f}s "
                     f"(success rate: {current_success_rate:.2%})")

    def get_stats(self) -> Dict:
        """Get queue statistics"""
        with self.stats_lock:
            stats = {
                "queue_size": self.request_queue.qsize(),
                "retry_queue_size": self.retry_queue.qsize(),
                "active_requests": self.active_requests,
                "total_processed": self.total_processed,
                "failed_requests": self.failed_requests,
                "success_rate": (
                        (self.total_processed - self.failed_requests) / max(1, self.total_processed)
                ),
                "adaptive_delay": self.adaptive_delay,
                "last_success_rate": self.last_success_rate
            }

        # Add rate limiter stats
        stats.update(self.rate_limiter.get_stats())

        return stats

    def force_process_all(self, timeout: float = 300.0) -> bool:
        """Force process all queued requests within timeout"""
        start_time = time.time()

        while (self.request_queue.qsize() > 0 or self.active_requests > 0):
            if time.time() - start_time > timeout:
                logger.warning(f"Timeout reached while processing queue")
                return False

            time.sleep(0.5)

        logger.info("All queued requests processed")
        return True

    def clear_queue(self):
        """Clear all pending requests"""
        cleared = 0

        # Clear main queue
        while not self.request_queue.empty():
            try:
                self.request_queue.get_nowait()
                cleared += 1
            except Empty:
                break

        # Clear retry queue
        while not self.retry_queue.empty():
            try:
                self.retry_queue.get_nowait()
                cleared += 1
            except Empty:
                break

        logger.info(f"Cleared {cleared} pending requests from queue")