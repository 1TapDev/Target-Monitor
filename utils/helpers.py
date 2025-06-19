"""
Helper functions and utilities for the Best Buy Monitor
"""

import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def format_uptime(start_time: datetime) -> str:
    """Format uptime duration"""
    if not start_time:
        return "Unknown"

    uptime = datetime.now() - start_time
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m {seconds}s"


def calculate_request_load(skus: List[str], zip_codes: List[str], 
                          interval: int) -> Dict[str, float]:
    """Calculate API request load metrics"""
    requests_per_cycle = len(skus) * len(zip_codes)
    cycles_per_day = 86400 / interval
    daily_requests = requests_per_cycle * cycles_per_day

    return {
        'requests_per_cycle': requests_per_cycle,
        'cycles_per_day': cycles_per_day, 
        'daily_requests': daily_requests,
        'safety_percentage': (daily_requests / 50000) * 100
    }


def validate_sku_format(sku: str) -> bool:
    """Validate SKU format"""
    return sku.isdigit() and len(sku) >= 6


def validate_zip_format(zip_code: str) -> bool:
    """Validate ZIP code format"""
    return zip_code.isdigit() and len(zip_code) == 5


def get_safety_level(percentage: float) -> str:
    """Get safety level based on API usage percentage"""
    if percentage < 60:
        return "SAFE"
    elif percentage < 80:
        return "CAUTION"
    else:
        return "HIGH RISK"


def retry_with_backoff(func, max_retries: int = 3, base_delay: float = 1.0):
    """Retry function with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e

            delay = base_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {e}")
            time.sleep(delay)
