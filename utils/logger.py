import logging
import sys
from logging.handlers import RotatingFileHandler
import os
import warnings
from datetime import datetime


def setup_logger(log_filename=None):
    """Configure logging to file with error output to console and suppress Chrome warnings"""

    # Suppress specific warnings and logs
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", message=".*GPU.*")
    warnings.filterwarnings("ignore", message=".*WebGL.*")
    warnings.filterwarnings("ignore", message=".*GroupMarkerNotSet.*")

    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Generate timestamp-based filename if not provided
    if log_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{timestamp}.log"

    # Full path to log file
    log_path = os.path.join('logs', log_filename)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers.clear()

    # File handler with rotation and UTF-8 encoding
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'  # Fix Unicode issues
    )
    file_handler.setLevel(logging.INFO)

    # Console handler only for errors with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.ERROR)

    # Set console encoding to UTF-8 if possible
    if hasattr(console_handler.stream, 'reconfigure'):
        try:
            console_handler.stream.reconfigure(encoding='utf-8')
        except:
            pass

    # Formatter with timestamp - use simple text instead of emojis for better compatibility
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Suppress console output for specific loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver.remote').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver.common').setLevel(logging.WARNING)

    # Suppress Discord.py debug logs
    logging.getLogger('discord').setLevel(logging.WARNING)
    logging.getLogger('discord.client').setLevel(logging.WARNING)
    logging.getLogger('discord.gateway').setLevel(logging.WARNING)
    logging.getLogger('discord.http').setLevel(logging.WARNING)

    # Create a custom filter to block Chrome GPU/WebGL messages
    class ChromeLogFilter(logging.Filter):
        def filter(self, record):
            message = record.getMessage().lower()
            blocked_keywords = [
                'webgl', 'gpu', 'groupmarkernotset', 'gles2_cmd_decoder',
                'command_buffer', 'swiftshader', 'automatic fallback',
                'unsafe-swiftshader', 'voice_transcription', 'absl::initializelog'
            ]
            return not any(keyword in message for keyword in blocked_keywords)

    # Apply the filter to both handlers
    chrome_filter = ChromeLogFilter()
    file_handler.addFilter(chrome_filter)
    console_handler.addFilter(chrome_filter)

    # Log the filename being used
    logger.info(f"Logger initialized - Log file: {log_path}")

    return logger

def setup_inventory_logger():
    """Setup specialized logger for inventory changes - condensed format for AI analysis"""

    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Create inventory logger
    inventory_logger = logging.getLogger('inventory')
    inventory_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    inventory_logger.handlers.clear()

    # Inventory file handler with rotation
    inventory_handler = RotatingFileHandler(
        'logs/inventory.log',
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=10,
        encoding='utf-8'
    )
    inventory_handler.setLevel(logging.INFO)

    # Condensed formatter for AI analysis
    # Format: TIMESTAMP|ACTION|SKU|STORE_ID|STORE_NAME|PREV_QTY|NEW_QTY|DISTANCE|CITY|STATE|ZIP
    inventory_formatter = logging.Formatter(
        '%(asctime)s|%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    inventory_handler.setFormatter(inventory_formatter)
    inventory_logger.addHandler(inventory_handler)

    # Prevent propagation to root logger
    inventory_logger.propagate = False

    return inventory_logger


def log_inventory_change(action: str, sku: str, store_id: str, store_name: str,
                         prev_qty: int, new_qty: int, distance: float = 0,
                         city: str = "", state: str = "", zip_code: str = "",
                         retailer: str = "target"):
    """
    Log inventory changes to the inventory log file

    Args:
        action: Type of change (NEW_STORE, RESTOCK, OUT_OF_STOCK, INCREASE, DECREASE)
        sku: Product SKU
        store_id: Store identifier
        store_name: Store name
        prev_qty: Previous quantity
        new_qty: New quantity
        distance: Distance from search location
        city: Store city
        state: Store state
        zip_code: Store ZIP code
        retailer: Retailer name (target, bestbuy, etc.)
    """
    try:
        # Get the inventory logger (assumes setup_inventory_logger was called)
        inventory_logger = logging.getLogger('inventory')

        # Format the log message with retailer context
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Enhanced log format with retailer
        log_message = (
            f"{timestamp} | {retailer.upper()} | {action} | "
            f"SKU:{sku} | Store:{store_id} | {store_name} | "
            f"{prev_qty}→{new_qty} | {city}, {state} {zip_code} | "
            f"Distance:{distance:.1f}mi"
        )

        # Log at appropriate level based on action
        if action in ['RESTOCK', 'NEW_STORE']:
            inventory_logger.info(log_message)
        elif action == 'OUT_OF_STOCK':
            inventory_logger.warning(log_message)
        else:
            inventory_logger.info(log_message)

    except Exception as e:
        # Fallback to main logger if inventory logger fails
        main_logger = logging.getLogger(__name__)
        main_logger.error(f"Failed to log inventory change: {e}")
        main_logger.info(f"Inventory change: {action} - {retailer} SKU {sku} at {store_name}")


def get_logger(name: str):
    """Get a logger instance for a specific module"""
    return logging.getLogger(name)