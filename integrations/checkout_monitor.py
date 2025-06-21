# checkout_monitor.py - Fixed version for PostgreSQL

import discord
import re
import logging
import json
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CheckoutMonitor:
    def __init__(self, db_manager, discord_handler):
        self.db_manager = db_manager
        self.discord_handler = discord_handler
        self.target_channel_id = 1367201642528772116
        self.checkout_webhook_url = self._load_checkout_webhook_url()

    def _load_checkout_webhook_url(self):
        """Load checkout webhook URL from config.json"""
        try:
            if os.path.exists('config.json'):
                with open('config.json', 'r') as f:
                    config = json.load(f)
                    return config.get('checkout_webhook_url', '')
            return ''
        except Exception as e:
            logger.error(f"Error loading checkout webhook URL: {e}")
            return ''

    def is_valid_product_name(self, product_name: str) -> bool:
        """Check if product name is valid (not a hash/encoded string)"""
        if not product_name or len(product_name) < 5:
            return False

        # Patterns that indicate invalid/encoded product names
        invalid_patterns = [
            r'^[a-f0-9]{20,}$',  # Long hex strings like be49a7c4804b9e6e14622628fcdd64d3
            r'^[a-zA-Z0-9]{32,}$',  # Long alphanumeric hashes
            r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$',  # UUIDs
            r'^[a-zA-Z0-9+/]{20,}={0,2}$',  # Base64 encoded strings
        ]

        for pattern in invalid_patterns:
            if re.match(pattern, product_name.strip()):
                logger.debug(f"Invalid product name detected: {product_name}")
                return False

        # Must contain at least one space or common word (real product names have spaces)
        if ' ' not in product_name and not any(word in product_name.lower() for word in [
            'pokemon', 'nintendo', 'xbox', 'playstation', 'card', 'game', 'bundle', 'trading'
        ]):
            logger.debug(f"Suspicious product name (no spaces): {product_name}")
            return False

        return True

    def extract_product_from_embed(self, embed):
        """Extract product name from Discord embed"""
        try:
            # Convert embed to dict for processing
            embed_dict = embed.to_dict()
            return self.extract_product_name(embed_dict)
        except Exception as e:
            logger.error(f"Error converting embed to dict: {e}")
            return None

    def extract_product_name(self, embed_dict):
        """Extract product name from embed"""
        try:
            # The embed structure has the product info in fields
            fields = embed_dict.get('fields', [])
            description = embed_dict.get('description', '') or ''

            # Look through fields for Product field
            for field in fields:
                field_name = field.get('name', '').lower()
                field_value = field.get('value', '')

                if 'product' in field_name:
                    # Clean and validate the product name
                    product_name = self.clean_product_name(field_value)

                    if not product_name:
                        continue

                    if self.is_encoded_product_name(product_name):
                        continue
                    else:
                        return product_name

            # If no fields, try extracting from description
            if description:
                lines = description.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and len(line) > 10 and not self.is_encoded_product_name(line):
                        # Skip lines that look like labels or metadata
                        if not any(skip in line.lower() for skip in
                                   ['price', 'checkout time', 'payment', 'mode', '$', 'am', 'pm', 'click here']):
                            logger.info(f"Found product name in description: {line}")
                            return line

            logger.warning("No valid product name found")
            return None

        except Exception as e:
            logger.error(f"Error extracting product name: {e}")
            import traceback
            traceback.print_exc()
            return None

    def clean_product_name(self, product_name):
        """Clean and normalize product name"""
        if not product_name:
            return None

        # Strip whitespace and common artifacts
        cleaned = product_name.strip()

        # Remove common checkout artifacts
        artifacts_to_remove = [
            'Click Here',
            'click here',
            'Create Task Group',
            'Requests',
            'Normal'
        ]

        for artifact in artifacts_to_remove:
            cleaned = cleaned.replace(artifact, '').strip()

        return cleaned if cleaned else None

    def is_encoded_product_name(self, name):
        """Check if product name is encoded/hash"""
        if not name:
            return True

        name = name.strip()

        # Check for hash-like patterns
        if re.match(r'^[a-f0-9]{32}$', name.lower()):
            return True
        if re.match(r'^[a-f0-9]{40}$', name.lower()):
            return True
        if re.match(r'^[a-f0-9]{20,}$', name.lower()):
            return True
        if len(name) >= 20 and all(c.lower() in 'abcdef0123456789' for c in name):
            return True
        if len(name) < 5 or name.isdigit():
            return True

        return False

    def process_checkout_embed(self, embed_dict):
        """Process a checkout embed (thread-safe version)"""
        try:
            # Extract embed data
            title = embed_dict.get('title', '') or ''
            description = embed_dict.get('description', '') or ''

            # Extract author information (THIS IS WHERE THE CHECKOUT INFO IS)
            author = embed_dict.get('author', {})
            author_name = author.get('name', '') if author else ''

            # Check if this is a Target checkout
            if not ("Successful Checkout" in author_name and "Target" in author_name):
                return

            # Extract product name from embed
            product_name = self.extract_product_name(embed_dict)

            if not product_name:
                logger.warning("Could not extract valid product name from checkout embed")
                return

            logger.info(f"✅ Processing checkout for product: {product_name}")

            # Store in database
            self.store_checkout_product(product_name)

        except Exception as e:
            logger.error(f"Error processing checkout embed: {e}")
            import traceback
            traceback.print_exc()

    def store_checkout_product(self, product_name):
        """Store checkout product in database"""
        try:

            # Add or update product in database
            result = self.db_manager.add_or_update_checkout_product(product_name)

            if result:

                # Send webhook if new product and webhook not sent
                if result.get('is_new') and not result.get('webhook_sent'):
                    self.send_new_product_webhook_sync(product_name)
            else:
                logger.error(f"Failed to store checkout for: {product_name}")

        except Exception as e:
            logger.error(f"Error storing checkout product {product_name}: {e}")

    def send_new_product_webhook_sync(self, product_name: str):
        """Send checkout webhook notification"""
        try:
            if not self.checkout_webhook_url:
                return False

            # Create embed for checkout webhook
            embed_data = {
                "title": "🛒 New Target Checkout Product Detected",
                "color": 0xff0000,
                "fields": [
                    {
                        "name": "**Product**",
                        "value": product_name,
                        "inline": False
                    },
                    {
                        "name": "**Status**",
                        "value": "New product being checked out",
                        "inline": True
                    }
                ],
                "footer": {
                    "text": f"Target Checkout Monitor • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Send to checkout webhook
            success = self._send_checkout_webhook(embed_data)

            if success:
                logger.info(f"Sent checkout webhook for: {product_name}")

            return success

        except Exception as e:
            logger.error(f"Error sending checkout webhook for {product_name}: {e}")
            return False

    def _send_checkout_webhook(self, embed_data):
        """Send webhook to checkout-specific URL"""
        try:
            import requests

            payload = {
                "embeds": [embed_data],
                "username": "🎯 Target Checkout Monitor",
                "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            }

            response = requests.post(
                self.checkout_webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )

            return response.status_code in [200, 204]

        except Exception as e:
            logger.error(f"Error sending checkout webhook: {e}")
            return False

    def _get_or_scrape_product_info(self, product_name):
        """Get product info and scrape image if needed"""
        try:
            # Try to extract SKU from product name or URL
            sku = self._extract_sku_from_product(product_name)

            if sku:
                # Use existing discord_handler method to get/scrape product info
                product_info = self.discord_handler._get_product_info(sku)

                # If no thumbnail, trigger scraping
                if not product_info.get('thumbnail_url'):
                    # This should trigger your existing product scraping logic
                    self.discord_handler.update_product_info(sku, product_name, "", True)
                    product_info = self.discord_handler._get_product_info(sku)

                return product_info
            else:
                # Fallback if no SKU found
                return {
                    'name': product_name,
                    'url': f'https://www.target.com/s?searchTerm={product_name.replace(" ", "+")}',
                    'thumbnail_url': ''
                }

        except Exception as e:
            logger.error(f"Error getting product info for {product_name}: {e}")
            return {
                'name': product_name,
                'url': '',
                'thumbnail_url': ''
            }

    def _extract_sku_from_product(self, product_name):
        """Try to extract SKU from product name or use scraping"""
        # This would need to be implemented based on your existing SKU extraction logic
        # For now, return None and rely on search
        return None

    def _send_checkout_webhook(self, embed_data):
        """Send webhook to checkout-specific URL"""
        try:
            import requests

            payload = {
                "embeds": [embed_data],
                "username": "🎯 Target Checkout Monitor",
                "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            }

            response = requests.post(
                self.checkout_webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )

            return response.status_code in [200, 204]

        except Exception as e:
            logger.error(f"Error sending checkout webhook: {e}")
            return False

    async def send_new_product_webhook(self, product_name: str, checkout_count: int = 1):
        """Send webhook notification for new checkout product"""
        return self.send_new_product_webhook_sync(product_name)

    async def process_checkout_message(self, message: discord.Message):
        """Process a message from the checkout channel"""
        try:
            # Only process messages from the target channel
            if message.channel.id != self.target_channel_id:
                return

            # Only process messages with embeds
            if not message.embeds:
                return

            logger.debug(f"Processing message from checkout channel with {len(message.embeds)} embeds")

            # Process each embed in the message
            for embed in message.embeds:
                product_name = self.extract_product_from_embed(embed)

                if product_name:
                    logger.info(f"Processing checkout for product: {product_name}")

                    # Add or update product in database
                    result = self.db_manager.add_or_update_checkout_product(product_name)

                    if result and result['is_new'] and not result['webhook_sent']:
                        # Send webhook for new product
                        await self.send_new_product_webhook(
                            product_name,
                            result['checkout_count']
                        )
                    elif result and not result['is_new']:
                        logger.info(f"Known product checked out again: {product_name} (Total: {result['checkout_count']})")

        except Exception as e:
            logger.error(f"Error processing checkout message: {e}")

    def get_checkout_stats(self) -> Dict[str, Any]:
        """Get statistics about checkout products"""
        return self.db_manager.get_checkout_stats()

    def get_recent_checkout_products(self, hours: int = 24):
        """Get recent checkout products"""
        return self.db_manager.get_recent_checkout_products(hours)