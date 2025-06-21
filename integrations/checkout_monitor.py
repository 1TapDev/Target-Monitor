# checkout_monitor.py - Complete file for PostgreSQL

import discord
import re
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class CheckoutMonitor:
    def __init__(self, db_manager, discord_handler):
        self.db_manager = db_manager
        self.discord_handler = discord_handler
        self.target_channel_id = 1367201642528772116  # Channel to monitor
        logger.info("Checkout monitor initialized successfully")

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

    def extract_product_from_embed(self, embed: discord.Embed) -> Optional[str]:
        """Extract product name from Target checkout embed"""
        try:
            # Check if this is a Target checkout embed
            if not embed.title or "Successful Checkout | Target" not in embed.title:
                return None

            # Look for Product field
            for field in embed.fields:
                if field.name and "Product" in field.name:
                    product_name = field.value.strip()

                    # Clean up the product name
                    # Remove markdown formatting
                    product_name = re.sub(r'\*\*', '', product_name)  # Remove **
                    product_name = re.sub(r'[_*`]', '', product_name)  # Remove other markdown

                    # Validate product name
                    if self.is_valid_product_name(product_name):
                        logger.info(f"Valid product extracted: {product_name}")
                        return product_name
                    else:
                        logger.debug(f"Invalid product name filtered out: {product_name}")
                        return None

            logger.debug("No Product field found in embed")
            return None

        except Exception as e:
            logger.error(f"Error extracting product from embed: {e}")
            return None

    async def send_new_product_webhook(self, product_name: str, checkout_count: int = 1):
        """Send webhook notification for new checkout product"""
        try:
            # Create embed for webhook
            embed_data = {
                "title": "🛒 New Target Checkout Product Detected",
                "color": 0xff0000,  # Target red
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
                    },
                    {
                        "name": "**Checkout Count**",
                        "value": str(checkout_count),
                        "inline": True
                    }
                ],
                "footer": {
                    "text": f"Target Checkout Monitor • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Send webhook
            success = self.discord_handler.send_webhook_embed(embed_data)

            if success:
                self.db_manager.mark_checkout_webhook_sent(product_name)
                logger.info(f"Sent webhook for new checkout product: {product_name}")
            else:
                logger.error(f"Failed to send webhook for: {product_name}")

            return success

        except Exception as e:
            logger.error(f"Error sending webhook for product {product_name}: {e}")
            return False

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
                        logger.info(
                            f"Known product checked out again: {product_name} (Total: {result['checkout_count']})")

        except Exception as e:
            logger.error(f"Error processing checkout message: {e}")

    def get_checkout_stats(self) -> Dict[str, Any]:
        """Get statistics about checkout products"""
        return self.db_manager.get_checkout_stats()

    def get_recent_checkout_products(self, hours: int = 24):
        """Get recent checkout products"""
        return self.db_manager.get_recent_checkout_products(hours)