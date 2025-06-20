import requests
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DiscordHandler:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.products_file = 'products.json'
        self.products_cache = None
        self.products_last_modified = 0
        # Role mappings for location tagging
        self.location_roles = {
            'NY': '<@&1348464319154753546>',  # New York role
            'GA': '<@&1348464394039721994>'  # Georgia role
        }
        # Target brand colors
        self.colors = {
            'restock': 0xCC0000,  # Target Red for restocks
            'out_of_stock': 0x8B0000,  # Dark Red for out of stock
            'increase': 0xFF4500,  # Orange Red for increases
            'decrease': 0xDC143C,  # Crimson for decreases
            'initial': 0xCC0000,  # Target Red for initial reports
            'info': 0xB22222,  # Fire Brick for general info
            'error': 0x800000  # Maroon for errors
        }
        # Load products initially
        self._load_products()

    def _load_products(self) -> Dict:
        """Load product information from products.json with file modification checking"""
        try:
            # Check if file exists
            if not os.path.exists(self.products_file):
                logger.warning(f"{self.products_file} not found, creating default file")
                self._create_default_products_file()
                return self.products_cache

            # Get file modification time
            current_modified_time = os.path.getmtime(self.products_file)

            # Check if we need to reload the file
            if (self.products_cache is None or
                    current_modified_time != self.products_last_modified):

                # Only log reloads, not initial loads
                if self.products_cache is not None:
                    logger.info(f"Reloading {self.products_file} (file modified)")

                with open(self.products_file, 'r') as f:
                    self.products_cache = json.load(f)

                self.products_last_modified = current_modified_time

                # Only log detailed info if it's a reload
                if self.products_cache is not None:
                    logger.info(f"Loaded {len(self.products_cache)} products from {self.products_file}")

            return self.products_cache

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {self.products_file}: {e}")
            # Return cached version if available, otherwise empty dict
            return self.products_cache if self.products_cache else {}
        except Exception as e:
            logger.error(f"Error loading {self.products_file}: {e}")
            # Return cached version if available, otherwise empty dict
            return self.products_cache if self.products_cache else {}

    def _create_default_products_file(self):
        """Create a default products.json file"""
        try:
            default_products = {
                "94693225": {
                    "name": "Nintendo Switch 2 Console",
                    "thumbnail_url": "",
                    "send_initial": False
                }
            }

            with open(self.products_file, 'w') as f:
                json.dump(default_products, f, indent=2)

            self.products_cache = default_products
            self.products_last_modified = os.path.getmtime(self.products_file)
            logger.info(f"Created default {self.products_file}")

        except Exception as e:
            logger.error(f"Failed to create default {self.products_file}: {e}")
            self.products_cache = {}

    def _save_products(self):
        """Save the current products cache to file"""
        try:
            with open(self.products_file, 'w') as f:
                json.dump(self.products_cache, f, indent=2)

            # Update our modification time tracking
            self.products_last_modified = os.path.getmtime(self.products_file)
            logger.info(f"Saved products to {self.products_file}")

        except Exception as e:
            logger.error(f"Failed to save {self.products_file}: {e}")

    def _get_product_info(self, sku: str) -> Dict:
        """Get product information for a SKU with dynamic reloading"""
        # Always reload products to check for changes
        products = self._load_products()

        product = products.get(sku, {})

        # If product not found, create entry with placeholder
        if not product:
            logger.warning(f"SKU {sku} not found in {self.products_file}, creating placeholder entry")
            placeholder_product = {
                "name": f"Target Product {sku}",
                "thumbnail_url": "",
                "send_initial": True  # Default to sending initial reports for new SKUs
            }

            # Add to products cache and save
            self.products_cache[sku] = placeholder_product
            self._save_products()

            product = placeholder_product

        return {
            'name': product.get('name', f'Target Product {sku}'),
            'thumbnail_url': product.get('thumbnail_url', ''),
            'url': f'https://www.target.com/p/-/A-{sku}',
            'send_initial': product.get('send_initial', True)  # Default to True if not specified
        }

    def _get_location_tag(self, state: str) -> str:
        """Get the appropriate role tag for a location"""
        return self.location_roles.get(state, '')

    def _format_target_store_name(self, store_name: str) -> str:
        """Format Target store names for better readability"""
        if not store_name:
            return "Unknown Store"

        # Clean up common Target store name patterns
        formatted_name = store_name.strip()

        # Add "Target" prefix if not present
        if not formatted_name.lower().startswith('target'):
            formatted_name = f"Target {formatted_name}"

        return formatted_name

    def _get_stock_emoji_and_status(self, current_qty: int, previous_qty: int) -> tuple:
        """Get appropriate emoji and status text for Target stock changes"""
        prev_qty = previous_qty if previous_qty is not None else 0

        if current_qty > 0 and prev_qty == 0:
            return "🔴🎯", "RESTOCK ALERT", self.colors['restock']
        elif current_qty == 0 and prev_qty > 0:
            return "❌🎯", "OUT OF STOCK", self.colors['out_of_stock']
        elif current_qty > prev_qty:
            return "📈🎯", "STOCK INCREASE", self.colors['increase']
        elif current_qty < prev_qty:
            return "📉🎯", "STOCK DECREASE", self.colors['decrease']
        else:
            return "🎯", "NO CHANGE", self.colors['info']

    def reload_products(self):
        """Force reload products.json (useful for testing)"""
        logger.info("Force reloading products.json")
        self.products_last_modified = 0  # Force reload
        return self._load_products()

    def update_product_info(self, sku: str, name: str, thumbnail_url: str = "", send_initial: bool = True):
        """Update product information for a SKU"""
        try:
            # Load current products
            products = self._load_products()

            # Update the product
            self.products_cache[sku] = {
                "name": name,
                "thumbnail_url": thumbnail_url,
                "send_initial": send_initial
            }

            # Save to file
            self._save_products()

            logger.info(f"Updated product info for SKU {sku}: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to update product info for SKU {sku}: {e}")
            return False

    def should_send_initial_report(self, sku: str) -> bool:
        """Check if initial reports should be sent for this SKU based on products.json"""
        try:
            product_info = self._get_product_info(sku)
            return product_info.get('send_initial', True)
        except Exception as e:
            logger.error(f"Error checking send_initial flag for SKU {sku}: {e}")
            return True  # Default to True if error

    def send_stock_alert(self, sku: str, store_data: Dict, previous_qty: Optional[int], current_qty: int):
        """Send a Target stock change alert with red theme and location tagging"""

        # Handle None previous quantity
        prev_qty = previous_qty if previous_qty is not None else 0

        # Get emoji, status, and color for this change
        emoji, status, color = self._get_stock_emoji_and_status(current_qty, prev_qty)

        # Skip if no actual change
        if current_qty == prev_qty:
            return

        # Get product info (this will now check for file changes)
        product_info = self._get_product_info(sku)

        # Format store name with Target branding
        store_name = self._format_target_store_name(store_data.get('name', 'Unknown'))

        # Format address
        address = store_data.get('address', '')
        city = store_data.get('city', '')
        state = store_data.get('state', '')
        zip_code = store_data.get('zipCode', '')
        full_address = f"{address}, {city}, {state} {zip_code}"

        # Get location tag based on state
        location_tag = self._get_location_tag(state)

        # Target-specific stock quantity formatting
        def format_quantity(qty):
            if qty == 0:
                return "❌ Out of Stock"
            elif qty >= 100:
                return "🟢 99+ in stock"
            elif qty >= 10:
                return f"🟡 {qty} in stock"
            else:
                return f"🟠 {qty} in stock"

        # Create fields for the store alert with Target-specific formatting
        fields = [
            {
                "name": "🎯 **Status**",
                "value": f"**{status}**",
                "inline": True
            },
            {
                "name": "🏪 **Store Location**",
                "value": store_name,
                "inline": True
            },
            {
                "name": "📍 **Distance**",
                "value": f"{store_data.get('distance', 0):.1f} miles",
                "inline": True
            },
            {
                "name": "📊 **Previous Stock**",
                "value": format_quantity(prev_qty),
                "inline": True
            },
            {
                "name": "📦 **Current Stock**",
                "value": format_quantity(current_qty),
                "inline": True
            },
            {
                "name": "📞 **Store Phone**",
                "value": store_data.get('phone', 'N/A'),
                "inline": True
            },
            {
                "name": "🗺️ **Full Address**",
                "value": full_address,
                "inline": False
            },
            {
                "name": "🔢 Product SKU",
                "value": f"`{sku}`",
                "inline": True
            },
            {
                "name": "🏬 Store ID",
                "value": f"`{store_data.get('id', 'Unknown')}`",
                "inline": True
            }
        ]

        embed = {
            "title": f"{emoji} {product_info['name']}",
            "url": product_info['url'],
            "color": color,
            "fields": fields,
            "author": {
                "name": "🎯 Target Stock Monitor",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "footer": {
                "text": f"Target Stock Alert • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add thumbnail if available
        if product_info['thumbnail_url']:
            embed["thumbnail"] = {"url": product_info['thumbnail_url']}

        # Prepare webhook payload with Target branding and location tagging
        content = location_tag if location_tag else ""

        payload = {
            "content": content,
            "embeds": [embed],
            "username": "🎯 Target Monitor",
            "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
        }

        # Log the alert being sent
        logger.info(f"TARGET ALERT: {status} - {store_name} - SKU {sku}")
        self._send_webhook_payload(payload)

    def send_store_list(self, sku: str, zip_code: str, stores_data: List[Dict]):
        """Send a full Target store list with red theme formatting"""

        if not stores_data:
            embed = {
                "title": "🎯 Target Stock Information",
                "description": f"No Target stores found for SKU: `{sku}` near ZIP code `{zip_code}`",
                "color": self.colors['error'],
                "author": {
                    "name": "🎯 Target Stock Monitor",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "footer": {
                    "text": f"Target Stock Monitor • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            self._send_webhook(embed)
            return

        # Sort by distance
        stores_data.sort(key=lambda x: x.get('distance', 999))

        # Get product info
        product_info = self._get_product_info(sku)

        # Check if all stores are out of stock
        stores_with_stock = [store for store in stores_data if store.get('quantity', 0) > 0]

        if not stores_with_stock:
            # All stores are out of stock - send Target-themed message
            fields = [
                {
                    "name": "❌ **Stock Status**",
                    "value": "All nearby Target locations are currently out of stock",
                    "inline": False
                },
                {
                    "name": "🔢 **Product SKU**",
                    "value": f"`{sku}`",
                    "inline": True
                },
                {
                    "name": "📍 **Search Area**",
                    "value": f"ZIP {zip_code}",
                    "inline": True
                },
                {
                    "name": "🏪 **Stores Checked**",
                    "value": f"{len(stores_data)} Target locations",
                    "inline": True
                }
            ]

            embed = {
                "title": f"🎯 {product_info['name']}",
                "url": product_info['url'],
                "color": self.colors['out_of_stock'],
                "fields": fields,
                "author": {
                    "name": "🎯 Target Stock Monitor",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "footer": {
                    "text": f"Target Stock Monitor • Powered by Target API • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "timestamp": datetime.utcnow().isoformat()
            }

            # Add thumbnail if available
            if product_info['thumbnail_url']:
                embed["thumbnail"] = {"url": product_info['thumbnail_url']}

            payload = {
                "embeds": [embed],
                "username": "🎯 Target Monitor",
                "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            }

            logger.info(f"TARGET STORE LIST: SKU {sku} - All stores out of stock")
            self._send_webhook_payload(payload)
            return

        # Some stores have stock - show Target-themed format
        fields = []

        for store in stores_data[:23]:  # Limit to 23 stores to leave room for SKU/ZIP fields
            name = self._format_target_store_name(store.get('name', 'Unknown'))
            stock = store.get('quantity', 0)
            distance = store.get('distance', 0)

            # Target-specific stock display
            if stock == 0:
                stock_display = "❌ Out of Stock"
                stock_emoji = "❌"
            elif stock >= 100:
                stock_display = "🟢 99+"
                stock_emoji = "🟢"
            elif stock >= 10:
                stock_display = f"🟡 {stock}"
                stock_emoji = "🟡"
            else:
                stock_display = f"🟠 {stock}"
                stock_emoji = "🟠"

            fields.append({
                "name": f"{stock_emoji} **{name}**",
                "value": f"Stock: {stock_display}\nDistance: {distance:.1f} mi",
                "inline": True
            })

        # Add summary fields
        fields.extend([
            {
                "name": "🔢 Product SKU",
                "value": f"`{sku}`",
                "inline": True
            },
            {
                "name": "📍 Search ZIP",
                "value": f"`{zip_code}`",
                "inline": True
            },
            {
                "name": "📊 Stock Summary",
                "value": f"{len(stores_with_stock)} of {len(stores_data)} stores have stock",
                "inline": True
            }
        ])

        embed = {
            "title": f"🎯 {product_info['name']}",
            "url": product_info['url'],
            "color": self.colors['info'],
            "fields": fields,
            "description": f"**Target Stock Report** • Found {len(stores_with_stock)} stores with inventory",
            "author": {
                "name": "🎯 Target Stock Monitor",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "footer": {
                "text": f"Target Stock Monitor • Powered by Target API • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add thumbnail if available
        if product_info['thumbnail_url']:
            embed["thumbnail"] = {"url": product_info['thumbnail_url']}

        payload = {
            "embeds": [embed],
            "username": "🎯 Target Monitor",
            "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
        }

        logger.info(f"TARGET STORE LIST: SKU {sku} - {len(stores_with_stock)} stores with stock")
        self._send_webhook_payload(payload)

    def send_initial_stock_report(self, sku: str, zip_code: str, stores_with_stock: List[Dict]):
        """Send initial Target stock report with red theme"""

        if not stores_with_stock:
            return

        # Sort by distance
        stores_with_stock.sort(key=lambda x: x.get('distance', 999))

        # Get product info
        product_info = self._get_product_info(sku)

        # Create fields for each store with stock
        fields = []

        for store in stores_with_stock[:23]:  # Limit to 23 stores
            name = self._format_target_store_name(store.get('name', 'Unknown'))
            stock = store.get('quantity', 0)
            distance = store.get('distance', 0)

            # Target stock formatting
            if stock >= 100:
                stock_display = "🟢 99+"
                stock_emoji = "🟢"
            elif stock >= 10:
                stock_display = f"🟡 {stock}"
                stock_emoji = "🟡"
            else:
                stock_display = f"🟠 {stock}"
                stock_emoji = "🟠"

            fields.append({
                "name": f"{stock_emoji} **{name}**",
                "value": f"Stock: {stock_display}\nDistance: {distance:.1f} mi",
                "inline": True
            })

        # Add summary fields
        fields.extend([
            {
                "name": "🔢 Product SKU",
                "value": f"`{sku}`",
                "inline": True
            },
            {
                "name": "📍 Search ZIP",
                "value": f"`{zip_code}`",
                "inline": True
            },
            {
                "name": "📦 Total Stock",
                "value": f"{sum(store.get('quantity', 0) for store in stores_with_stock)} units",
                "inline": True
            }
        ])

        embed = {
            "title": f"🎯📦 {product_info['name']} - Initial Stock Report",
            "url": product_info['url'],
            "color": self.colors['initial'],
            "fields": fields,
            "description": f"**Initial Target Stock Scan** • Found {len(stores_with_stock)} stores with inventory",
            "author": {
                "name": "🎯 Target Stock Monitor",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "footer": {
                "text": f"Target Stock Monitor • Initial scan complete • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add thumbnail if available
        if product_info['thumbnail_url']:
            embed["thumbnail"] = {"url": product_info['thumbnail_url']}

        if len(stores_with_stock) > 23:
            embed[
                "description"] += f"\n*Showing first 23 stores. {len(stores_with_stock) - 23} more stores have stock.*"

        payload = {
            "embeds": [embed],
            "username": "🎯 Target Monitor",
            "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
        }

        logger.info(f"TARGET INITIAL STOCK: SKU {sku}, ZIP {zip_code} - {len(stores_with_stock)} stores")
        self._send_webhook_payload(payload)

    def send_location_stock_summary(self, zip_code: str, location_stores: List[Dict]):
        """Send a Target location stock summary with red theme"""

        if not location_stores:
            embed = {
                "title": f"🎯📍 Target Stock Summary for ZIP {zip_code}",
                "description": "No Target stores found near this ZIP code.",
                "color": self.colors['error'],
                "author": {
                    "name": "🎯 Target Stock Monitor",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "footer": {
                    "text": f"Target Stock Monitor • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            self._send_webhook(embed)
            return

        # Group by SKU and count stores with stock
        sku_summary = {}
        total_stores_with_stock = 0

        for store in location_stores:
            sku = store.get('sku')
            if sku not in sku_summary:
                product_info = self._get_product_info(sku)
                sku_summary[sku] = {
                    'name': product_info['name'],
                    'stores_with_stock': 0,
                    'total_quantity': 0
                }

            if store.get('quantity', 0) > 0:
                sku_summary[sku]['stores_with_stock'] += 1
                sku_summary[sku]['total_quantity'] += store.get('quantity', 0)
                total_stores_with_stock += 1

        # Create fields for each SKU that has stock
        fields = []
        skus_with_stock = 0

        for sku, data in sku_summary.items():
            if data['stores_with_stock'] > 0:
                skus_with_stock += 1

                # Target stock formatting
                total_qty = data['total_quantity']
                if total_qty >= 100:
                    qty_display = "🟢 99+"
                elif total_qty >= 50:
                    qty_display = f"🟡 {total_qty}"
                else:
                    qty_display = f"🟠 {total_qty}"

                fields.append({
                    "name": f"🎯 **{data['name']}**",
                    "value": f"SKU: `{sku}`\nStores: {data['stores_with_stock']}\nTotal Stock: {qty_display}",
                    "inline": True
                })

        if not fields:
            embed = {
                "title": f"🎯📍 Target Stock Summary for ZIP {zip_code}",
                "description": "No products currently in stock at nearby Target locations.",
                "color": self.colors['out_of_stock'],
                "author": {
                    "name": "🎯 Target Stock Monitor",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "footer": {
                    "text": f"Target Stock Monitor • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            embed = {
                "title": f"🎯📍 Target Stock Summary for ZIP {zip_code}",
                "description": f"Found **{skus_with_stock}** products with stock across **{total_stores_with_stock}** Target store locations",
                "color": self.colors['info'],
                "fields": fields,
                "author": {
                    "name": "🎯 Target Stock Monitor",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "footer": {
                    "text": f"Target Stock Monitor • Location Summary • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "timestamp": datetime.utcnow().isoformat()
            }

        payload = {
            "embeds": [embed],
            "username": "🎯 Target Monitor",
            "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
        }

        logger.info(f"TARGET LOCATION SUMMARY: ZIP {zip_code} - {skus_with_stock} products with stock")
        self._send_webhook_payload(payload)

    def send_stores_near_location(self, zip_code: str, stores: List[Dict]):
        """Send a list of Target stores near a location with red theme"""

        if not stores:
            embed = {
                "title": f"🎯🏪 Target Stores Near ZIP {zip_code}",
                "description": "No Target stores found within 100 miles of this ZIP code.",
                "color": self.colors['error'],
                "author": {
                    "name": "🎯 Target Store Locator",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "footer": {
                    "text": f"Target Store Locator • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                    "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            self._send_webhook(embed)
            return

        # Sort by distance
        stores.sort(key=lambda x: x.get('distance', 999))

        # Create fields for each store
        fields = []

        for store in stores[:25]:  # Limit to 25 stores
            name = self._format_target_store_name(store.get('store_name', store.get('name', 'Unknown')))
            store_zip = store.get('zip_code', 'Unknown')
            distance = store.get('distance', 0)

            fields.append({
                "name": f"🎯 **{name}**",
                "value": f"ZIP: `{store_zip}`\nDistance: {distance:.1f} mi",
                "inline": True
            })

        embed = {
            "title": f"🎯🏪 Target Stores Near ZIP {zip_code}",
            "description": f"Found **{len(stores)}** Target stores within 100 miles" + (
                f" (showing first 25)" if len(stores) > 25 else ""),
            "color": self.colors['info'],
            "fields": fields,
            "author": {
                "name": "🎯 Target Store Locator",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "footer": {
                "text": f"Target Store Locator • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
                "icon_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        payload = {
            "embeds": [embed],
            "username": "🎯 Target Monitor",
            "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
        }

        logger.info(f"TARGET STORES NEAR: ZIP {zip_code} - {len(stores)} stores found")
        self._send_webhook_payload(payload)

    def _send_webhook_payload(self, payload: Dict):
        """Send webhook payload to Discord"""
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )

            # Only log webhook failures, not successes
            if response.status_code != 204:
                logger.error(f"Discord webhook failed: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"Failed to send Discord webhook: {e}")

    def _send_webhook_with_image(self, embed: Dict, image_path: Optional[str] = None):
        """Send webhook to Discord with optional image attachment (legacy method)"""
        try:
            files = {}

            if image_path and os.path.exists(image_path):
                # Read the image file
                with open(image_path, 'rb') as f:
                    files['file'] = (f"image.webp", f.read(), 'image/webp')

                # Set the attachment URL in the embed
                embed["thumbnail"] = {"url": "attachment://image.webp"}

            # Prepare the payload with Target branding
            payload = {
                "embeds": [embed],
                "username": "🎯 Target Monitor",
                "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
            }

            if files:
                # Send with file attachment
                response = requests.post(
                    self.webhook_url,
                    data={'payload_json': json.dumps(payload)},
                    files=files
                )
            else:
                # Send without file
                response = requests.post(
                    self.webhook_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'}
                )

            # Only log failures
            if response.status_code != 204:
                logger.error(f"Discord webhook failed: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"Failed to send Discord webhook: {e}")

    def _send_webhook(self, embed: Dict):
        """Send webhook to Discord without image"""
        payload = {
            "embeds": [embed],
            "username": "🎯 Target Monitor",
            "avatar_url": "https://logos-world.net/wp-content/uploads/2020/04/Target-Logo.png"
        }
        self._send_webhook_payload(payload)