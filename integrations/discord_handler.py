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
                "6624827": {
                    "name": "Destined Rivals Sleeved Booster",
                    "thumbnail_url": "https://media.gamestop.com/i/gamestop/20021588",
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
                "name": f"Unknown Product (SKU: {sku})",
                "thumbnail_url": "",
                "send_initial": True  # Default to sending initial reports for new SKUs
            }

            # Add to products cache and save
            self.products_cache[sku] = placeholder_product
            self._save_products()

            product = placeholder_product

        return {
            'name': product.get('name', f'SKU {sku}'),
            'thumbnail_url': product.get('thumbnail_url', ''),
            'url': f'https://www.bestbuy.com/site/{sku}.p',
            'send_initial': product.get('send_initial', True)  # Default to True if not specified
        }

    def _get_location_tag(self, state: str) -> str:
        """Get the appropriate role tag for a location"""
        return self.location_roles.get(state, '')

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
        """Send a stock change alert with location tagging"""

        # Handle None previous quantity
        prev_qty = previous_qty if previous_qty is not None else 0

        # Determine status and color
        if current_qty > 0 and prev_qty == 0:
            status = "🟢 RESTOCK"
            color = 0x00ff00  # Green
        elif current_qty == 0 and prev_qty > 0:
            status = "🔴 OUT OF STOCK"
            color = 0xff0000  # Red
        elif current_qty > prev_qty:
            status = "📈 STOCK INCREASE"
            color = 0x00ff00  # Green
        elif current_qty < prev_qty:
            status = "📉 STOCK DECREASE"
            color = 0xffa500  # Orange
        else:
            return  # No change, don't send

        # Get product info (this will now check for file changes)
        product_info = self._get_product_info(sku)

        # Format address
        address = store_data.get('address', '')
        city = store_data.get('city', '')
        state = store_data.get('state', '')
        zip_code = store_data.get('zipCode', '')
        full_address = f"{address}, {city}, {state} {zip_code}"

        # Get location tag based on state
        location_tag = self._get_location_tag(state)

        # Create fields for the store alert
        fields = [
            {
                "name": "**Status**",
                "value": status,
                "inline": True
            },
            {
                "name": "**Store**",
                "value": store_data.get('name', 'Unknown'),
                "inline": True
            },
            {
                "name": "**Distance**",
                "value": f"{store_data.get('distance', 0):.1f} miles",
                "inline": True
            },
            {
                "name": "**Previous Stock**",
                "value": str(prev_qty),
                "inline": True
            },
            {
                "name": "**Current Stock**",
                "value": str(current_qty),
                "inline": True
            },
            {
                "name": "**Phone**",
                "value": store_data.get('phone', 'N/A'),
                "inline": True
            },
            {
                "name": "**Address**",
                "value": full_address,
                "inline": False
            },
            {
                "name": "SKU",
                "value": sku,
                "inline": True
            },
            {
                "name": "Store ID",
                "value": store_data.get('id', 'Unknown'),
                "inline": True
            }
        ]

        embed = {
            "title": f"🚨 {product_info['name']} - Stock Alert",
            "url": product_info['url'],
            "color": color,
            "fields": fields,
            "author": {
                "name": "BestBuy Stock Checker"
            },
            "footer": {
                "text": f"Best Buy Stock Monitor • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add thumbnail if available
        if product_info['thumbnail_url']:
            embed["thumbnail"] = {"url": product_info['thumbnail_url']}

        # Prepare webhook payload with custom username and avatar, and location tagging
        content = location_tag if location_tag else ""

        payload = {
            "content": content,
            "embeds": [embed],
            "username": "BestBuy",
            "avatar_url": "https://us1-photo.nextdoor.com/business_logo/b9/e8/b9e8bb8b4990e4b61213d97f8f843757.jpg"
        }

        # Log the alert being sent
        logger.info(f"ALERT SENT: {status} - {store_data.get('name', 'Unknown')} - SKU {sku}")
        self._send_webhook_payload(payload)

    def send_store_list(self, sku: str, zip_code: str, stores_data: List[Dict]):
        """Send a full store list (Your custom format with fields)"""

        if not stores_data:
            embed = {
                "title": "Best Buy Stock Information",
                "description": f"No stores found for SKU: {sku} near {zip_code}",
                "color": 0xff0000,
                "timestamp": datetime.utcnow().isoformat()
            }
            self._send_webhook(embed)
            return

        # Sort by distance
        stores_data.sort(key=lambda x: x.get('distance', 999))

        # Get product info (this will now check for file changes)
        product_info = self._get_product_info(sku)

        # Check if all stores are out of stock
        stores_with_stock = [store for store in stores_data if store.get('quantity', 0) > 0]

        if not stores_with_stock:
            # All stores are out of stock - send simplified message
            fields = [
                {
                    "name": "**Status**",
                    "value": "All nearby locations are out of stock",
                    "inline": False
                },
                {
                    "name": "**SKU**",
                    "value": sku,
                    "inline": True
                },
                {
                    "name": "**Checked ZIP**",
                    "value": zip_code,
                    "inline": True
                }
            ]

            embed = {
                "title": product_info['name'],
                "url": product_info['url'],
                "color": 0xff0000,  # Red for out of stock
                "fields": fields,
                "author": {
                    "name": "BestBuy Stock Checker"
                },
                "footer": {
                    "text": f"Best Buy Stock Monitor • Powered by BestBuy API • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
                },
                "timestamp": datetime.utcnow().isoformat()
            }

            # Add thumbnail if available
            if product_info['thumbnail_url']:
                embed["thumbnail"] = {"url": product_info['thumbnail_url']}

            # Prepare webhook payload with custom username and avatar
            payload = {
                "embeds": [embed],
                "username": "BestBuy",
                "avatar_url": "https://us1-photo.nextdoor.com/business_logo/b9/e8/b9e8bb8b4990e4b61213d97f8f843757.jpg"
            }

            logger.info(f"STORE LIST: SKU {sku} - All stores out of stock")
            self._send_webhook_payload(payload)
            return

        # Some stores have stock - show normal format
        # Create fields for each store (your requested format)
        fields = []

        for store in stores_data[:25]:  # Limit to 25 stores (Discord limit minus SKU/ZIP fields)
            name = store.get('name', 'Unknown')
            stock = store.get('quantity', 0)
            distance = store.get('distance', 0)

            if stock == 0:
                stock_display = "0"
            elif stock > 100:
                stock_display = "9999"
            else:
                stock_display = str(stock)

            fields.append({
                "name": f"**{name}**",
                "value": f"Stock: {stock_display}\nDistance: {distance:.1f}",
                "inline": True
            })

        # Add SKU and ZIP fields at the end
        fields.extend([
            {
                "name": "SKU",
                "value": sku,
                "inline": True
            },
            {
                "name": "Checked ZIP",
                "value": zip_code,
                "inline": True
            }
        ])

        embed = {
            "title": product_info['name'],
            "url": product_info['url'],
            "color": 255,
            "fields": fields,
            "author": {
                "name": "BestBuy Stock Checker"
            },
            "footer": {
                "text": f"Best Buy Stock Monitor • Powered by BestBuy API • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add thumbnail if available
        if product_info['thumbnail_url']:
            embed["thumbnail"] = {"url": product_info['thumbnail_url']}

        # Prepare webhook payload with custom username and avatar
        payload = {
            "embeds": [embed],
            "username": "BestBuy",
            "avatar_url": "https://us1-photo.nextdoor.com/business_logo/b9/e8/b9e8bb8b4990e4b61213d97f8f843757.jpg"
        }

        logger.info(f"STORE LIST: SKU {sku} - {len(stores_with_stock)} stores with stock")
        self._send_webhook_payload(payload)

    def send_initial_stock_report(self, sku: str, zip_code: str, stores_with_stock: List[Dict]):
        """Send initial stock report using the new field format"""

        if not stores_with_stock:
            return

        # Sort by distance
        stores_with_stock.sort(key=lambda x: x.get('distance', 999))

        # Get product info (this will now check for file changes)
        product_info = self._get_product_info(sku)

        # Create fields for each store with stock
        fields = []

        for store in stores_with_stock[:25]:  # Limit to 25 stores
            name = store.get('name', 'Unknown')
            stock = store.get('quantity', 0)
            distance = store.get('distance', 0)

            if stock > 100:
                stock_display = "9999"
            else:
                stock_display = str(stock)

            fields.append({
                "name": f"**{name}**",
                "value": f"Stock: {stock_display}\nDistance: {distance:.1f}",
                "inline": True
            })

        # Add SKU and ZIP fields at the end
        fields.extend([
            {
                "name": "SKU",
                "value": sku,
                "inline": True
            },
            {
                "name": "ZIP Code",
                "value": zip_code,
                "inline": True
            }
        ])

        embed = {
            "title": f"📦 {product_info['name']} - Initial Stock",
            "url": product_info['url'],
            "color": 0x00ff00,
            "fields": fields,
            "author": {
                "name": "BestBuy Stock Checker"
            },
            "footer": {
                "text": f"Best Buy Stock Monitor • Initial scan complete • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add thumbnail if available
        if product_info['thumbnail_url']:
            embed["thumbnail"] = {"url": product_info['thumbnail_url']}

        if len(stores_with_stock) > 25:
            embed["description"] = f"*Showing first 25 stores. {len(stores_with_stock) - 25} more stores have stock.*"

        # Prepare webhook payload with custom username and avatar
        payload = {
            "embeds": [embed],
            "username": "BestBuy",
            "avatar_url": "https://us1-photo.nextdoor.com/business_logo/b9/e8/b9e8bb8b4990e4b61213d97f8f843757.jpg"
        }

        logger.info(f"INITIAL STOCK: SKU {sku}, ZIP {zip_code} - {len(stores_with_stock)} stores")
        self._send_webhook_payload(payload)

    def send_location_stock_summary(self, zip_code: str, location_stores: List[Dict]):
        """Send a summary of all products with stock at a specific location"""

        if not location_stores:
            embed = {
                "title": f"📍 Stock Summary for ZIP {zip_code}",
                "description": "No stores found near this ZIP code or ZIP code not found.",
                "color": 0xff0000,
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
                fields.append({
                    "name": f"**{data['name']}**",
                    "value": f"SKU: {sku}\nStores: {data['stores_with_stock']}\nTotal Stock: {data['total_quantity']}",
                    "inline": True
                })

        if not fields:
            embed = {
                "title": f"📍 Stock Summary for ZIP {zip_code}",
                "description": "No products currently in stock at nearby locations.",
                "color": 0xff0000,
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            embed = {
                "title": f"📍 Stock Summary for ZIP {zip_code}",
                "description": f"Found **{skus_with_stock}** products with stock across **{total_stores_with_stock}** store locations",
                "color": 0x00ff00,
                "fields": fields,
                "author": {
                    "name": "BestBuy Stock Checker"
                },
                "footer": {
                    "text": f"Best Buy Stock Monitor • Location Summary • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
                },
                "timestamp": datetime.utcnow().isoformat()
            }

        # Prepare webhook payload
        payload = {
            "embeds": [embed],
            "username": "BestBuy",
            "avatar_url": "https://us1-photo.nextdoor.com/business_logo/b9/e8/b9e8bb8b4990e4b61213d97f8f843757.jpg"
        }

        logger.info(f"LOCATION SUMMARY: ZIP {zip_code} - {skus_with_stock} products with stock")
        self._send_webhook_payload(payload)

    def send_stores_near_location(self, zip_code: str, stores: List[Dict]):
        """Send a list of all stores near a location"""

        if not stores:
            embed = {
                "title": f"🏪 Stores Near ZIP {zip_code}",
                "description": "No Best Buy stores found within 100 miles of this ZIP code or ZIP code not found.",
                "color": 0xff0000,
                "timestamp": datetime.utcnow().isoformat()
            }
            self._send_webhook(embed)
            return

        # Sort by distance
        stores.sort(key=lambda x: x.get('distance', 999))

        # Create fields for each store
        fields = []

        for store in stores[:25]:  # Limit to 25 stores (Discord limit)
            name = store.get('store_name', store.get('name', 'Unknown'))
            store_zip = store.get('zip_code', 'Unknown')

            fields.append({
                "name": f"**{name}**",
                "value": f"ZIP: {store_zip}",
                "inline": True
            })

        embed = {
            "title": f"🏪 Best Buy Stores Near ZIP {zip_code}",
            "description": f"Found **{len(stores)}** stores within 100 miles" + (
                f" (showing first 25)" if len(stores) > 25 else ""),
            "color": 0x0066cc,
            "fields": fields,
            "author": {
                "name": "BestBuy Stock Checker"
            },
            "footer": {
                "text": f"Best Buy Stock Monitor • Store Locator • {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

        # Prepare webhook payload
        payload = {
            "embeds": [embed],
            "username": "BestBuy",
            "avatar_url": "https://us1-photo.nextdoor.com/business_logo/b9/e8/b9e8bb8b4990e4b61213d97f8f843757.jpg"
        }

        logger.info(f"STORES NEAR: ZIP {zip_code} - {len(stores)} stores found")
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

            # Prepare the payload
            payload = {
                "embeds": [embed]
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
        self._send_webhook_payload({"embeds": [embed]})