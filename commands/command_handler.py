from api.bestbuy_api import BestBuyAPI
from integrations.discord_handler import DiscordHandler
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class CommandHandler:
    def __init__(self, discord_handler: DiscordHandler):
        self.api = BestBuyAPI()
        self.discord_handler = discord_handler

    def bestbuy_command(self, sku: str, zip_code: str, send_webhook: bool = True) -> Dict:
        """
        Mimic /bestbuy sku zip command
        Returns store data and optionally sends Discord embed
        """
        logger.info(f"Processing bestbuy command for SKU {sku}, ZIP {zip_code}, send_webhook={send_webhook}")

        try:
            # Get data from API
            response_data = self.api.get_stock_data(sku, zip_code)

            if not response_data:
                logger.error(f"No data received for SKU {sku}, ZIP {zip_code}")
                return {
                    'success': False,
                    'message': 'Failed to fetch stock data',
                    'stores': []
                }

            # Extract stores
            stores = self.api.extract_stores_from_response(response_data)

            if not stores:
                logger.warning(f"No stores found for SKU {sku}, ZIP {zip_code}")
                return {
                    'success': True,
                    'message': 'No stores found',
                    'stores': []
                }

            # Process store data and add quantities
            processed_stores = []
            for store in stores:
                quantity = self.api.get_store_quantity(store)
                store_info = {
                    'id': store.get('id'),
                    'name': store.get('name'),
                    'address': store.get('address'),
                    'city': store.get('city'),
                    'state': store.get('state'),
                    'zip_code': store.get('zipCode'),
                    'phone': store.get('phone'),
                    'distance': store.get('distance', 0),
                    'quantity': quantity,
                    'location_format': store.get('locationFormat')
                }
                processed_stores.append(store_info)

            # Sort by distance
            processed_stores.sort(key=lambda x: x.get('distance', 999))

            # Send Discord embed only if requested (for webhook commands, not Discord bot)
            if send_webhook:
                self.discord_handler.send_store_list(sku, zip_code, processed_stores)

            logger.info(f"Successfully processed {len(processed_stores)} stores for SKU {sku}")

            return {
                'success': True,
                'message': f'Found {len(processed_stores)} stores',
                'stores': processed_stores
            }

        except Exception as e:
            logger.error(f"Error in bestbuy command for SKU {sku}, ZIP {zip_code}: {e}")
            return {
                'success': False,
                'message': f'Error: {str(e)}',
                'stores': []
            }

    def close(self):
        """Close API connection"""
        self.api.close()