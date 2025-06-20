from api.target_api import TargetAPI
from integrations.discord_handler import DiscordHandler
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class CommandHandler:
    def __init__(self, discord_handler: DiscordHandler):
        self.api = TargetAPI()
        self.discord_handler = discord_handler

    def target_command(self, sku: str, zip_code: str, send_webhook: bool = True) -> Dict:
        """
        Mimic /target sku zip command for Target stores
        Returns store data and optionally sends Discord embed
        """
        logger.info(f"Processing Target command for SKU {sku}, ZIP {zip_code}, send_webhook={send_webhook}")

        try:
            # Get data from Target API
            response_data = self.api.get_stock_data(sku, zip_code)

            if not response_data:
                logger.error(f"No Target data received for SKU {sku}, ZIP {zip_code}")
                return {
                    'success': False,
                    'message': 'Failed to fetch Target stock data',
                    'stores': []
                }

            # Extract stores using Target-specific method
            stores = self.api.extract_stores_from_response(response_data)

            if not stores:
                logger.warning(f"No Target stores found for SKU {sku}, ZIP {zip_code}")
                return {
                    'success': True,
                    'message': 'No Target stores found',
                    'stores': []
                }

            # Process Target store data with dual quantity support
            processed_stores = []
            stores_with_stock = 0

            for store in stores:
                # Extract Target-specific quantities
                pickup_qty = 0
                instore_qty = 0
                total_qty = 0

                # Get quantity using Target API method
                total_qty = self.api.get_store_quantity(store)

                # Extract Target-specific pickup and in-store quantities
                if hasattr(store, 'get') and 'pickup_quantity' in store:
                    pickup_qty = store.get('pickup_quantity', 0)
                    instore_qty = store.get('instore_quantity', 0)
                else:
                    # Extract from Target API response structure
                    if 'availability_data' in store and store['availability_data']:
                        availability = store['availability_data'].get('availability', {})
                        instore_availability = store['availability_data'].get('inStoreAvailability', {})

                        pickup_qty = availability.get('availablePickupQuantity', 0)
                        instore_qty = instore_availability.get('availableInStoreQuantity', 0)

                        # Handle Target's 9999 high stock indicator
                        if pickup_qty == 9999:
                            pickup_qty = 999
                        if instore_qty == 9999:
                            instore_qty = 999

                        # Use the higher quantity as total if not already set
                        if total_qty == 0:
                            total_qty = max(pickup_qty, instore_qty)

                # Count stores with any stock
                if total_qty > 0:
                    stores_with_stock += 1

                # Create Target store info with enhanced data
                store_info = {
                    'id': store.get('id'),
                    'name': store.get('name', 'Unknown Target Store'),
                    'address': store.get('address', ''),
                    'city': store.get('city', ''),
                    'state': store.get('state', ''),
                    'zip_code': store.get('zipCode', ''),
                    'phone': store.get('phone', ''),
                    'distance': float(store.get('distance', 0)),
                    'quantity': total_qty,
                    'pickup_quantity': pickup_qty,  # Target-specific
                    'instore_quantity': instore_qty,  # Target-specific
                    'location_format': store.get('locationFormat', ''),
                    'retailer': 'target'  # Add retailer identifier
                }

                # Add Target-specific availability info for debugging
                if 'availability_data' in store:
                    store_info['availability_debug'] = store['availability_data']

                processed_stores.append(store_info)

            # Sort by distance (closest first)
            processed_stores.sort(key=lambda x: x.get('distance', 999))

            # Log Target-specific summary
            total_stores = len(processed_stores)
            logger.info(f"Target command results: {total_stores} stores found, {stores_with_stock} with stock")

            # Log quantity breakdown for debugging
            if stores_with_stock > 0:
                total_pickup = sum(store['pickup_quantity'] for store in processed_stores)
                total_instore = sum(store['instore_quantity'] for store in processed_stores)
                logger.info(f"Target quantity breakdown - Pickup: {total_pickup}, In-store: {total_instore}")

            # Send Discord embed only if requested (for webhook commands, not Discord bot)
            if send_webhook:
                try:
                    self.discord_handler.send_store_list(sku, zip_code, processed_stores)
                    logger.info(f"Sent Target Discord webhook for SKU {sku}")
                except Exception as webhook_error:
                    logger.error(f"Failed to send Target Discord webhook: {webhook_error}")
                    # Don't fail the whole command if webhook fails

            # Create success message with Target context
            if stores_with_stock == 0:
                message = f'Found {total_stores} Target stores, but none have stock'
            else:
                message = f'Found {total_stores} Target stores, {stores_with_stock} with stock'

            logger.info(f"Successfully processed Target command: {message}")

            return {
                'success': True,
                'message': message,
                'stores': processed_stores,
                'summary': {
                    'total_stores': total_stores,
                    'stores_with_stock': stores_with_stock,
                    'retailer': 'target',
                    'sku': sku,
                    'zip_code': zip_code
                }
            }

        except Exception as e:
            error_msg = f"Error in Target command for SKU {sku}, ZIP {zip_code}: {e}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': f'Target API Error: {str(e)}',
                'stores': [],
                'error': str(e)
            }

    def close(self):
        """Close API connection"""
        self.api.close()