import discord
from discord.ext import commands
from commands.command_handler import CommandHandler
from integrations.discord_handler import DiscordHandler
from data.database import DatabaseManager
from utils.config import ConfigLoader
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging
import asyncio
import json
import os
from typing import Optional, Dict, List
import re

logger = logging.getLogger(__name__)


class TargetBot(commands.Bot):
    def __init__(self, token: str, webhook_url: str, db_manager: DatabaseManager):
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None
        )

        self.token = token
        self.discord_handler = DiscordHandler(webhook_url)
        self.command_handler = CommandHandler(self.discord_handler)
        self.db_manager = db_manager
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def on_ready(self):
        logger.info(f'Discord bot logged in as {self.user} (ID: {self.user.id})')

        # Sync slash commands
        try:
            synced = await self.tree.sync()
            logger.info(f'Synced {len(synced)} slash commands')
        except Exception as e:
            logger.error(f'Failed to sync slash commands: {e}')

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CommandNotFound):
            return  # Ignore unknown commands

        logger.error(f'Command error: {error}')
        await ctx.send(f'❌ An error occurred: {str(error)}')

    def parse_skus(self, sku_input: str) -> List[str]:
        """Parse SKU input and return list of individual SKUs"""
        # Split by comma and clean up whitespace
        skus = [sku.strip() for sku in sku_input.split(',')]

        # Filter out empty strings and validate SKU format (should be numeric)
        valid_skus = []
        for sku in skus:
            if sku and sku.isdigit() and len(sku) >= 6:  # Basic SKU validation
                valid_skus.append(sku)
            elif sku:  # Log invalid SKUs
                logger.warning(f"Invalid SKU format: {sku}")

        return valid_skus

    def validate_zip_code(self, zip_code: str) -> bool:
        """Validate ZIP code format"""
        # Remove any spaces and validate 5-digit ZIP code
        zip_code = zip_code.strip()
        return bool(re.match(r'^\d{5}$', zip_code))

    def add_skus_to_config(self, sku_list: List[str]) -> List[str]:
        """Add new SKUs to monitoring config and return list of newly added SKUs"""
        try:
            # Load current config
            config_loader = ConfigLoader()
            current_config_data = {}

            if os.path.exists('config.json'):
                with open('config.json', 'r') as f:
                    current_config_data = json.load(f)

            current_skus = set(current_config_data.get('skus', []))
            new_skus = []

            # Check which SKUs are new
            for sku in sku_list:
                if sku not in current_skus:
                    current_skus.add(sku)
                    new_skus.append(sku)

            # Update config if there are new SKUs
            if new_skus:
                current_config_data['skus'] = list(current_skus)

                with open('config.json', 'w') as f:
                    json.dump(current_config_data, f, indent=4)

                logger.info(f"Added {len(new_skus)} new SKUs to config.json: {new_skus}")

                # Notify the main monitor to reload config if possible
                self._notify_config_update()

            return new_skus

        except Exception as e:
            logger.error(f"Failed to add SKUs to config: {e}")
            return []

    def _notify_config_update(self):
        """Notify that config has been updated (for future expansion)"""
        # This could be expanded to signal the main monitor process
        # For now, just log that config was updated
        logger.info("Config updated - main monitor will pick up changes on next cycle")


# Initialize bot instance
bot: Optional[TargetBot] = None


def setup_discord_bot(token: str, webhook_url: str, db_manager: DatabaseManager) -> TargetBot:
    """Setup and return Discord bot instance"""
    global bot

    if not token or token.strip() == "":
        logger.warning("No Discord bot token provided")
        return None

    bot = TargetBot(token, webhook_url, db_manager)

    # Add slash commands
    @bot.tree.command(name="target", description="Check Target stock for SKU(s) and ZIP code")
    async def target_slash(interaction: discord.Interaction, sku: str, zip_code: str):
        """Slash command version of /target - supports multiple SKUs separated by commas"""
        await interaction.response.defer(ephemeral=True)

        try:
            logger.info(f"Slash command /target called by {interaction.user} with SKU: {sku}, ZIP: {zip_code}")

            # Validate ZIP code
            if not bot.validate_zip_code(zip_code):
                await interaction.followup.send(
                    "❌ **Invalid ZIP Code Format**\nPlease provide a valid 5-digit ZIP code (e.g., 30313).",
                    ephemeral=True
                )
                return

            # Parse SKUs - handle both single and multiple SKUs
            sku_list = bot.parse_skus(sku)

            if not sku_list:
                await interaction.followup.send(
                    "❌ **Invalid SKU Format**\nPlease provide valid SKU(s). Examples:\n• Single: `6624827`\n• Multiple: `6624827,6624826,6624825`",
                    ephemeral=True
                )
                return

            if len(sku_list) > 20:  # Limit to prevent abuse
                await interaction.followup.send(
                    f"❌ **Too Many SKUs**\nPlease limit to 20 SKUs or fewer. You provided {len(sku_list)} SKUs.",
                    ephemeral=True
                )
                return

            # Process each SKU individually
            total_results = []
            failed_skus = []

            progress_msg = await interaction.followup.send(
                f"🔄 **Processing {len(sku_list)} SKU(s)...**\nThis may take a moment.",
                ephemeral=True
            )

            for i, single_sku in enumerate(sku_list):
                try:
                    logger.info(f"Processing SKU {single_sku} ({i + 1}/{len(sku_list)})")

                    # Check if this is a new product that needs scraping
                    product_info = bot.discord_handler._get_product_info(single_sku)
                    needs_scraping = "Unknown Product" in product_info['name']

                    if needs_scraping:
                        # Wait 1 minute timeout for new products to get name/image
                        await progress_msg.edit(
                            content=f"🔄 **Processing {len(sku_list)} SKU(s)...**\n⏳ New product detected for SKU {single_sku}, waiting for product info...")

                        # Trigger product scraping (this should be done asynchronously in real implementation)
                        logger.info(f"New product detected for SKU {single_sku}, triggering product info update")

                        # Wait 60 seconds for product info to be scraped
                        await asyncio.sleep(60)

                    # Run the command WITHOUT sending webhook (Discord bot handles its own response)
                    result = bot.command_handler.target_command(single_sku, zip_code, send_webhook=False)

                    if result['success'] and result['stores']:
                        total_results.append({
                            'sku': single_sku,
                            'stores': result['stores'],
                            'message': result['message']
                        })
                    else:
                        failed_skus.append(single_sku)
                        logger.warning(
                            f"Failed to get data for SKU {single_sku}: {result.get('message', 'Unknown error')}")

                    # Small delay between SKUs to be respectful
                    if i < len(sku_list) - 1:
                        await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing SKU {single_sku}: {e}")
                    failed_skus.append(single_sku)

            # Send results
            if total_results:
                stores_with_stock = sum(
                    len([s for s in result['stores'] if s['quantity'] > 0]) for result in total_results)
                total_stores = sum(len(result['stores']) for result in total_results)

                await progress_msg.edit(
                    content=f"✅ **Processing Complete**\nFound {stores_with_stock} stores with stock out of {total_stores} total stores.\n\n**Detailed results sent via DM** ✉️")

                # Send detailed results via DM
                try:
                    for result in total_results:
                        single_sku = result['sku']
                        stores = result['stores']
                        product_info = bot.discord_handler._get_product_info(single_sku)

                        fields = []
                        for store in stores[:23]:  # Limit fields
                            name = store['name']
                            stock = store['quantity']
                            distance = store['distance']
                            stock_display = "0" if stock == 0 else ("9999" if stock > 100 else str(stock))

                            fields.append({
                                "name": f"**{name}**",
                                "value": f"Stock: {stock_display}\nDistance: {distance:.1f}",
                                "inline": True
                            })

                        fields.extend([
                            {"name": "SKU", "value": single_sku, "inline": True},
                            {"name": "Checked ZIP", "value": zip_code, "inline": True}
                        ])

                        dm_embed = discord.Embed(
                            title=product_info['name'],
                            url=product_info['url'],
                            color=0xff0000
                        )

                        for field in fields:
                            dm_embed.add_field(name=field["name"], value=field["value"], inline=field["inline"])

                        if product_info['thumbnail_url']:
                            dm_embed.set_thumbnail(url=product_info['thumbnail_url'])

                        await interaction.user.send(embed=dm_embed)
                        await asyncio.sleep(1)

                except discord.Forbidden:
                    await interaction.followup.send("⚠️ Couldn't send DM. Please enable DMs from server members.",
                                                    ephemeral=True)
            else:
                await progress_msg.edit(content="❌ **All Stock Checks Failed**\nNo valid data received for any SKUs.")

        except Exception as e:
            logger.error(f"Error in slash command: {e}")
            await interaction.followup.send("❌ An error occurred while checking stock.", ephemeral=True)

    @bot.tree.command(name="location-stock", description="Check stock for all monitored products near a ZIP code")
    async def location_stock_slash(interaction: discord.Interaction, zip_code: str):
        """Check stock for all monitored products at a specific location with interactive store selection"""
        await interaction.response.defer()

        try:
            logger.info(f"Slash command /location-stock called by {interaction.user} with ZIP: {zip_code}")

            if not bot.validate_zip_code(zip_code):
                await interaction.followup.send(
                    "❌ **Invalid ZIP Code Format**\nPlease provide a valid 5-digit ZIP code (e.g., 30313).",
                    ephemeral=True)
                return

            config_loader = ConfigLoader()
            current_config = config_loader.load_config()

            if not current_config.skus:
                await interaction.followup.send("❌ **No Products Monitored**\nNo SKUs are currently being monitored.",
                                                ephemeral=True)
                return

            # Initialize stores_by_id at the beginning
            stores_by_id = {}  # To avoid duplicates and store full info

            for sku in current_config.skus:
                try:
                    # Get fresh API data for this specific SKU and ZIP
                    api_result = bot.command_handler.target_command(sku, zip_code, send_webhook=False)

                    if api_result['success'] and api_result['stores']:
                        # Process fresh API data
                        for api_store in api_result['stores']:
                            distance = api_store.get('distance', 999)
                            quantity = api_store.get('quantity', 0)

                            # Only include stores within 50 miles that have stock
                            if distance <= 50 and quantity > 0:
                                store_id = api_store.get('id')

                                # Update database with fresh data
                                store_data = {
                                    'id': store_id,
                                    'name': api_store.get('name'),
                                    'address': api_store.get('address'),
                                    'city': api_store.get('city'),
                                    'state': api_store.get('state'),
                                    'zipCode': api_store.get('zip_code'),
                                    'distance': distance,
                                    'phone': api_store.get('phone'),
                                    'quantity': quantity
                                }
                                bot.db_manager.update_stock(sku, store_data)

                                if store_id not in stores_by_id:
                                    stores_by_id[store_id] = {
                                        'store_id': store_id,
                                        'store_name': api_store.get('name'),
                                        'address': api_store.get('address'),
                                        'city': api_store.get('city'),
                                        'state': api_store.get('state'),
                                        'zip_code': api_store.get('zip_code'),
                                        'distance': distance,
                                        'products': []
                                    }

                                # Add product info to this store
                                product_info = bot.discord_handler._get_product_info(sku)

                                stores_by_id[store_id]['products'].append({
                                    'sku': sku,
                                    'name': product_info['name'],
                                    'quantity': quantity,
                                    'last_updated': datetime.now()
                                })

                    # Small delay between API calls to be respectful
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"Error getting fresh data for SKU {sku}: {e}")

            if not stores_by_id:
                await interaction.followup.send(
                    f"📍 **No Stock Found**\nNo monitored products currently have stock near ZIP code **{zip_code}**.",
                    ephemeral=True)
                return

            # Sort stores by distance
            sorted_stores = sorted(stores_by_id.values(), key=lambda x: x.get('distance', 999))

            # Create initial summary embed
            summary_embed = discord.Embed(
                title=f"📍 Interactive Stock Summary for ZIP {zip_code}",
                description=f"Found **{len(sorted_stores)}** stores with stock. Use the dropdown below to view detailed information for specific stores.",
                color=0x00ff00
            )

            # Add summary fields for each store
            for store in sorted_stores[:10]:  # Show first 10 in summary
                product_count = len(store['products'])
                total_items = sum(product['quantity'] for product in store['products'])

                summary_embed.add_field(
                    name=f"**{store['store_name']}**",
                    value=f"Distance: {store['distance']:.1f} mi\nProducts: {product_count}\nTotal Items: {total_items}",
                    inline=True
                )

            if len(sorted_stores) > 10:
                summary_embed.add_field(
                    name="**Additional Stores**",
                    value=f"...and {len(sorted_stores) - 10} more stores available in dropdown",
                    inline=False
                )

            # Create dropdown options (Discord limits to 25 options)
            options = []
            for i, store in enumerate(sorted_stores[:25]):
                product_count = len(store['products'])
                distance = store['distance']

                # Truncate store name if too long
                store_name = store['store_name']
                if len(store_name) > 80:
                    store_name = store_name[:77] + "..."

                options.append(discord.SelectOption(
                    label=store_name,
                    value=store['store_id'],
                    description=f"{product_count} products • {distance:.1f} mi away",
                    emoji="🏪"
                ))

            if len(sorted_stores) > 25:
                summary_embed.add_field(
                    name="⚠️ **Note**",
                    value=f"Showing first 25 stores in dropdown. {len(sorted_stores) - 25} additional stores have stock but aren't shown.",
                    inline=False
                )

            # Create the select menu
            class StoreSelectView(discord.ui.View):
                def __init__(self, stores_data, zip_code):
                    super().__init__(timeout=300)  # 5 minute timeout
                    self.stores_data = {store['store_id']: store for store in stores_data}
                    self.zip_code = zip_code

                @discord.ui.select(
                    placeholder="Select a store to view detailed stock information...",
                    options=options,
                    custom_id="store_select"
                )
                async def store_select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
                    await interaction.response.defer()

                    selected_store_id = select.values[0]
                    store_info = self.stores_data.get(selected_store_id)

                    if not store_info:
                        await interaction.followup.send("❌ Store information not found.", ephemeral=True)
                        return

                    # Create detailed embed for selected store
                    detail_embed = discord.Embed(
                        title=f"🏪 {store_info['store_name']}",
                        description=f"**Address:** {store_info['address']}, {store_info['city']}, {store_info['state']} {store_info['zip_code']}\n**Distance:** {store_info['distance']:.1f} miles",
                        color=0xff000
                    )

                    # Add product information
                    for product in store_info['products']:
                        stock_display = "9999+" if product['quantity'] > 100 else str(product['quantity'])

                        # Calculate time since stock was added
                        time_display = "Unknown"
                        if product['last_updated']:
                            try:
                                from datetime import datetime, timezone
                                if isinstance(product['last_updated'], str):
                                    # Parse string timestamp
                                    last_updated = datetime.fromisoformat(
                                        product['last_updated'].replace('Z', '+00:00'))
                                else:
                                    last_updated = product['last_updated']

                                # Calculate time difference
                                now = datetime.now(timezone.utc)
                                if last_updated.tzinfo is None:
                                    last_updated = last_updated.replace(tzinfo=timezone.utc)

                                time_diff = now - last_updated

                                if time_diff.days > 0:
                                    time_display = f"{time_diff.days}d ago"
                                elif time_diff.seconds > 3600:
                                    hours = time_diff.seconds // 3600
                                    time_display = f"{hours}h ago"
                                elif time_diff.seconds > 60:
                                    minutes = time_diff.seconds // 60
                                    time_display = f"{minutes}m ago"
                                else:
                                    time_display = "Just now"
                            except:
                                time_display = "Unknown"

                        detail_embed.add_field(
                            name=f"**{product['name']}**",
                            value=f"SKU: `{product['sku']}`\nStock: {stock_display}\nStock Added: {time_display}",
                            inline=True
                        )

                    detail_embed.set_footer(text=f"Store ID: {store_info['store_id']} • ZIP: {self.zip_code}")

                    await interaction.followup.send(embed=detail_embed, ephemeral=True)

                async def on_timeout(self):
                    # Disable the select menu when timeout occurs
                    for item in self.children:
                        item.disabled = True

            # Send the summary with interactive dropdown
            view = StoreSelectView(sorted_stores, zip_code)
            await interaction.followup.send(embed=summary_embed, view=view)

        except Exception as e:
            logger.error(f"Error in location-stock command: {e}")
            await interaction.followup.send("❌ An error occurred while checking location stock.", ephemeral=True)

    @bot.tree.command(name="stores-near", description="Find all Target stores within 100 miles of a ZIP code")
    async def stores_near_slash(interaction: discord.Interaction, zip_code: str):
        """Find all Target stores near a ZIP code and send via DM"""
        await interaction.response.defer(ephemeral=True)

        try:
            logger.info(f"Slash command /stores-near called by {interaction.user} with ZIP: {zip_code}")

            if not bot.validate_zip_code(zip_code):
                await interaction.followup.send(
                    "❌ **Invalid ZIP Code Format**\nPlease provide a valid 5-digit ZIP code (e.g., 30313).",
                    ephemeral=True)
                return

            config_loader = ConfigLoader()
            current_config = config_loader.load_config()

            if not current_config.skus:
                await interaction.followup.send(
                    "❌ **No Data Available**\nNo SKUs are currently being monitored to provide store location data.",
                    ephemeral=True)
                return

            # Show processing message
            progress_msg = await interaction.followup.send(
                f"🔄 **Finding stores near ZIP {zip_code}...**\nSearching for Target locations.",
                ephemeral=True)

            sample_sku = current_config.skus[0]
            all_stores = []
            seen_store_ids = set()

            try:
                stores_from_db = bot.db_manager.get_stores_for_sku_zip(sample_sku, zip_code)

                for store in stores_from_db:
                    store_id = store.get('store_id')
                    if store_id not in seen_store_ids:
                        seen_store_ids.add(store_id)
                        all_stores.append({
                            'store_id': store_id,
                            'store_name': store.get('store_name'),
                            'address': store.get('address'),
                            'city': store.get('city'),
                            'state': store.get('state'),
                            'zip_code': store.get('zip_code'),
                            'distance': store.get('distance'),
                            'phone': store.get('phone') if hasattr(bot.db_manager,
                                                                   'column_exists') and bot.db_manager.column_exists(
                                'store_stock', 'phone') else None
                        })

                if not all_stores:
                    result = bot.command_handler.target_command(sample_sku, zip_code, send_webhook=False)
                    if result['success'] and result['stores']:
                        for store in result['stores']:
                            store_id = store.get('id')
                            if store_id not in seen_store_ids:
                                seen_store_ids.add(store_id)
                                all_stores.append({
                                    'store_id': store_id,
                                    'store_name': store.get('name'),
                                    'address': store.get('address'),
                                    'city': store.get('city'),
                                    'state': store.get('state'),
                                    'zip_code': store.get('zip_code'),
                                    'distance': store.get('distance'),
                                    'phone': store.get('phone')
                                })

            except Exception as e:
                logger.error(f"Error getting stores for ZIP {zip_code}: {e}")

            if not all_stores:
                await progress_msg.edit(
                    content=f"🏪 **No Stores Found**\nNo Target stores found within 100 miles of ZIP code **{zip_code}**.")
                return

            # Sort stores by distance
            all_stores.sort(key=lambda x: x.get('distance', 999))

            # Update progress message
            await progress_msg.edit(
                content=f"✅ **Found {len(all_stores)} stores**\nSending detailed list via DM...")

            # Send results via DM
            try:
                # Split stores into chunks to avoid Discord embed limits
                chunk_size = 25  # 25 stores per embed
                store_chunks = [all_stores[i:i + chunk_size] for i in range(0, len(all_stores), chunk_size)]

                for chunk_num, store_chunk in enumerate(store_chunks):
                    # Create embed for this chunk
                    embed = discord.Embed(
                        title=f"🏪 Target Stores Near ZIP {zip_code}",
                        description=f"Found **{len(all_stores)}** stores within 100 miles" +
                                    (f" (Part {chunk_num + 1}/{len(store_chunks)})" if len(store_chunks) > 1 else ""),
                        color=0xff000
                    )

                    # Add fields for stores in this chunk
                    for store in store_chunk:
                        name = store.get('store_name', 'Unknown')
                        store_zip = store.get('zip_code', 'Unknown')
                        distance = store.get('distance', 0)

                        embed.add_field(
                            name=f"**{name}**",
                            value=f"ZIP: {store_zip}\nDistance: {distance:.1f} mi",
                            inline=True
                        )

                    embed.set_footer(text=f"Target Store Locator • ZIP: {zip_code}")

                    # Send via DM
                    await interaction.user.send(embed=embed)

                    # Small delay between embeds
                    if chunk_num < len(store_chunks) - 1:
                        await asyncio.sleep(1)

                await progress_msg.edit(
                    content=f"✅ **Store List Sent via DM**\nFound **{len(all_stores)}** Target stores within 100 miles of ZIP **{zip_code}**.")

            except discord.Forbidden:
                await progress_msg.edit(
                    content="⚠️ **Couldn't send DM**\nPlease enable DMs from server members to receive the store list.")
            except Exception as e:
                logger.error(f"Error sending DM: {e}")
                await progress_msg.edit(
                    content="❌ **Error sending DM**\nThere was an error sending the store list via DM.")

        except Exception as e:
            logger.error(f"Error in stores-near command: {e}")
            await interaction.followup.send("❌ An error occurred while locating stores.", ephemeral=True)

    @bot.tree.command(name="add-sku", description="Add SKU(s) to monitoring list and start tracking them")
    async def add_sku_slash(interaction: discord.Interaction, sku: str):
        """Add SKU(s) to the monitoring configuration"""
        await interaction.response.defer(ephemeral=True)

        try:
            logger.info(f"Slash command /add-sku called by {interaction.user} with SKU: {sku}")

            sku_list = bot.parse_skus(sku)

            if not sku_list:
                await interaction.followup.send("❌ **Invalid SKU Format**\nPlease provide valid SKU(s).",
                                                ephemeral=True)
                return

            if len(sku_list) > 10:
                await interaction.followup.send(
                    f"❌ **Too Many SKUs**\nPlease limit to 10 SKUs or fewer for monitoring.", ephemeral=True)
                return

            new_skus = bot.add_skus_to_config(sku_list)

            if not new_skus:
                await interaction.followup.send(
                    f"ℹ️ **All SKUs Already Monitored**\nAll provided SKUs are already being monitored.",
                    ephemeral=True)
                return

            for new_sku in new_skus:
                bot.db_manager.reset_initial_reports_for_sku(new_sku)

            response_lines = [f"✅ **Successfully Added {len(new_skus)} SKU(s) to Monitoring**", "",
                              "**New SKUs added:**"]
            for new_sku in new_skus:
                response_lines.append(f"• `{new_sku}`")

            await interaction.followup.send("\n".join(response_lines), ephemeral=True)

        except Exception as e:
            logger.error(f"Error in add-sku command: {e}")
            await interaction.followup.send("❌ An error occurred while adding SKUs.", ephemeral=True)

    @bot.tree.command(name="list-skus", description="Show all currently monitored SKUs")
    async def list_skus_slash(interaction: discord.Interaction):
        """List all currently monitored SKUs with pagination to avoid 2000 char limit"""
        await interaction.response.defer(ephemeral=True)

        try:
            config_loader = ConfigLoader()
            current_config = config_loader.load_config()

            if not current_config.skus:
                await interaction.followup.send(
                    "📋 **No SKUs Currently Monitored**\nUse `/add-sku` to add SKUs to the monitoring list.",
                    ephemeral=True)
                return

            # Split into multiple messages to avoid 2000 char limit
            header = f"📋 **Currently Monitoring {len(current_config.skus)} SKU(s)**\n🗺️ **ZIP Codes**: {', '.join(current_config.zip_codes)}\n⏱️ **Check Interval**: {current_config.monitoring_interval} seconds\n\n**Monitored SKUs:**\n"

            current_message = header
            messages_to_send = []

            # Create list of SKUs with their product names for sorting
            sku_data = []
            for sku in current_config.skus:
                try:
                    product_info = bot.discord_handler._get_product_info(sku)
                    product_name = product_info['name']
                    if "Unknown Product" in product_name:
                        sort_name = f"zzz_{sku}"  # Put unknown products at the end
                        display_name = ""
                    else:
                        sort_name = product_name.lower()  # Sort by lowercase name
                        display_name = product_name

                    sku_data.append({
                        'sku': sku,
                        'sort_name': sort_name,
                        'display_name': display_name
                    })
                except:
                    sku_data.append({
                        'sku': sku,
                        'sort_name': f"zzz_{sku}",
                        'display_name': ""
                    })

            # Sort SKUs alphabetically by product name
            sku_data.sort(key=lambda x: x['sort_name'])

            # Build the message with sorted SKUs
            for sku_item in sku_data:
                sku = sku_item['sku']
                display_name = sku_item['display_name']

                if display_name:
                    # Truncate long names
                    if len(display_name) > 80:
                        display_name = display_name[:77] + "..."
                    line = f"• `{sku}` - {display_name}\n"
                else:
                    line = f"• `{sku}`\n"

                # Check if adding this line would exceed limit
                if len(current_message + line) > 1800:
                    messages_to_send.append(current_message)
                    current_message = "**Monitored SKUs (continued):**\n" + line
                else:
                    current_message += line

            # Add the last message
            if current_message:
                messages_to_send.append(current_message)

            # Send first message
            await interaction.followup.send(messages_to_send[0], ephemeral=True)

            # Send additional messages if needed
            for message in messages_to_send[1:]:
                await asyncio.sleep(0.5)
                await interaction.followup.send(message, ephemeral=True)

        except Exception as e:
            logger.error(f"Error in list-skus command: {e}")
            await interaction.followup.send("❌ An error occurred while listing SKUs.", ephemeral=True)

    @bot.tree.command(name="clear", description="Clear spam/duplicate stock alerts from recent messages")
    async def clear_spam_slash(interaction: discord.Interaction, minutes: int = 60, dry_run: bool = True):
        """Clear spam messages from the channel"""
        await interaction.response.defer(ephemeral=True)

        try:
            logger.info(
                f"Slash command /clear called by {interaction.user} with minutes: {minutes}, dry_run: {dry_run}")

            if minutes < 1 or minutes > 1440:
                await interaction.followup.send(
                    "❌ **Invalid Time Range**\nPlease specify between 1 and 1440 minutes (24 hours).", ephemeral=True)
                return

            if not interaction.user.guild_permissions.manage_messages:
                await interaction.followup.send(
                    "❌ **Insufficient Permissions**\nYou need 'Manage Messages' permission to use this command.",
                    ephemeral=True)
                return

            channel = interaction.channel
            if not channel:
                await interaction.followup.send("❌ **Channel Error**\nCould not determine the current channel.",
                                                ephemeral=True)
                return

            import datetime
            cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=minutes)

            messages_to_analyze = []
            async for message in channel.history(limit=500, after=cutoff_time):
                if (message.author.name == "Target" and message.embeds and len(message.embeds) > 0 and "Stock Alert" in
                        message.embeds[0].title):
                    messages_to_analyze.append(message)

            if not messages_to_analyze:
                await interaction.followup.send(
                    f"📊 **No Stock Alerts Found**\nNo Target stock alert messages found in the last {minutes} minutes.",
                    ephemeral=True)
                return

            # Analyze for spam patterns - group by SKU and Store
            message_groups = {}
            for message in messages_to_analyze:
                try:
                    embed = message.embeds[0]
                    sku = None
                    store_name = None

                    # Extract SKU and Store Name from embed fields
                    for field in embed.fields:
                        if field.name == "SKU":
                            sku = field.value
                        elif field.name == "**Store**":
                            store_name = field.value

                    if sku and store_name:
                        key = f"{sku}_{store_name}"
                        if key not in message_groups:
                            message_groups[key] = []
                        message_groups[key].append(message)
                except Exception as e:
                    logger.warning(f"Error analyzing message {message.id}: {e}")

            spam_messages = []
            spam_details = {
                'rapid_oscillation': [],
                'duplicate_status': [],
                'excessive_frequency': []
            }

            for key, messages in message_groups.items():
                if len(messages) < 2:  # Need at least 2 messages to detect spam
                    continue

                # Sort by timestamp (oldest first)
                messages.sort(key=lambda m: m.created_at)

                # Extract status and timestamp for each message
                message_data = []
                for msg in messages:
                    try:
                        embed = msg.embeds[0]
                        status = None
                        for field in embed.fields:
                            if field.name == "**Status**":
                                status_text = field.value
                                if "🟢 RESTOCK" in status_text:
                                    status = "RESTOCK"
                                elif "🔴 OUT OF STOCK" in status_text:
                                    status = "OUT_OF_STOCK"
                                elif "📈 STOCK INCREASE" in status_text:
                                    status = "RESTOCK"
                                elif "📉 STOCK DECREASE" in status_text:
                                    status = "OUT_OF_STOCK"
                                break

                        if status:
                            message_data.append({
                                'message': msg,
                                'status': status,
                                'timestamp': msg.created_at
                            })
                    except Exception as e:
                        logger.warning(f"Error extracting status from message {msg.id}: {e}")
                        continue

                if len(message_data) < 2:
                    continue

                # Check for rapid oscillation (OUT_OF_STOCK -> RESTOCK -> OUT_OF_STOCK within short time)
                for i in range(len(message_data) - 1):
                    current = message_data[i]
                    next_msg = message_data[i + 1]

                    time_diff = (next_msg['timestamp'] - current['timestamp']).total_seconds()

                    # If same store goes OUT_OF_STOCK -> RESTOCK within 5 minutes, likely spam
                    if (current['status'] == "OUT_OF_STOCK" and
                            next_msg['status'] == "RESTOCK" and
                            time_diff <= 300):  # 5 minutes

                        spam_details['rapid_oscillation'].append(current['message'])
                        spam_details['rapid_oscillation'].append(next_msg['message'])

                        # Check for triple oscillation (OUT -> RESTOCK -> OUT again)
                        if i + 2 < len(message_data):
                            third_msg = message_data[i + 2]
                            time_diff_third = (third_msg['timestamp'] - current['timestamp']).total_seconds()

                            if (third_msg['status'] == "OUT_OF_STOCK" and
                                    time_diff_third <= 600):  # 10 minutes total
                                spam_details['rapid_oscillation'].append(third_msg['message'])

                # Check for duplicate status within 5 minutes
                for i in range(len(message_data) - 1):
                    current = message_data[i]
                    next_msg = message_data[i + 1]

                    time_diff = (next_msg['timestamp'] - current['timestamp']).total_seconds()

                    if (current['status'] == next_msg['status'] and time_diff <= 300):
                        spam_details['duplicate_status'].append(next_msg['message'])

                # Check for excessive frequency (more than 4 alerts in 30 minutes for same store/SKU)
                recent_messages = [msg_data for msg_data in message_data
                                   if (datetime.datetime.now(datetime.timezone.utc) - msg_data[
                        'timestamp']).total_seconds() <= 1800]

                if len(recent_messages) > 4:
                    # Keep first 2 messages, mark rest as spam
                    for msg_data in recent_messages[2:]:
                        spam_details['excessive_frequency'].append(msg_data['message'])

            # Combine all spam messages (remove duplicates)
            all_spam = set()
            for spam_list in spam_details.values():
                all_spam.update(spam_list)

            spam_messages = list(all_spam)

            if not spam_messages:
                await interaction.followup.send(
                    f"✅ **No Spam Detected**\nAnalyzed {len(messages_to_analyze)} stock alerts from the last {minutes} minutes.\nNo spam patterns detected.",
                    ephemeral=True)
                return

            # Create detailed summary
            summary_lines = [
                f"🔍 **Spam Analysis Complete**",
                f"📊 **Messages Analyzed**: {len(messages_to_analyze)}",
                f"🗑️ **Spam Messages Found**: {len(spam_messages)}",
                "",
                "**Spam Categories:**"
            ]

            if spam_details['rapid_oscillation']:
                summary_lines.append(
                    f"• 🔄 Rapid Oscillation (OUT→RESTOCK→OUT): {len(spam_details['rapid_oscillation'])} messages")
            if spam_details['duplicate_status']:
                summary_lines.append(
                    f"• 📋 Duplicate Status (within 5 min): {len(spam_details['duplicate_status'])} messages")
            if spam_details['excessive_frequency']:
                summary_lines.append(
                    f"• ⚡ Excessive Frequency (>4 in 30 min): {len(spam_details['excessive_frequency'])} messages")

            if dry_run:
                summary_lines.extend([
                    "",
                    "🔍 **DRY RUN MODE** - No messages will be deleted",
                    "💡 Use `dry_run:False` to actually delete messages",
                    "",
                    "**Sample Spam Messages:**"
                ])

                # Show some examples of what would be deleted
                for i, msg in enumerate(spam_messages[:5]):  # Show first 5 examples
                    try:
                        embed = msg.embeds[0]
                        store_name = "Unknown"
                        status = "Unknown"

                        for field in embed.fields:
                            if field.name == "**Store**":
                                store_name = field.value
                            elif field.name == "**Status**":
                                status = field.value.split()[1] if len(field.value.split()) > 1 else field.value

                        summary_lines.append(f"• {store_name} - {status} ({msg.created_at.strftime('%H:%M:%S')})")
                    except:
                        summary_lines.append(f"• Message ID {msg.id}")

                if len(spam_messages) > 5:
                    summary_lines.append(f"• ...and {len(spam_messages) - 5} more messages")

                await interaction.followup.send("\n".join(summary_lines), ephemeral=True)
                return

            # Actually delete messages
            deleted_count = 0
            errors = 0

            # Delete in small batches to avoid rate limits
            for i in range(0, len(spam_messages), 3):
                batch = spam_messages[i:i + 3]
                for message in batch:
                    try:
                        await message.delete()
                        deleted_count += 1
                        await asyncio.sleep(0.5)  # Rate limit protection
                    except discord.NotFound:
                        deleted_count += 1  # Already deleted
                    except discord.Forbidden:
                        errors += 1
                        logger.warning(f"No permission to delete message {message.id}")
                    except Exception as e:
                        errors += 1
                        logger.error(f"Error deleting message {message.id}: {e}")

            # Final summary
            final_summary = [
                f"✅ **Cleanup Complete**",
                f"🗑️ **Messages Deleted**: {deleted_count}",
                f"❌ **Errors**: {errors}",
                f"📊 **Remaining Messages**: {len(messages_to_analyze) - deleted_count}",
                "",
                f"🧹 **Removed spam from {len(message_groups)} store/SKU combinations**"
            ]

            # Add breakdown by spam type
            if spam_details['rapid_oscillation']:
                final_summary.append(
                    f"• Oscillation spam: {len([m for m in spam_details['rapid_oscillation'] if m in spam_messages])} messages")
            if spam_details['duplicate_status']:
                final_summary.append(
                    f"• Duplicate spam: {len([m for m in spam_details['duplicate_status'] if m in spam_messages])} messages")
            if spam_details['excessive_frequency']:
                final_summary.append(
                    f"• Frequency spam: {len([m for m in spam_details['excessive_frequency'] if m in spam_messages])} messages")

            await interaction.followup.send("\n".join(final_summary), ephemeral=True)

        except Exception as e:
            logger.error(f"Error in clear command: {e}")
            await interaction.followup.send("❌ An error occurred while clearing messages.", ephemeral=True)

    return bot


async def run_discord_bot(token: str, webhook_url: str, db_manager: DatabaseManager):
    """Run the Discord bot"""
    if not token or token.strip() == "":
        logger.info("No Discord bot token provided, skipping bot startup")
        return

    try:
        bot = setup_discord_bot(token, webhook_url, db_manager)
        if bot:
            logger.info("Starting Discord bot...")
            await bot.start(token)
    except Exception as e:
        logger.error(f"Failed to start Discord bot: {e}")


def stop_discord_bot():
    """Stop the Discord bot"""
    global bot
    if bot:
        logger.info("Stopping Discord bot...")
        try:
            import threading
            if threading.current_thread() == threading.main_thread():
                if hasattr(bot, 'loop') and bot.loop and not bot.loop.is_closed():
                    bot.loop.call_soon_threadsafe(lambda: asyncio.ensure_future(bot.close()))
            logger.info("Discord bot stop initiated")
        except Exception as e:
            logger.error(f"Error stopping Discord bot: {e}")