import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from utils.config import DatabaseConfig
from utils.logger import log_inventory_change
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.username,
                password=self.db_config.password
            )
            self.connection.autocommit = True
            # Only log successful connection once
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            with self.connection.cursor() as cursor:
                # Main stock table - just ensure it exists, don't modify structure
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS store_stock (
                        id SERIAL PRIMARY KEY,
                        sku VARCHAR(20) NOT NULL,
                        store_id VARCHAR(10) NOT NULL,
                        store_name VARCHAR(255),
                        address VARCHAR(500),
                        city VARCHAR(100),
                        state VARCHAR(5),
                        zip_code VARCHAR(10),
                        distance FLOAT,
                        quantity INTEGER NOT NULL,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(sku, store_id)
                    );
                """)

                # Initial stock reports tracking table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS initial_stock_reports (
                        id SERIAL PRIMARY KEY,
                        sku VARCHAR(20) NOT NULL,
                        zip_code VARCHAR(10) NOT NULL,
                        reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(sku, zip_code)
                    );
                """)

                # Create basic indexes
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sku_store 
                    ON store_stock(sku, store_id);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_initial_reports 
                    ON initial_stock_reports(sku, zip_code);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_zip_code 
                    ON store_stock(zip_code);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_state 
                    ON store_stock(state);
                """)

                # Try to create indexes for new columns if they exist
                try:
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_quantity_changed 
                        ON store_stock(sku, store_id, quantity_last_changed);
                    """)
                except:
                    # Column doesn't exist, skip this index
                    pass

                try:
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_quantity_gt_zero 
                        ON store_stock(sku, zip_code) WHERE quantity > 0;
                    """)
                except:
                    # Skip if there's an issue
                    pass

                logger.info("Database tables and indexes created/verified successfully")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def has_initial_report_been_sent(self, sku: str, zip_code: str) -> bool:
        """Check if an initial stock report has been sent for this SKU-ZIP combination"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM initial_stock_reports WHERE sku = %s AND zip_code = %s",
                    (sku, zip_code)
                )
                result = cursor.fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Failed to check initial report status for {sku}, {zip_code}: {e}")
            return False

    def mark_initial_report_sent(self, sku: str, zip_code: str):
        """Mark that an initial stock report has been sent for this SKU-ZIP combination"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO initial_stock_reports (sku, zip_code)
                    VALUES (%s, %s)
                    ON CONFLICT (sku, zip_code) DO NOTHING
                """, (sku, zip_code))
                logger.info(f"Marked initial report as sent for SKU {sku}, ZIP {zip_code}")
        except Exception as e:
            logger.error(f"Failed to mark initial report for {sku}, {zip_code}: {e}")

    def get_stores_for_sku_zip_optimized(self, sku: str, zip_code: str) -> List[Dict]:
        """Optimized version with better indexing"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Use index hints and limit results
                cursor.execute("""
                    SELECT * FROM store_stock 
                    WHERE sku = %s AND zip_code = %s AND last_updated > NOW() - INTERVAL '24 hours'
                    ORDER BY distance ASC
                    LIMIT 50
                """, (sku, zip_code))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get stores for SKU {sku}, ZIP {zip_code}: {e}")
            return []

    def reset_initial_reports_for_sku(self, sku: str):
        """Reset initial report flags for a specific SKU (useful when adding new SKU)"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM initial_stock_reports WHERE sku = %s",
                    (sku,)
                )
                logger.info(f"Reset initial report flags for SKU {sku}")
        except Exception as e:
            logger.error(f"Failed to reset initial reports for {sku}: {e}")

    def get_previous_stock(self, sku: str, store_id: str) -> Optional[int]:
        """Get the previous stock quantity for a store"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT quantity FROM store_stock WHERE sku = %s AND store_id = %s",
                    (sku, store_id)
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get previous stock for {sku}, {store_id}: {e}")
            return None

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name=%s AND column_name=%s;
                """, (table_name, column_name))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check if column {column_name} exists: {e}")
            return False

    def update_stock(self, sku: str, store_data: Dict) -> Tuple[bool, Optional[int]]:
        """
        Update stock for a store and return if it changed and previous quantity
        Returns: (changed, previous_quantity)
        """
        store_id = store_data.get('id')
        current_quantity = self._extract_quantity(store_data)

        previous_quantity = self.get_previous_stock(sku, store_id)

        # Handle None comparison safely
        if previous_quantity is None:
            changed = True  # First time seeing this store, consider it a change
            quantity_changed = True

            # Log as new store
            log_inventory_change(
                action="NEW_STORE",
                sku=sku,
                store_id=store_id,
                store_name=store_data.get('name', ''),
                prev_qty=0,
                new_qty=current_quantity,
                distance=store_data.get('distance', 0),
                city=store_data.get('city', ''),
                state=store_data.get('state', ''),
                zip_code=store_data.get('zipCode', '')
            )
        else:
            changed = previous_quantity != current_quantity
            quantity_changed = changed

            # Log inventory changes
            if changed:
                # Determine action type
                if current_quantity > 0 and previous_quantity == 0:
                    action = "RESTOCK"
                elif current_quantity == 0 and previous_quantity > 0:
                    action = "OUT_OF_STOCK"
                elif current_quantity > previous_quantity:
                    action = "INCREASE"
                elif current_quantity < previous_quantity:
                    action = "DECREASE"
                else:
                    action = "NO_CHANGE"  # Shouldn't happen but just in case

                log_inventory_change(
                    action=action,
                    sku=sku,
                    store_id=store_id,
                    store_name=store_data.get('name', ''),
                    prev_qty=previous_quantity,
                    new_qty=current_quantity,
                    distance=store_data.get('distance', 0),
                    city=store_data.get('city', ''),
                    state=store_data.get('state', ''),
                    zip_code=store_data.get('zipCode', '')
                )

        # Rest of the database update logic stays the same...
        try:
            with self.connection.cursor() as cursor:
                # Check if new columns exist
                has_quantity_changed_col = self.column_exists('store_stock', 'quantity_last_changed')
                has_phone_col = self.column_exists('store_stock', 'phone')

                if quantity_changed and has_quantity_changed_col:
                    # Use new columns if they exist
                    if has_phone_col:
                        cursor.execute("""
                            INSERT INTO store_stock 
                            (sku, store_id, store_name, address, city, state, zip_code, distance, quantity, phone, quantity_last_changed)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (sku, store_id) 
                            DO UPDATE SET 
                                quantity = EXCLUDED.quantity,
                                last_updated = CURRENT_TIMESTAMP,
                                quantity_last_changed = CURRENT_TIMESTAMP,
                                phone = EXCLUDED.phone
                        """, (
                            sku, store_id, store_data.get('name', ''), store_data.get('address', ''),
                            store_data.get('city', ''), store_data.get('state', ''), store_data.get('zipCode', ''),
                            store_data.get('distance', 0), current_quantity, store_data.get('phone', '')
                        ))
                    else:
                        cursor.execute("""
                            INSERT INTO store_stock 
                            (sku, store_id, store_name, address, city, state, zip_code, distance, quantity, quantity_last_changed)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (sku, store_id) 
                            DO UPDATE SET 
                                quantity = EXCLUDED.quantity,
                                last_updated = CURRENT_TIMESTAMP,
                                quantity_last_changed = CURRENT_TIMESTAMP
                        """, (
                            sku, store_id, store_data.get('name', ''), store_data.get('address', ''),
                            store_data.get('city', ''), store_data.get('state', ''), store_data.get('zipCode', ''),
                            store_data.get('distance', 0), current_quantity
                        ))
                else:
                    # Use basic columns only
                    cursor.execute("""
                        INSERT INTO store_stock 
                        (sku, store_id, store_name, address, city, state, zip_code, distance, quantity)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sku, store_id) 
                        DO UPDATE SET 
                            quantity = EXCLUDED.quantity,
                            last_updated = CURRENT_TIMESTAMP
                    """, (
                        sku, store_id, store_data.get('name', ''), store_data.get('address', ''),
                        store_data.get('city', ''), store_data.get('state', ''), store_data.get('zipCode', ''),
                        store_data.get('distance', 0), current_quantity
                    ))

        except Exception as e:
            logger.error(f"Failed to update stock for {sku}, {store_id}: {e}")

        return changed, previous_quantity

    def get_stock_first_seen_time(self, sku: str, store_id: str) -> Optional[datetime]:
        """Get the time when this item was first added to stock (quantity > 0)"""
        try:
            with self.connection.cursor() as cursor:
                # Check if quantity_last_changed column exists
                if self.column_exists('store_stock', 'quantity_last_changed'):
                    cursor.execute("""
                        SELECT quantity_last_changed 
                        FROM store_stock 
                        WHERE sku = %s AND store_id = %s AND quantity > 0
                    """, (sku, store_id))
                else:
                    # Fallback to last_updated
                    cursor.execute("""
                        SELECT last_updated 
                        FROM store_stock 
                        WHERE sku = %s AND store_id = %s AND quantity > 0
                    """, (sku, store_id))

                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get first seen time for {sku}, {store_id}: {e}")
            return None

    def get_stale_single_stock_stores(self, sku: str, hours: int = 1) -> List[str]:
        """Get store IDs that have had quantity=1 for more than specified hours AND haven't been marked as stale recently"""
        try:
            with self.connection.cursor() as cursor:
                if self.column_exists('store_stock', 'quantity_last_changed'):
                    # Only mark stores as stale if:
                    # 1. They currently have quantity = 1
                    # 2. The quantity_last_changed is more than X hours ago
                    # 3. The last_updated is recent (within last 10 minutes) - meaning we're still getting fresh data
                    # 4. They haven't been marked as 0 and back to 1 recently (oscillation protection)
                    cursor.execute("""
                        SELECT store_id, store_name, quantity_last_changed 
                        FROM store_stock 
                        WHERE sku = %s 
                        AND quantity = 1 
                        AND quantity_last_changed < NOW() - INTERVAL %s
                        AND last_updated > NOW() - INTERVAL '10 minutes'
                        AND NOT EXISTS (
                            SELECT 1 FROM store_stock s2 
                            WHERE s2.sku = store_stock.sku 
                            AND s2.store_id = store_stock.store_id 
                            AND s2.quantity = 0 
                            AND s2.last_updated > NOW() - INTERVAL '2 hours'
                        )
                    """, (sku, f'{hours} hours'))
                else:
                    # Fallback to last_updated if quantity_last_changed doesn't exist
                    # More conservative approach - only mark stale if last_updated is old
                    cursor.execute("""
                        SELECT store_id, store_name, last_updated 
                        FROM store_stock 
                        WHERE sku = %s 
                        AND quantity = 1 
                        AND last_updated < NOW() - INTERVAL %s
                    """, (sku, f'{hours * 2} hours'))  # Double the time for safety

                stale_stores = cursor.fetchall()
                if stale_stores:
                    # Additional validation - only return stores that consistently show 1 stock
                    validated_stores = []
                    for store_id, store_name, last_changed in stale_stores:
                        # Check if this store has been oscillating between 0 and 1
                        cursor.execute("""
                            SELECT COUNT(DISTINCT quantity) as distinct_quantities
                            FROM store_stock 
                            WHERE sku = %s 
                            AND store_id = %s 
                            AND last_updated > NOW() - INTERVAL '6 hours'
                        """, (sku, store_id))

                        result = cursor.fetchone()
                        distinct_quantities = result[0] if result else 1

                        # Only mark as stale if it hasn't been oscillating
                        if distinct_quantities <= 2:  # Allow some fluctuation but not rapid oscillation
                            validated_stores.append(store_id)
                            logger.info(
                                f"  Validated stale store: {store_name} (ID: {store_id}) - unchanged since {last_changed}")

                    if validated_stores:
                        logger.info(f"Found {len(validated_stores)} confirmed stale single-stock stores for SKU {sku}")
                        return validated_stores
                    else:
                        logger.info(f"No confirmed stale stores for SKU {sku} after validation")

                return []
        except Exception as e:
            logger.error(f"Failed to get stale stores for SKU {sku}: {e}")
            return []

    def mark_stale_stores_as_zero(self, sku: str, store_ids: List[str]) -> int:
        """Mark stale single-stock stores as having 0 quantity with oscillation protection"""
        if not store_ids:
            return 0

        try:
            with self.connection.cursor() as cursor:
                updated_count = 0

                for store_id in store_ids:
                    # Double-check the store is still at quantity 1 and hasn't been updated recently
                    cursor.execute("""
                        SELECT quantity, last_updated 
                        FROM store_stock 
                        WHERE sku = %s AND store_id = %s
                    """, (sku, store_id))

                    result = cursor.fetchone()
                    if not result:
                        continue

                    current_qty, last_updated = result

                    # Only update if still at quantity 1 and hasn't been updated in last few minutes
                    if current_qty == 1:
                        if self.column_exists('store_stock', 'quantity_last_changed'):
                            cursor.execute("""
                                UPDATE store_stock 
                                SET quantity = 0, 
                                    last_updated = CURRENT_TIMESTAMP,
                                    quantity_last_changed = CURRENT_TIMESTAMP
                                WHERE sku = %s AND store_id = %s AND quantity = 1
                            """, (sku, store_id))
                        else:
                            cursor.execute("""
                                UPDATE store_stock 
                                SET quantity = 0, 
                                    last_updated = CURRENT_TIMESTAMP
                                WHERE sku = %s AND store_id = %s AND quantity = 1
                            """, (sku, store_id))

                        if cursor.rowcount > 0:
                            updated_count += 1
                            logger.info(f"Marked store {store_id} as out of stock (was stale at 1)")

                if updated_count > 0:
                    logger.info(f"Marked {updated_count} confirmed stale stores as out of stock for SKU {sku}")

                return updated_count
        except Exception as e:
            logger.error(f"Failed to mark stale stores as zero for SKU {sku}: {e}")
            return 0

    def _extract_quantity(self, store_data: Dict) -> int:
        """Extract available quantity from store data"""

        # First check if quantity is directly available (from command_handler.bestbuy_command)
        if 'quantity' in store_data:
            return store_data['quantity']

        # Check for pickup availability from API data structure
        locations = store_data.get('locations', [])
        for location in locations:
            if location.get('locationId') == store_data.get('id'):
                availability = location.get('availability', {})
                in_store = location.get('inStoreAvailability', {})

                # Check pickup quantity
                pickup_qty = availability.get('availablePickupQuantity', 0)
                if pickup_qty and pickup_qty != 9999:
                    return pickup_qty

                # Check in-store quantity
                instore_qty = in_store.get('availableInStoreQuantity', 0)
                if instore_qty and instore_qty != 9999:
                    return instore_qty

                # If 9999, treat as high stock
                if pickup_qty == 9999 or instore_qty == 9999:
                    return 999

        return 0

    def get_all_stores_for_sku(self, sku: str) -> List[Dict]:
        """Get all stored data for a specific SKU"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM store_stock 
                    WHERE sku = %s 
                    ORDER BY distance ASC
                """, (sku,))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get stores for SKU {sku}: {e}")
            return []

    def get_stores_within_radius_for_sku(self, sku: str, center_zip: str, radius_miles: float = 50) -> List[Dict]:
        """Get all stores for a SKU within a certain radius of a center ZIP code"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # First, we need to get all stores for this SKU and then filter by distance
                # Since we don't have lat/long coordinates, we'll use the distance field
                # which is calculated from the original API call
                cursor.execute("""
                    SELECT DISTINCT ON (store_id, zip_code) 
                           sku, store_id, store_name, address, city, state, zip_code, 
                           distance, quantity, last_updated
                    """, (sku,))

                # Add phone column if it exists
                if self.column_exists('store_stock', 'phone'):
                    cursor.execute("""
                        SELECT DISTINCT ON (store_id, zip_code) 
                               sku, store_id, store_name, address, city, state, zip_code, 
                               distance, quantity, last_updated, phone
                        FROM store_stock 
                        WHERE sku = %s 
                        AND distance <= %s
                        ORDER BY store_id, zip_code, distance ASC
                    """, (sku, radius_miles))
                else:
                    cursor.execute("""
                        SELECT DISTINCT ON (store_id, zip_code) 
                               sku, store_id, store_name, address, city, state, zip_code, 
                               distance, quantity, last_updated
                        FROM store_stock 
                        WHERE sku = %s 
                        AND distance <= %s
                        ORDER BY store_id, zip_code, distance ASC
                    """, (sku, radius_miles))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get stores within radius for SKU {sku}: {e}")
            return []

    def get_stores_for_sku_zip(self, sku: str, zip_code: str) -> List[Dict]:
        """Get all stored data for a specific SKU and ZIP code combination"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM store_stock 
                    WHERE sku = %s AND zip_code = %s
                    ORDER BY distance ASC
                """, (sku, zip_code))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get stores for SKU {sku}, ZIP {zip_code}: {e}")
            return []

    def get_stores_with_stock_near_zip(self, zip_code: str, monitored_skus: List[str]) -> List[Dict]:
        """Get all stores with stock > 0 near a ZIP code for monitored SKUs"""
        try:
            if not monitored_skus:
                return []

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Create placeholders for SKUs
                sku_placeholders = ','.join(['%s'] * len(monitored_skus))

                query = f"""
                    SELECT sku, store_id, store_name, address, city, state, zip_code, 
                           distance, quantity, last_updated
                """

                # Add phone column if it exists
                if self.column_exists('store_stock', 'phone'):
                    query += ", phone"

                query += f"""
                    FROM store_stock 
                    WHERE zip_code = %s 
                    AND sku IN ({sku_placeholders})
                    AND quantity > 0
                    ORDER BY sku, distance ASC
                """

                cursor.execute(query, [zip_code] + monitored_skus)

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get stores with stock for ZIP {zip_code}: {e}")
            return []

    def get_all_stores_near_zip(self, zip_code: str) -> List[Dict]:
        """Get all unique stores near a ZIP code regardless of SKU or stock"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT DISTINCT ON (store_id) 
                           store_id, store_name, address, city, state, zip_code, 
                           distance, last_updated
                """

                # Add phone column if it exists
                if self.column_exists('store_stock', 'phone'):
                    query += ", phone"

                query += """
                    FROM store_stock 
                    WHERE zip_code = %s
                    ORDER BY store_id, distance ASC
                """

                cursor.execute(query, (zip_code,))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get all stores for ZIP {zip_code}: {e}")
            return []

    def validate_zip_code_exists(self, zip_code: str) -> bool:
        """Check if a ZIP code exists in our database (has been queried before)"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(DISTINCT store_id) 
                    FROM store_stock 
                    WHERE zip_code = %s
                    LIMIT 1
                """, (zip_code,))

                result = cursor.fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Failed to validate ZIP code {zip_code}: {e}")
            return False

    def get_states_with_stock_changes(self, sku: str, stores_with_changes: List[Dict]) -> List[str]:
        """Get list of states that have stock changes for location tagging"""
        states = set()

        for store in stores_with_changes:
            state = store.get('state')
            if state:
                states.add(state)

        return list(states)

    def has_sku_been_seen(self, sku: str) -> bool:
        """Check if a SKU has ever been seen in the database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM store_stock WHERE sku = %s LIMIT 1
                """, (sku,))
                result = cursor.fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Failed to check if SKU {sku} exists: {e}")
            return False

    def get_stock_summary_by_location(self, zip_code: str, monitored_skus: List[str]) -> Dict:
        """Get a summary of stock by SKU for a specific location"""
        try:
            if not monitored_skus:
                return {}

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                sku_placeholders = ','.join(['%s'] * len(monitored_skus))

                cursor.execute(f"""
                    SELECT sku, 
                           COUNT(*) as total_stores,
                           COUNT(CASE WHEN quantity > 0 THEN 1 END) as stores_with_stock,
                           SUM(quantity) as total_quantity,
                           MAX(last_updated) as last_updated
                    FROM store_stock 
                    WHERE zip_code = %s 
                    AND sku IN ({sku_placeholders})
                    GROUP BY sku
                    ORDER BY stores_with_stock DESC, total_quantity DESC
                """, [zip_code] + monitored_skus)

                results = cursor.fetchall()

                summary = {}
                for row in results:
                    summary[row['sku']] = {
                        'total_stores': row['total_stores'],
                        'stores_with_stock': row['stores_with_stock'],
                        'total_quantity': row['total_quantity'],
                        'last_updated': row['last_updated']
                    }

                return summary
        except Exception as e:
            logger.error(f"Failed to get stock summary for ZIP {zip_code}: {e}")
            return {}

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")