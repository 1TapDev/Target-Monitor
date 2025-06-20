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
                # Enhanced stock table for multi-retailer support
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS store_stock (
                        id SERIAL PRIMARY KEY,
                        retailer VARCHAR(20) DEFAULT 'target',
                        sku VARCHAR(20) NOT NULL,
                        store_id VARCHAR(10) NOT NULL,
                        store_name VARCHAR(255),
                        address VARCHAR(500),
                        city VARCHAR(100),
                        state VARCHAR(50),
                        zip_code VARCHAR(20),
                        distance FLOAT,
                        quantity INTEGER NOT NULL,
                        pickup_quantity INTEGER DEFAULT 0,
                        instore_quantity INTEGER DEFAULT 0,
                        phone VARCHAR(20),
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        quantity_last_changed TIMESTAMP,
                        UNIQUE(retailer, sku, store_id)
                    );
                """)

                # Initial stock reports tracking table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS initial_stock_reports (
                        id SERIAL PRIMARY KEY,
                        retailer VARCHAR(20) DEFAULT 'target',
                        sku VARCHAR(20) NOT NULL,
                        zip_code VARCHAR(10) NOT NULL,
                        reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(retailer, sku, zip_code)
                    );
                """)

                # Create enhanced indexes for multi-retailer support
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_retailer_sku_store 
                    ON store_stock(retailer, sku, store_id);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_retailer_initial_reports 
                    ON initial_stock_reports(retailer, sku, zip_code);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_retailer_zip_code 
                    ON store_stock(retailer, zip_code);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_retailer_state 
                    ON store_stock(retailer, state);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_target_quantity_changed 
                    ON store_stock(retailer, sku, store_id, quantity_last_changed);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_target_quantity_gt_zero 
                    ON store_stock(retailer, sku, zip_code) WHERE quantity > 0;
                """)

                # Legacy indexes for backward compatibility
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sku_store 
                    ON store_stock(sku, store_id);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_initial_reports 
                    ON initial_stock_reports(sku, zip_code);
                """)

                logger.info("Target database tables and indexes created/verified successfully")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def has_initial_report_been_sent(self, sku: str, zip_code: str, retailer: str = 'target') -> bool:
        """Check if an initial stock report has been sent for this retailer-SKU-ZIP combination"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM initial_stock_reports WHERE retailer = %s AND sku = %s AND zip_code = %s",
                    (retailer, sku, zip_code)
                )
                result = cursor.fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Failed to check initial report status for {retailer} {sku}, {zip_code}: {e}")
            return False

    def mark_initial_report_sent(self, sku: str, zip_code: str, retailer: str = 'target'):
        """Mark that an initial stock report has been sent for this retailer-SKU-ZIP combination"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO initial_stock_reports (retailer, sku, zip_code)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (retailer, sku, zip_code) DO NOTHING
                """, (retailer, sku, zip_code))
                logger.info(f"Marked initial report as sent for {retailer} SKU {sku}, ZIP {zip_code}")
        except Exception as e:
            logger.error(f"Failed to mark initial report for {retailer} {sku}, {zip_code}: {e}")

    def get_stores_for_sku_zip_optimized(self, sku: str, zip_code: str, retailer: str = 'target') -> List[Dict]:
        """Optimized version with better indexing for Target"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM store_stock 
                    WHERE retailer = %s AND sku = %s AND zip_code = %s 
                    AND last_updated > NOW() - INTERVAL '24 hours'
                    ORDER BY distance ASC
                    LIMIT 50
                """, (retailer, sku, zip_code))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get {retailer} stores for SKU {sku}, ZIP {zip_code}: {e}")
            return []

    def reset_initial_reports_for_sku(self, sku: str, retailer: str = 'target'):
        """Reset initial report flags for a specific retailer SKU"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM initial_stock_reports WHERE retailer = %s AND sku = %s",
                    (retailer, sku)
                )
                logger.info(f"Reset initial report flags for {retailer} SKU {sku}")
        except Exception as e:
            logger.error(f"Failed to reset initial reports for {retailer} {sku}: {e}")

    def get_previous_stock(self, sku: str, store_id: str, retailer: str = 'target') -> Optional[int]:
        """Get the previous stock quantity for a Target store"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT quantity FROM store_stock WHERE retailer = %s AND sku = %s AND store_id = %s",
                    (retailer, sku, store_id)
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get previous stock for {retailer} {sku}, {store_id}: {e}")
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

    def update_stock(self, sku: str, store_data: Dict, retailer: str = 'target') -> Tuple[bool, Optional[int]]:
        """
        Update stock for a Target store and return if it changed and previous quantity
        Returns: (changed, previous_quantity)
        """
        store_id = store_data.get('id')
        current_quantity = self._extract_quantity(store_data)
        pickup_quantity = store_data.get('pickup_quantity', 0)
        instore_quantity = store_data.get('instore_quantity', 0)

        previous_quantity = self.get_previous_stock(sku, store_id, retailer)

        # Handle None comparison safely
        if previous_quantity is None:
            changed = True  # First time seeing this store, consider it a change
            quantity_changed = True

            # Log as new store with Target context
            log_inventory_change(
                action="NEW_STORE",
                sku=sku,
                store_id=store_id,
                store_name=f"Target {store_data.get('name', '')}",
                prev_qty=0,
                new_qty=current_quantity,
                distance=store_data.get('distance', 0),
                city=store_data.get('city', ''),
                state=store_data.get('state', ''),
                zip_code=store_data.get('zipCode', ''),
                retailer=retailer
            )
        else:
            changed = previous_quantity != current_quantity
            quantity_changed = changed

            # Log inventory changes with Target context
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
                    action = "NO_CHANGE"

                log_inventory_change(
                    action=action,
                    sku=sku,
                    store_id=store_id,
                    store_name=f"Target {store_data.get('name', '')}",
                    prev_qty=previous_quantity,
                    new_qty=current_quantity,
                    distance=store_data.get('distance', 0),
                    city=store_data.get('city', ''),
                    state=store_data.get('state', ''),
                    zip_code=store_data.get('zipCode', ''),
                    retailer=retailer
                )

        # Database update with Target-specific fields
        try:
            with self.connection.cursor() as cursor:
                # DEBUG: Check data lengths before insert
                debug_data = {
                    'retailer': retailer,
                    'sku': sku,
                    'store_id': store_data.get('id', ''),
                    'store_name': store_data.get('name', ''),
                    'address': store_data.get('address', ''),
                    'city': store_data.get('city', ''),
                    'state': store_data.get('state', ''),
                    'zip_code': store_data.get('zipCode', ''),
                    'phone': store_data.get('phone', '')
                }

                # Log field lengths for debugging
                for field, value in debug_data.items():
                    if value and len(str(value)) > 50:  # Log long values
                        logger.warning(f"Long {field} value ({len(str(value))} chars): {str(value)[:100]}...")
                    elif field in ['state', 'zip_code'] and len(str(value)) > 10:
                        logger.warning(f"Potentially problematic {field} ({len(str(value))} chars): {value}")

                # Continue with your existing insert/update logic...
                if quantity_changed:
                    cursor.execute("""
                        INSERT INTO store_stock 
                        (retailer, sku, store_id, store_name, address, city, state, zip_code, 
                         distance, quantity, pickup_quantity, instore_quantity, phone, 
                         quantity_last_changed)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (retailer, sku, store_id) 
                        DO UPDATE SET 
                            store_name = EXCLUDED.store_name,
                            address = EXCLUDED.address,
                            city = EXCLUDED.city,
                            state = EXCLUDED.state,
                            zip_code = EXCLUDED.zip_code,
                            distance = EXCLUDED.distance,
                            quantity = EXCLUDED.quantity,
                            pickup_quantity = EXCLUDED.pickup_quantity,
                            instore_quantity = EXCLUDED.instore_quantity,
                            phone = EXCLUDED.phone,
                            last_updated = CURRENT_TIMESTAMP,
                            quantity_last_changed = CURRENT_TIMESTAMP
                    """, (
                        retailer, sku, store_data.get('id', ''), store_data.get('name', ''),
                        store_data.get('address', ''), store_data.get('city', ''),
                        store_data.get('state', ''), store_data.get('zipCode', ''),
                        store_data.get('distance', 0), current_quantity,
                        pickup_quantity, instore_quantity, store_data.get('phone', '')
                    ))
                else:
                    # Handle case when quantity didn't change
                    cursor.execute("""
                        INSERT INTO store_stock 
                        (retailer, sku, store_id, store_name, address, city, state, zip_code, 
                         distance, quantity, pickup_quantity, instore_quantity, phone)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (retailer, sku, store_id) 
                        DO UPDATE SET 
                            store_name = EXCLUDED.store_name,
                            address = EXCLUDED.address,
                            city = EXCLUDED.city,
                            state = EXCLUDED.state,
                            zip_code = EXCLUDED.zip_code,
                            distance = EXCLUDED.distance,
                            quantity = EXCLUDED.quantity,
                            pickup_quantity = EXCLUDED.pickup_quantity,
                            instore_quantity = EXCLUDED.instore_quantity,
                            phone = EXCLUDED.phone,
                            last_updated = CURRENT_TIMESTAMP
                    """, (
                        retailer, sku, store_data.get('id', ''), store_data.get('name', ''),
                        store_data.get('address', ''), store_data.get('city', ''),
                        store_data.get('state', ''), store_data.get('zipCode', ''),
                        store_data.get('distance', 0), current_quantity,
                        pickup_quantity, instore_quantity, store_data.get('phone', '')
                    ))
        except Exception as e:
            logger.error(f"Failed to update stock for {retailer} {sku}, {store_id}: {e}")
            # Log the problematic data for debugging
            logger.error(f"Problematic data: {debug_data}")

        return changed, previous_quantity

    def get_stock_first_seen_time(self, sku: str, store_id: str, retailer: str = 'target') -> Optional[datetime]:
        """Get the time when this Target item was first added to stock (quantity > 0)"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT quantity_last_changed 
                    FROM store_stock 
                    WHERE retailer = %s AND sku = %s AND store_id = %s AND quantity > 0
                """, (retailer, sku, store_id))

                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get first seen time for {retailer} {sku}, {store_id}: {e}")
            return None

    def get_stale_single_stock_stores(self, sku: str, hours: int = 1, retailer: str = 'target') -> List[str]:
        """Get Target store IDs that have had quantity=1 for more than specified hours"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT store_id, store_name, quantity_last_changed 
                    FROM store_stock 
                    WHERE retailer = %s AND sku = %s 
                    AND quantity = 1 
                    AND quantity_last_changed < NOW() - INTERVAL %s
                    AND last_updated > NOW() - INTERVAL '10 minutes'
                    AND NOT EXISTS (
                        SELECT 1 FROM store_stock s2 
                        WHERE s2.retailer = store_stock.retailer
                        AND s2.sku = store_stock.sku 
                        AND s2.store_id = store_stock.store_id 
                        AND s2.quantity = 0 
                        AND s2.last_updated > NOW() - INTERVAL '2 hours'
                    )
                """, (retailer, sku, f'{hours} hours'))

                stale_stores = cursor.fetchall()
                if stale_stores:
                    validated_stores = []
                    for store_id, store_name, last_changed in stale_stores:
                        cursor.execute("""
                            SELECT COUNT(DISTINCT quantity) as distinct_quantities
                            FROM store_stock 
                            WHERE retailer = %s AND sku = %s AND store_id = %s 
                            AND last_updated > NOW() - INTERVAL '6 hours'
                        """, (retailer, sku, store_id))

                        result = cursor.fetchone()
                        distinct_quantities = result[0] if result else 1

                        if distinct_quantities <= 2:
                            validated_stores.append(store_id)
                            logger.info(f"Validated stale Target store: {store_name} (ID: {store_id})")

                    if validated_stores:
                        logger.info(f"Found {len(validated_stores)} confirmed stale Target stores for SKU {sku}")
                        return validated_stores

                return []
        except Exception as e:
            logger.error(f"Failed to get stale stores for {retailer} SKU {sku}: {e}")
            return []

    def mark_stale_stores_as_zero(self, sku: str, store_ids: List[str], retailer: str = 'target') -> int:
        """Mark stale single-stock Target stores as having 0 quantity"""
        if not store_ids:
            return 0

        try:
            with self.connection.cursor() as cursor:
                updated_count = 0

                for store_id in store_ids:
                    cursor.execute("""
                        SELECT quantity, last_updated 
                        FROM store_stock 
                        WHERE retailer = %s AND sku = %s AND store_id = %s
                    """, (retailer, sku, store_id))

                    result = cursor.fetchone()
                    if not result:
                        continue

                    current_qty, last_updated = result

                    if current_qty == 1:
                        cursor.execute("""
                            UPDATE store_stock 
                            SET quantity = 0, 
                                pickup_quantity = 0,
                                instore_quantity = 0,
                                last_updated = CURRENT_TIMESTAMP,
                                quantity_last_changed = CURRENT_TIMESTAMP
                            WHERE retailer = %s AND sku = %s AND store_id = %s AND quantity = 1
                        """, (retailer, sku, store_id))

                        if cursor.rowcount > 0:
                            updated_count += 1
                            logger.info(f"Marked Target store {store_id} as out of stock (was stale at 1)")

                if updated_count > 0:
                    logger.info(f"Marked {updated_count} stale Target stores as out of stock for SKU {sku}")

                return updated_count
        except Exception as e:
            logger.error(f"Failed to mark stale stores as zero for {retailer} SKU {sku}: {e}")
            return 0

    def _extract_quantity(self, store_data: Dict) -> int:
        """Extract available quantity from Target store data with dual quantity support"""

        # First check if quantity is directly available (already processed)
        if 'quantity' in store_data:
            return store_data['quantity']

        # Target-specific: Handle pickup and in-store quantities
        pickup_qty = store_data.get('pickup_quantity', 0)
        instore_qty = store_data.get('instore_quantity', 0)

        # If we have Target-specific quantities, use the higher of the two
        if pickup_qty > 0 or instore_qty > 0:
            return max(pickup_qty, instore_qty)

        # Check for Target API availability data structure
        locations = store_data.get('locations', [])
        for location in locations:
            if location.get('locationId') == store_data.get('id'):
                availability = location.get('availability', {})
                in_store = location.get('inStoreAvailability', {})

                # Extract Target pickup quantity
                pickup_qty = availability.get('availablePickupQuantity', 0)

                # Extract Target in-store quantity
                instore_qty = in_store.get('availableInStoreQuantity', 0)

                # Handle Target's high stock indicator (9999)
                if pickup_qty == 9999 or instore_qty == 9999:
                    return 999  # Normalize to 999

                # Return the higher of pickup or in-store quantity
                if pickup_qty > 0 or instore_qty > 0:
                    # Store the individual quantities for database
                    store_data['pickup_quantity'] = pickup_qty
                    store_data['instore_quantity'] = instore_qty
                    return max(pickup_qty, instore_qty)

        return 0

    def get_all_stores_for_sku(self, sku: str, retailer: str = 'target') -> List[Dict]:
        """Get all stored data for a specific Target SKU"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM store_stock 
                    WHERE retailer = %s AND sku = %s 
                    ORDER BY distance ASC
                """, (retailer, sku))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get {retailer} stores for SKU {sku}: {e}")
            return []

    def get_stores_within_radius_for_sku(self, sku: str, center_zip: str, radius_miles: float = 50,
                                         retailer: str = 'target') -> List[Dict]:
        """Get all Target stores for a SKU within a certain radius"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT ON (store_id, zip_code) 
                           retailer, sku, store_id, store_name, address, city, state, zip_code, 
                           distance, quantity, pickup_quantity, instore_quantity, phone, last_updated
                    FROM store_stock 
                    WHERE retailer = %s AND sku = %s 
                    AND distance <= %s
                    ORDER BY store_id, zip_code, distance ASC
                """, (retailer, sku, radius_miles))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get {retailer} stores within radius for SKU {sku}: {e}")
            return []

    def get_stores_for_sku_zip(self, sku: str, zip_code: str, retailer: str = 'target') -> List[Dict]:
        """Get all stored data for a specific Target SKU and ZIP code combination"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM store_stock 
                    WHERE retailer = %s AND sku = %s AND zip_code = %s
                    ORDER BY distance ASC
                """, (retailer, sku, zip_code))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get {retailer} stores for SKU {sku}, ZIP {zip_code}: {e}")
            return []

    def get_stores_with_stock_near_zip(self, zip_code: str, monitored_skus: List[str], retailer: str = 'target') -> \
    List[Dict]:
        """Get all Target stores with stock > 0 near a ZIP code for monitored SKUs"""
        try:
            if not monitored_skus:
                return []

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                sku_placeholders = ','.join(['%s'] * len(monitored_skus))

                query = f"""
                    SELECT retailer, sku, store_id, store_name, address, city, state, zip_code, 
                           distance, quantity, pickup_quantity, instore_quantity, phone, last_updated
                    FROM store_stock 
                    WHERE retailer = %s AND zip_code = %s 
                    AND sku IN ({sku_placeholders})
                    AND quantity > 0
                    ORDER BY sku, distance ASC
                """

                cursor.execute(query, [retailer, zip_code] + monitored_skus)

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get {retailer} stores with stock for ZIP {zip_code}: {e}")
            return []

    def get_all_stores_near_zip(self, zip_code: str, retailer: str = 'target') -> List[Dict]:
        """Get all unique Target stores near a ZIP code regardless of SKU or stock"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT ON (store_id) 
                           retailer, store_id, store_name, address, city, state, zip_code, 
                           distance, phone, last_updated
                    FROM store_stock 
                    WHERE retailer = %s AND zip_code = %s
                    ORDER BY store_id, distance ASC
                """, (retailer, zip_code))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get all {retailer} stores for ZIP {zip_code}: {e}")
            return []

    def validate_zip_code_exists(self, zip_code: str, retailer: str = 'target') -> bool:
        """Check if a ZIP code exists in our Target database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(DISTINCT store_id) 
                    FROM store_stock 
                    WHERE retailer = %s AND zip_code = %s
                    LIMIT 1
                """, (retailer, zip_code))

                result = cursor.fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Failed to validate ZIP code {zip_code} for {retailer}: {e}")
            return False

    def get_states_with_stock_changes(self, sku: str, stores_with_changes: List[Dict]) -> List[str]:
        """Get list of states that have stock changes for location tagging"""
        states = set()

        for store in stores_with_changes:
            state = store.get('state')
            if state:
                states.add(state)

        return list(states)

    def has_sku_been_seen(self, sku: str, retailer: str = 'target') -> bool:
        """Check if a Target SKU has ever been seen in the database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM store_stock 
                    WHERE retailer = %s AND sku = %s LIMIT 1
                """, (retailer, sku))
                result = cursor.fetchone()
                return result[0] > 0 if result else False
        except Exception as e:
            logger.error(f"Failed to check if {retailer} SKU {sku} exists: {e}")
            return False

    def get_stock_summary_by_location(self, zip_code: str, monitored_skus: List[str], retailer: str = 'target') -> Dict:
        """Get a summary of Target stock by SKU for a specific location"""
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
                           SUM(pickup_quantity) as total_pickup_quantity,
                           SUM(instore_quantity) as total_instore_quantity,
                           MAX(last_updated) as last_updated
                    FROM store_stock 
                    WHERE retailer = %s AND zip_code = %s 
                    AND sku IN ({sku_placeholders})
                    GROUP BY sku
                    ORDER BY stores_with_stock DESC, total_quantity DESC
                """, [retailer, zip_code] + monitored_skus)

                results = cursor.fetchall()

                summary = {}
                for row in results:
                    summary[row['sku']] = {
                        'total_stores': row['total_stores'],
                        'stores_with_stock': row['stores_with_stock'],
                        'total_quantity': row['total_quantity'],
                        'total_pickup_quantity': row['total_pickup_quantity'],
                        'total_instore_quantity': row['total_instore_quantity'],
                        'last_updated': row['last_updated']
                    }

                return summary
        except Exception as e:
            logger.error(f"Failed to get stock summary for {retailer} ZIP {zip_code}: {e}")
            return {}

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")