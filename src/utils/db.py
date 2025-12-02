import asyncio
import psycopg2
import psycopg2.extras
import json
from config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import logging

logger = logging.getLogger(__name__)


def get_conn():
    """Get database connection with logging"""
    logger.debug(f"Establishing database connection to {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]):
        logger.error("Database configuration incomplete!")
        logger.error(f"DB_HOST: {DB_HOST}, DB_NAME: {DB_NAME}, DB_USER: {DB_USER}, DB_PORT: {DB_PORT}")
        raise RuntimeError("Database configuration incomplete")

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logger.debug("✓ Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        raise


# -------------------------
# ASYNC WRAPPERS
# -------------------------

async def async_init_db():
    logger.info("Initializing database (async)")
    return await asyncio.to_thread(init_db)


async def async_save_category_data(category: str, s3_url: str, headers: list, data: list, workbook: str = None, quarter: str = None):
    logger.info(f"Saving category data (async): category={category}, workbook={workbook}, rows={len(data)}")
    return await asyncio.to_thread(save_category_data, category, s3_url, headers, data, workbook, quarter)


async def async_get_category_data(category: str):
    logger.info(f"Fetching category data (async): category={category}")
    return await asyncio.to_thread(get_category_data, category)


async def async_get_all_data():
    logger.info("Fetching all data (async)")
    return await asyncio.to_thread(get_all_data)


async def async_update_inventory_quantity(location: str, workbook: str, quantity_change: int):
    logger.info(f"Updating inventory (async): location={location}, workbook={workbook}, change={quantity_change}")
    return await asyncio.to_thread(update_inventory_quantity, location, workbook, quantity_change)


async def async_get_milestone_data():
    logger.info("Fetching milestone data (async)")
    return await asyncio.to_thread(get_milestone_data)


async def async_get_inventory_data():
    logger.info("Fetching inventory data (async)")
    return await asyncio.to_thread(get_inventory_data)


async def async_get_low_inventory_alerts(threshold: int = 40):
    logger.info(f"Fetching low inventory alerts (async): threshold={threshold}")
    return await asyncio.to_thread(get_low_inventory_alerts, threshold)


# -------------------------
# BLOCKING DATABASE FUNCTIONS
# -------------------------

def init_db():
    """Initialize database with category-wise tables"""
    logger.info("=" * 80)
    logger.info("DATABASE INITIALIZATION START")
    logger.info("=" * 80)
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Check if inventory_data table exists and has quarter column
        logger.info("Checking for existing tables and columns...")
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'inventory_data' AND column_name = 'quarter';
        """)
        
        has_quarter = cur.fetchone() is not None
        logger.info(f"Quarter column exists: {has_quarter}")
        
        # If table exists but doesn't have quarter column, add it
        if not has_quarter:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'inventory_data'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                logger.info("Adding 'quarter' column to existing inventory_data table")
                cur.execute("""
                    ALTER TABLE inventory_data 
                    ADD COLUMN IF NOT EXISTS quarter TEXT;
                """)
                conn.commit()
                logger.info("✓ Successfully added 'quarter' column")
        
        # Projects table
        logger.info("Creating/verifying 'projects' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                id SERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                s3_url TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT now()
            );
        """)
        logger.info("✓ Projects table ready")
        
        # Milestone data table
        logger.info("Creating/verifying 'milestone_data' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS milestone_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT now(),
                CONSTRAINT fk_milestone_project FOREIGN KEY (project_id) 
                    REFERENCES projects(id) ON DELETE CASCADE
            );
        """)
        logger.info("✓ Milestone data table ready")
        
        # Welcome kit data table
        logger.info("Creating/verifying 'welcome_kit_data' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS welcome_kit_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT now(),
                CONSTRAINT fk_welcome_kit_project FOREIGN KEY (project_id) 
                    REFERENCES projects(id) ON DELETE CASCADE
            );
        """)
        logger.info("✓ Welcome kit data table ready")
        
        # Inventory data table
        logger.info("Creating/verifying 'inventory_data' table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inventory_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER,
                location TEXT,
                workbook TEXT,
                quarter TEXT,
                data JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT now(),
                CONSTRAINT fk_inventory_project FOREIGN KEY (project_id) 
                    REFERENCES projects(id) ON DELETE CASCADE
            );
        """)
        logger.info("✓ Inventory data table ready")
        
        # Create indexes
        logger.info("Creating/verifying indexes...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_milestone_data_project 
            ON milestone_data(project_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_welcome_kit_data_project 
            ON welcome_kit_data(project_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inventory_data_location_workbook 
            ON inventory_data(location, workbook, quarter);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_projects_category 
            ON projects(category);
        """)
        logger.info("✓ All indexes ready")
        
        conn.commit()
        logger.info("=" * 80)
        logger.info("✓ DATABASE INITIALIZATION COMPLETE")
        logger.info("=" * 80)
        
    except Exception as e:
        conn.rollback()
        logger.error("=" * 80)
        logger.error("✗ DATABASE INITIALIZATION FAILED")
        logger.exception(f"Error: {e}")
        logger.error("=" * 80)
        raise
    finally:
        cur.close()
        conn.close()


def save_category_data(category: str, s3_url: str, headers: list, data: list, workbook: str = None, quarter: str = None):
    """Save category data with dynamic headers"""
    logger.info("-" * 80)
    logger.info(f"SAVING CATEGORY DATA: {category}")
    logger.info(f"S3 URL: {s3_url}")
    logger.info(f"Workbook: {workbook}")
    logger.info(f"Quarter: {quarter}")
    logger.info(f"Headers: {len(headers)} columns")
    logger.info(f"Data: {len(data)} rows")
    logger.info("-" * 80)
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Insert project record
        logger.debug(f"Inserting project record for category '{category}'")
        cur.execute(
            "INSERT INTO projects (category, s3_url) VALUES (%s, %s) RETURNING id",
            (category, s3_url)
        )
        project_id = cur.fetchone()[0]
        logger.info(f"✓ Created project ID: {project_id}")
        
        # Helper function to clean values for JSON serialization
        def clean_value(val):
            """Clean value for JSON serialization"""
            if val is None:
                return None
            if isinstance(val, float):
                import math
                if math.isnan(val) or math.isinf(val):
                    return None
            if isinstance(val, str):
                val = val.strip()
                if val.lower() in ['nan', 'nat', 'none', '']:
                    return None
            return val
        
        # Save data based on category
        rows_saved = 0
        if category == "milestone":
            logger.info("Saving milestone data...")
            for idx, row in enumerate(data):
                row_data = {}
                for header, value in zip(headers, row):
                    cleaned_value = clean_value(value)
                    row_data[header] = cleaned_value
                
                cur.execute(
                    "INSERT INTO milestone_data (project_id, data) VALUES (%s, %s)",
                    (project_id, json.dumps(row_data, default=str))
                )
                rows_saved += 1
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Saved {idx + 1}/{len(data)} milestone rows...")
                
        elif category == "welcome_kit":
            logger.info("Saving welcome kit data...")
            for idx, row in enumerate(data):
                row_data = {}
                for header, value in zip(headers, row):
                    cleaned_value = clean_value(value)
                    row_data[header] = cleaned_value
                
                cur.execute(
                    "INSERT INTO welcome_kit_data (project_id, data) VALUES (%s, %s)",
                    (project_id, json.dumps(row_data, default=str))
                )
                rows_saved += 1
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Saved {idx + 1}/{len(data)} welcome kit rows...")
                
        elif category == "inventory":
            logger.info(f"Saving inventory data for workbook '{workbook}'...")
            for idx, row in enumerate(data):
                row_data = {}
                for header, value in zip(headers, row):
                    cleaned_value = clean_value(value)
                    row_data[header] = cleaned_value
                
                # Extract location (from "Location" column)
                location = None
                for header, value in zip(headers, row):
                    if "location" in header.lower():
                        location = clean_value(value)
                        break
                
                if not location:
                    location = "Unknown"
                
                cur.execute(
                    """
                    INSERT INTO inventory_data (project_id, location, workbook, quarter, data) 
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (project_id, location, workbook or "Unknown", quarter or "Unknown", json.dumps(row_data, default=str))
                )
                rows_saved += 1
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Saved {idx + 1}/{len(data)} inventory rows...")
        
        conn.commit()
        logger.info(f"✓ Successfully saved {rows_saved} rows for category '{category}' (workbook: {workbook})")
        logger.info("-" * 80)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to save category data: {e}")
        logger.exception("Full error details:")
        raise
    finally:
        cur.close()
        conn.close()


def get_category_data(category: str):
    """Retrieve all data for a specific category"""
    logger.info(f"Retrieving data for category: {category}")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        if category == "milestone":
            cur.execute("SELECT data FROM milestone_data ORDER BY id DESC")
        elif category == "welcome_kit":
            cur.execute("SELECT data FROM welcome_kit_data ORDER BY id DESC")
        elif category == "inventory":
            cur.execute("SELECT location, workbook, quarter, data FROM inventory_data ORDER BY id DESC")
        else:
            raise ValueError(f"Invalid category: {category}")
        
        rows = cur.fetchall()
        logger.info(f"✓ Retrieved {len(rows)} rows for category '{category}'")
        return rows
        
    except Exception as e:
        logger.error(f"✗ Failed to retrieve data for category '{category}': {e}")
        raise
    finally:
        cur.close()
        conn.close()


def get_all_data():
    """Retrieve all data from all categories"""
    logger.info("Retrieving ALL data from all categories...")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        result = {
            "milestone": [],
            "welcome_kit": [],
            "inventory": []
        }
        
        # Get milestone data
        logger.debug("Fetching milestone data...")
        cur.execute("SELECT data FROM milestone_data ORDER BY id DESC")
        result["milestone"] = [json.loads(row['data']) if isinstance(row['data'], str) else row['data'] for row in cur.fetchall()]
        logger.info(f"✓ Retrieved {len(result['milestone'])} milestone records")
        
        # Get welcome kit data
        logger.debug("Fetching welcome kit data...")
        cur.execute("SELECT data FROM welcome_kit_data ORDER BY id DESC")
        result["welcome_kit"] = [json.loads(row['data']) if isinstance(row['data'], str) else row['data'] for row in cur.fetchall()]
        logger.info(f"✓ Retrieved {len(result['welcome_kit'])} welcome kit records")
        
        # Get inventory data
        logger.debug("Fetching inventory data...")
        cur.execute("SELECT location, workbook, quarter, data FROM inventory_data ORDER BY id DESC")
        inventory_rows = cur.fetchall()
        result["inventory"] = []
        for row in inventory_rows:
            data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
            data['_location'] = row['location']
            data['_workbook'] = row['workbook']
            data['_quarter'] = row['quarter']
            result["inventory"].append(data)
        logger.info(f"✓ Retrieved {len(result['inventory'])} inventory records")
        
        logger.info(f"✓ Total records retrieved: {sum(len(v) for v in result.values())}")
        return result
        
    except Exception as e:
        logger.error(f"✗ Failed to retrieve all data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def get_milestone_data():
    """Get all milestone data for processing"""
    logger.info("Fetching milestone data for processing...")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        cur.execute("SELECT data FROM milestone_data ORDER BY id DESC")
        rows = cur.fetchall()
        result = [json.loads(row['data']) if isinstance(row['data'], str) else row['data'] for row in rows]
        logger.info(f"✓ Retrieved {len(result)} milestone records")
        return result
        
    except Exception as e:
        logger.error(f"✗ Failed to retrieve milestone data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def get_inventory_data():
    """Get all inventory data"""
    logger.info("Fetching inventory data...")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        cur.execute("SELECT location, workbook, quarter, data FROM inventory_data ORDER BY id DESC")
        rows = cur.fetchall()
        result = []
        for row in rows:
            data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
            result.append({
                'location': row['location'],
                'workbook': row['workbook'],
                'quarter': row['quarter'],
                'data': data
            })
        logger.info(f"✓ Retrieved {len(result)} inventory records")
        return result
        
    except Exception as e:
        logger.error(f"✗ Failed to retrieve inventory data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def update_inventory_quantity(location: str, workbook: str, quantity_change: int):
    """Update inventory quantity for a specific location and workbook"""
    logger.info(f"Updating inventory: location='{location}', workbook='{workbook}', change={quantity_change}")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        # Get current inventory data
        cur.execute(
            """
            SELECT id, data FROM inventory_data 
            WHERE location = %s AND workbook = %s
            ORDER BY id DESC LIMIT 1
            """,
            (location, workbook)
        )
        
        row = cur.fetchone()
        if not row:
            logger.warning(f"⚠ No inventory found for location='{location}', workbook='{workbook}'")
            return None
        
        inventory_id = row['id']
        data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
        
        # Update quantity (find "Quantity Received" or similar column)
        quantity_col = None
        for key in data.keys():
            if "quantity" in key.lower() and "received" in key.lower():
                quantity_col = key
                break
        
        if quantity_col:
            current_qty = int(data.get(quantity_col, 0)) if data.get(quantity_col) else 0
            new_qty = max(0, current_qty + quantity_change)
            data[quantity_col] = new_qty
            
            # Update in database
            cur.execute(
                """
                UPDATE inventory_data 
                SET data = %s, updated_at = now() 
                WHERE id = %s
                """,
                (json.dumps(data), inventory_id)
            )
            conn.commit()
            
            logger.info(f"✓ Updated {workbook} at {location}: {current_qty} → {new_qty} (change: {quantity_change:+d})")
            return {"location": location, "workbook": workbook, "old_quantity": current_qty, "new_quantity": new_qty}
        else:
            logger.warning(f"⚠ Quantity column not found in inventory data for {location}, {workbook}")
            return None
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to update inventory: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def get_low_inventory_alerts(threshold: int = 40):
    """Get inventory items that are below the threshold"""
    logger.info(f"Checking for low inventory (threshold: {threshold})...")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        cur.execute("SELECT location, workbook, quarter, data FROM inventory_data ORDER BY id DESC")
        rows = cur.fetchall()
        
        alerts = []
        for row in rows:
            data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
            
            # Find quantity column
            quantity_col = None
            for key in data.keys():
                if "quantity" in key.lower() and "received" in key.lower():
                    quantity_col = key
                    break
            
            if quantity_col:
                quantity = int(data.get(quantity_col, 0)) if data.get(quantity_col) else 0
                if quantity < threshold:
                    alerts.append({
                        'location': row['location'],
                        'workbook': row['workbook'],
                        'quarter': row['quarter'],
                        'current_quantity': quantity,
                        'threshold': threshold
                    })
        
        if alerts:
            logger.warning(f"⚠ Found {len(alerts)} low inventory items (below {threshold})")
            for alert in alerts:
                logger.warning(f"  - {alert['workbook']} at {alert['location']}: {alert['current_quantity']} units")
        else:
            logger.info(f"✓ No low inventory alerts")
        
        return alerts
        
    except Exception as e:
        logger.error(f"✗ Failed to check low inventory: {e}")
        raise
    finally:
        cur.close()
        conn.close()