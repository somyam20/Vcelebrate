import asyncio
import psycopg2
import psycopg2.extras
import json
from config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import logging

logger = logging.getLogger(__name__)


def get_conn():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]):
        raise RuntimeError("Database configuration incomplete")

    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )


# -------------------------
# ASYNC WRAPPERS
# -------------------------

async def async_init_db():
    return await asyncio.to_thread(init_db)


async def async_save_category_data(category: str, s3_url: str, headers: list, data: list, workbook: str = None, quarter: str = None):
    return await asyncio.to_thread(save_category_data, category, s3_url, headers, data, workbook, quarter)


async def async_get_category_data(category: str):
    return await asyncio.to_thread(get_category_data, category)


async def async_get_all_data():
    return await asyncio.to_thread(get_all_data)


async def async_update_inventory_quantity(location: str, workbook: str, quantity_change: int):
    return await asyncio.to_thread(update_inventory_quantity, location, workbook, quantity_change)


async def async_get_milestone_data():
    return await asyncio.to_thread(get_milestone_data)


async def async_get_inventory_data():
    return await asyncio.to_thread(get_inventory_data)


async def async_get_low_inventory_alerts(threshold: int = 40):
    return await asyncio.to_thread(get_low_inventory_alerts, threshold)


# -------------------------
# BLOCKING DATABASE FUNCTIONS
# -------------------------

def init_db():
    """Initialize database with category-wise tables"""
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Check if inventory_data table exists and has quarter column
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'inventory_data' AND column_name = 'quarter';
        """)
        
        has_quarter = cur.fetchone() is not None
        
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
                logger.info("→ Adding 'quarter' column to existing inventory_data table")
                cur.execute("""
                    ALTER TABLE inventory_data 
                    ADD COLUMN IF NOT EXISTS quarter TEXT;
                """)
                conn.commit()
                logger.info("✓ Successfully added 'quarter' column")
        
        # Projects table to track uploads (must be created first)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id SERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                s3_url TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT now()
            );
            """
        )
        
        # Milestone data table (dynamic columns)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS milestone_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT now(),
                CONSTRAINT fk_milestone_project FOREIGN KEY (project_id) 
                    REFERENCES projects(id) ON DELETE CASCADE
            );
            """
        )
        
        # Welcome kit data table (dynamic columns)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS welcome_kit_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT now(),
                CONSTRAINT fk_welcome_kit_project FOREIGN KEY (project_id) 
                    REFERENCES projects(id) ON DELETE CASCADE
            );
            """
        )
        
        # Inventory data table (dynamic columns) - updated to include quarter and workbook
        cur.execute(
            """
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
            """
        )
        
        # Create indexes for faster queries
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_milestone_data_project 
            ON milestone_data(project_id);
            """
        )
        
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_welcome_kit_data_project 
            ON welcome_kit_data(project_id);
            """
        )
        
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_inventory_data_location_workbook 
            ON inventory_data(location, workbook, quarter);
            """
        )
        
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_projects_category 
            ON projects(category);
            """
        )
        
        conn.commit()
        logger.info("✓ Database initialized successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Database initialization failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def save_category_data(category: str, s3_url: str, headers: list, data: list, workbook: str = None, quarter: str = None):
    """Save category data with dynamic headers"""
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Insert project record
        cur.execute(
            "INSERT INTO projects (category, s3_url) VALUES (%s, %s) RETURNING id",
            (category, s3_url)
        )
        project_id = cur.fetchone()[0]
        
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
        if category == "milestone":
            for row in data:
                row_data = {}
                for header, value in zip(headers, row):
                    cleaned_value = clean_value(value)
                    row_data[header] = cleaned_value
                
                cur.execute(
                    "INSERT INTO milestone_data (project_id, data) VALUES (%s, %s)",
                    (project_id, json.dumps(row_data, default=str))
                )
                
        elif category == "welcome_kit":
            for row in data:
                row_data = {}
                for header, value in zip(headers, row):
                    cleaned_value = clean_value(value)
                    row_data[header] = cleaned_value
                
                cur.execute(
                    "INSERT INTO welcome_kit_data (project_id, data) VALUES (%s, %s)",
                    (project_id, json.dumps(row_data, default=str))
                )
                
        elif category == "inventory":
            for row in data:
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
        
        conn.commit()
        logger.info(f"✓ Saved {len(data)} rows for category '{category}' (workbook: {workbook}, quarter: {quarter})")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to save category data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def get_category_data(category: str):
    """Retrieve all data for a specific category"""
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
        return rows
        
    finally:
        cur.close()
        conn.close()


def get_all_data():
    """Retrieve all data from all categories"""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        result = {
            "milestone": [],
            "welcome_kit": [],
            "inventory": []
        }
        
        # Get milestone data
        cur.execute("SELECT data FROM milestone_data ORDER BY id DESC")
        result["milestone"] = [json.loads(row['data']) if isinstance(row['data'], str) else row['data'] for row in cur.fetchall()]
        
        # Get welcome kit data
        cur.execute("SELECT data FROM welcome_kit_data ORDER BY id DESC")
        result["welcome_kit"] = [json.loads(row['data']) if isinstance(row['data'], str) else row['data'] for row in cur.fetchall()]
        
        # Get inventory data
        cur.execute("SELECT location, workbook, quarter, data FROM inventory_data ORDER BY id DESC")
        inventory_rows = cur.fetchall()
        result["inventory"] = []
        for row in inventory_rows:
            data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
            data['_location'] = row['location']
            data['_workbook'] = row['workbook']
            data['_quarter'] = row['quarter']
            result["inventory"].append(data)
        
        return result
        
    finally:
        cur.close()
        conn.close()


def get_milestone_data():
    """Get all milestone data for processing"""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        cur.execute("SELECT data FROM milestone_data ORDER BY id DESC")
        rows = cur.fetchall()
        return [json.loads(row['data']) if isinstance(row['data'], str) else row['data'] for row in rows]
        
    finally:
        cur.close()
        conn.close()


def get_inventory_data():
    """Get all inventory data"""
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
        return result
        
    finally:
        cur.close()
        conn.close()


def update_inventory_quantity(location: str, workbook: str, quantity_change: int):
    """Update inventory quantity for a specific location and workbook"""
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
            
            logger.info(f"✓ Updated {workbook} at {location}: {current_qty} → {new_qty} (change: {quantity_change})")
            return {"location": location, "workbook": workbook, "old_quantity": current_qty, "new_quantity": new_qty}
        else:
            logger.warning(f"⚠ Quantity column not found in inventory data")
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
        
        return alerts
        
    finally:
        cur.close()
        conn.close()