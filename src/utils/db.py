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


async def async_save_category_data(category: str, s3_url: str, headers: list, data: list):
    return await asyncio.to_thread(save_category_data, category, s3_url, headers, data)


async def async_get_category_data(category: str):
    return await asyncio.to_thread(get_category_data, category)


async def async_update_inventory_quantity(location: str, workbook: str, quantity_change: int):
    return await asyncio.to_thread(update_inventory_quantity, location, workbook, quantity_change)


async def async_get_milestone_data():
    return await asyncio.to_thread(get_milestone_data)


# -------------------------
# BLOCKING DATABASE FUNCTIONS
# -------------------------

def init_db():
    """Initialize database with category-wise tables"""
    conn = get_conn()
    cur = conn.cursor()
    
    # Projects table to track uploads
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
            project_id INTEGER REFERENCES projects(id),
            data JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );
        """
    )
    
    # Welcome kit data table (dynamic columns)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS welcome_kit_data (
            id SERIAL PRIMARY KEY,
            project_id INTEGER REFERENCES projects(id),
            data JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );
        """
    )
    
    # Inventory data table (dynamic columns)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_data (
            id SERIAL PRIMARY KEY,
            project_id INTEGER REFERENCES projects(id),
            location TEXT,
            workbook TEXT,
            data JSONB NOT NULL,
            updated_at TIMESTAMP DEFAULT now()
        );
        """
    )
    
    # Create indexes for faster queries
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_milestone_data_project 
        ON milestone_data(project_id);
        
        CREATE INDEX IF NOT EXISTS idx_welcome_kit_data_project 
        ON welcome_kit_data(project_id);
        
        CREATE INDEX IF NOT EXISTS idx_inventory_data_location_workbook 
        ON inventory_data(location, workbook);
        
        CREATE INDEX IF NOT EXISTS idx_projects_category 
        ON projects(category);
        """
    )
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✓ Database initialized successfully")


def save_category_data(category: str, s3_url: str, headers: list, data: list):
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
        
        # Save data based on category
        if category == "milestone":
            for row in data:
                row_data = dict(zip(headers, row))
                cur.execute(
                    "INSERT INTO milestone_data (project_id, data) VALUES (%s, %s)",
                    (project_id, json.dumps(row_data))
                )
                
        elif category == "welcome_kit":
            for row in data:
                row_data = dict(zip(headers, row))
                cur.execute(
                    "INSERT INTO welcome_kit_data (project_id, data) VALUES (%s, %s)",
                    (project_id, json.dumps(row_data))
                )
                
        elif category == "inventory":
            for row in data:
                row_data = dict(zip(headers, row))
                # Extract location and workbook for indexing
                location = row_data.get("Location", "Unknown")
                # Assuming first column after location contains workbook info
                workbook = list(row_data.keys())[1] if len(row_data) > 1 else "Unknown"
                
                cur.execute(
                    """
                    INSERT INTO inventory_data (project_id, location, workbook, data) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (project_id, location, workbook, json.dumps(row_data))
                )
        
        conn.commit()
        logger.info(f"✓ Saved {len(data)} rows for category '{category}'")
        
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
            cur.execute("SELECT location, workbook, data FROM inventory_data ORDER BY id DESC")
        else:
            raise ValueError(f"Invalid category: {category}")
        
        rows = cur.fetchall()
        return rows
        
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
        return [json.loads(row['data']) for row in rows]
        
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
            return
        
        inventory_id = row['id']
        data = json.loads(row['data'])
        
        # Update quantity (assuming it's in "Quantity Received" or similar column)
        quantity_col = None
        for key in data.keys():
            if "quantity" in key.lower() or "received" in key.lower():
                quantity_col = key
                break
        
        if quantity_col:
            current_qty = int(data.get(quantity_col, 0))
            new_qty = max(0, current_qty + quantity_change)  # Ensure non-negative
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
        else:
            logger.warning(f"⚠ Quantity column not found in inventory data")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to update inventory: {e}")
        raise
    finally:
        cur.close()
        conn.close()