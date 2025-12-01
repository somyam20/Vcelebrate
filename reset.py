"""
Script to drop existing tables and recreate them fresh.
Run this once to fix the foreign key constraint issue.

Usage: python reset_database.py
"""
import psycopg2
from config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def reset_database():
    """Drop all tables and recreate them with correct schema"""
    
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    
    cur = conn.cursor()
    
    try:
        logger.info("=" * 80)
        logger.info("RESETTING DATABASE")
        logger.info("=" * 80)
        
        # Drop all tables in reverse order (dependent tables first)
        logger.info("Step 1: Dropping existing tables...")
        
        cur.execute("DROP TABLE IF EXISTS milestone_data CASCADE;")
        logger.info("  ✓ Dropped milestone_data")
        
        cur.execute("DROP TABLE IF EXISTS welcome_kit_data CASCADE;")
        logger.info("  ✓ Dropped welcome_kit_data")
        
        cur.execute("DROP TABLE IF EXISTS inventory_data CASCADE;")
        logger.info("  ✓ Dropped inventory_data")
        
        cur.execute("DROP TABLE IF EXISTS projects CASCADE;")
        logger.info("  ✓ Dropped projects")
        
        conn.commit()
        logger.info("✓ All tables dropped successfully")
        
        # Now create tables fresh
        logger.info("\nStep 2: Creating tables with correct schema...")
        
        # Create projects table
        cur.execute(
            """
            CREATE TABLE projects (
                id SERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                s3_url TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT now()
            );
            """
        )
        logger.info("  ✓ Created projects table")
        
        # Create milestone_data table
        cur.execute(
            """
            CREATE TABLE milestone_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            );
            """
        )
        logger.info("  ✓ Created milestone_data table")
        
        # Create welcome_kit_data table
        cur.execute(
            """
            CREATE TABLE welcome_kit_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
                data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            );
            """
        )
        logger.info("  ✓ Created welcome_kit_data table")
        
        # Create inventory_data table
        cur.execute(
            """
            CREATE TABLE inventory_data (
                id SERIAL PRIMARY KEY,
                project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
                location TEXT,
                workbook TEXT,
                data JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT now()
            );
            """
        )
        logger.info("  ✓ Created inventory_data table")
        
        # Create indexes
        logger.info("\nStep 3: Creating indexes...")
        
        cur.execute(
            """
            CREATE INDEX idx_milestone_data_project 
            ON milestone_data(project_id);
            """
        )
        logger.info("  ✓ Created index on milestone_data.project_id")
        
        cur.execute(
            """
            CREATE INDEX idx_welcome_kit_data_project 
            ON welcome_kit_data(project_id);
            """
        )
        logger.info("  ✓ Created index on welcome_kit_data.project_id")
        
        cur.execute(
            """
            CREATE INDEX idx_inventory_data_location_workbook 
            ON inventory_data(location, workbook);
            """
        )
        logger.info("  ✓ Created index on inventory_data(location, workbook)")
        
        cur.execute(
            """
            CREATE INDEX idx_projects_category 
            ON projects(category);
            """
        )
        logger.info("  ✓ Created index on projects.category")
        
        conn.commit()
        
        logger.info("=" * 80)
        logger.info("✓ DATABASE RESET COMPLETE")
        logger.info("=" * 80)
        logger.info("\nAll tables have been recreated successfully!")
        logger.info("You can now run your main application.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Database reset failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        reset_database()
    except Exception as e:
        logger.error(f"Failed to reset database: {e}")
        exit(1)