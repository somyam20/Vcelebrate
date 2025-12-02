import os
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# Import logging after load_dotenv
import logging
logger = logging.getLogger(__name__)

# DB configuration (individual parameters)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

logger.info(f"Database Configuration Loaded: Host={DB_HOST}, DB={DB_NAME}, Port={DB_PORT}")

# AWS configuration
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

logger.info(f"AWS Configuration Loaded: Bucket={AWS_S3_BUCKET}, Region={AWS_REGION}")

# Upload settings
S3_UPLOAD_PREFIX = os.getenv("S3_UPLOAD_PREFIX", "projects/")

# Category configuration
CATEGORIES = ["milestone", "welcome_kit", "inventory"]

# Milestone Data Field Mappings
DATE_OF_BIRTH_COL = os.getenv("DATE_OF_BIRTH_COL", "Date of Birth (as per Records)")
DATE_OF_BIRTH_MONTH_COL = os.getenv("DATE_OF_BIRTH_MONTH_COL", "MM Birth - WE Celebrate")
DATE_OF_MARRIAGE_COL = os.getenv("DATE_OF_MARRIAGE_COL", "Date of Marriage")
DATE_OF_JOINING_COL = os.getenv("DATE_OF_JOINING_COL", "Employment Details Date of Joining")
DATE_OF_JOINING_WE_COL = os.getenv("DATE_OF_JOINING_WE_COL", "DOJ - WE Celebrate")
DATE_OF_JOINING_MONTH_COL = os.getenv("DATE_OF_JOINING_MONTH_COL", "MM Service Completion - WE Celebrate")
LOCATION_COL = os.getenv("LOCATION_COL", "Place of posting")
LOCATION_ALT_COL = os.getenv("LOCATION_ALT_COL", "Base Location  Name")
EMPLOYEE_NAME_COL = os.getenv("EMPLOYEE_NAME_COL", "Full Name")

logger.info("Milestone field mappings loaded from environment")

# Inventory Workbook Names
INVENTORY_WORKBOOKS = {
    "birthday": os.getenv("INVENTORY_WORKBOOK_BIRTHDAY", "Birthday"),
    "anniversary": os.getenv("INVENTORY_WORKBOOK_ANNIVERSARY", "As on 03-10-25"),
    "service_completion": os.getenv("INVENTORY_WORKBOOK_SERVICE", "Service Completion")
}

logger.info(f"Inventory workbooks configured: {INVENTORY_WORKBOOKS}")

# Inventory Row Configuration - NEW
# These specify which rows contain the gift quantities in the "As On 03-10-25" workbook
BIRTHDAY_GIFTS_ROW = int(os.getenv("BIRTHDAY_GIFTS_ROW", "5"))  # 5th row (index 4)
ANNIVERSARY_GIFTS_ROW = int(os.getenv("ANNIVERSARY_GIFTS_ROW", "8"))  # 8th row (index 7)

logger.info(f"Gift quantity rows configured: Birthday Row={BIRTHDAY_GIFTS_ROW}, Anniversary Row={ANNIVERSARY_GIFTS_ROW}")

# Inventory Thresholds
LOW_INVENTORY_THRESHOLD = int(os.getenv("LOW_INVENTORY_THRESHOLD", "40"))

logger.info(f"Low inventory threshold set to: {LOW_INVENTORY_THRESHOLD}")

# Inventory Data Field Mappings
INVENTORY_LOCATION_COL = os.getenv("INVENTORY_LOCATION_COL", "Location")
INVENTORY_QUANTITY_COL = os.getenv("INVENTORY_QUANTITY_COL", "Quantity Received")
INVENTORY_WORKBOOK_COL = "_workbook"
INVENTORY_QUARTER_COL = "_quarter"

# Location Name Normalization
LOCATION_ALIASES = {
    "YASH IT Part": "Indore-YASH IT Park-SC-DC",
    "Indore YIT": "Indore-YASH IT Park-SC-DC",
    "YIT": "Indore-YASH IT Park-SC-DC",
    "Yash IT Park": "Indore-YASH IT Park-SC-DC",
    "YASH IT Park": "Indore-YASH IT Park-SC-DC",
    "Mindspace": "Hyderabad-Mindspace I-DC",
    "BTC": "Indore-BTC-CO",
    "Indore YIT": "Indore-YASH IT Park-SC-DC",
    "CIT": "Indore-YASH IT Park-SC-DC",
    "Hyd": "Hyderabad-Mindspace I-DC",
    "Magarpatta": "Pune",
    "MIDC": "Hyderabad-Mindspace I-DC",
    "BNG": "Bangalore",
    "Pune-Hinjewadi III-DC": "Pune",
    "Indore-Crystal IT Park-DC": "Indore-YASH IT Park-SC-DC",
    "Bangalore-Whitefield-DC": "Bangalore",
    "Bangalore-BHIVE-DC": "Bangalore"
}

# Scheduler configuration
INVENTORY_UPDATE_DAY = int(os.getenv("INVENTORY_UPDATE_DAY", "1"))
INVENTORY_UPDATE_HOUR = int(os.getenv("INVENTORY_UPDATE_HOUR", "0"))
SCHEDULER_CHECK_INTERVAL = int(os.getenv("SCHEDULER_CHECK_INTERVAL", "3600"))

logger.info(f"Scheduler configured: Update on day {INVENTORY_UPDATE_DAY} at {INVENTORY_UPDATE_HOUR}:00, Check interval={SCHEDULER_CHECK_INTERVAL}s")

# Validate critical configuration
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    logger.error("CRITICAL: Database configuration incomplete!")
    raise ValueError("Database configuration is incomplete. Check .env file.")

if not AWS_S3_BUCKET:
    logger.warning("AWS S3 bucket not configured")

logger.info("Configuration loaded successfully")