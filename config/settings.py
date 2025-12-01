import os
from dotenv import load_dotenv
load_dotenv()

# DB configuration (individual parameters)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

# AWS configuration
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Upload settings
S3_UPLOAD_PREFIX = os.getenv("S3_UPLOAD_PREFIX", "projects/")

# Category configuration
CATEGORIES = ["milestone", "welcome_kit", "inventory"]

# ============================================================================
# MILESTONE DATA FIELD MAPPINGS
# ============================================================================
# These are the column names we look for in the milestone data
# Updated based on your actual data structure

# Date fields - primary (full dates)
DATE_OF_BIRTH_COL = "Date of Birth (as per Records)"
DATE_OF_MARRIAGE_COL = "Date of Marriage"
DATE_OF_JOINING_COL = "Employment Details Date of Joining"

# Date fields - pre-calculated month fields (preferred for calculations)
DATE_OF_BIRTH_MONTH_COL = "MM Birth - WE Celebrate"
DATE_OF_JOINING_MONTH_COL = "MM Service Completion - WE Celebrate"

# DOJ - WE Celebrate is used for service completion anniversaries
DOJ_CELEBRATE_COL = "DOJ - WE Celebrate"

# Location fields (try both)
LOCATION_COL = "Base Location  Name"  # Primary location field
LOCATION_ALT_COL = "Place of posting"  # Alternative location field

# Employee identification
EMPLOYEE_NAME_COL = "Full Name"
EMPLOYEE_ID_COL = "User/Employee ID"

# ============================================================================
# INVENTORY DATA FIELD MAPPINGS
# ============================================================================

# Inventory workbook names (worksheet names in Excel)
INVENTORY_WORKBOOKS = {
    "birthday": "Birthday",
    "anniversary": "As on 03-10-25",  # Anniversary workbook
    "service_completion": "Service Completion"
}

# Inventory column names
INVENTORY_LOCATION_COL = "Location"
INVENTORY_QUANTITY_COL = "Quantity Received"
INVENTORY_WORKBOOK_COL = "_workbook"  # Internal field added during parsing
INVENTORY_QUARTER_COL = "_quarter"    # Internal field added during parsing

# Low inventory threshold (alert when below this number)
LOW_INVENTORY_THRESHOLD = 40

# ============================================================================
# LOCATION NAME NORMALIZATION
# ============================================================================
# Map common location name variations to standard names
# This helps match user queries with database values

LOCATION_ALIASES = {
    "YASH IT Part": "Indore-YASH IT Park-SC-DC",
    "YIT": "Indore-YASH IT Park-SC-DC",
    "Yash IT Park": "Indore-YASH IT Park-SC-DC",
    "YASH IT Park": "Indore-YASH IT Park-SC-DC",
    "Mindspace": "Hyderabad-Mindspace I-DC",
    "BTC": "Indore-BTC-CO",
    # Add more aliases as needed based on user queries
}

# ============================================================================
# SCHEDULER SETTINGS
# ============================================================================

# When to run monthly inventory updates
INVENTORY_UPDATE_DAY = 1      # 1st of each month
INVENTORY_UPDATE_HOUR = 0     # At midnight (00:00)
SCHEDULER_CHECK_INTERVAL = 3600  # Check every hour (in seconds)