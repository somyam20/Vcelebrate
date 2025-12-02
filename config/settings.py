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

# Milestone Data Field Mappings
# These are the column names we look for in the milestone data
DATE_OF_BIRTH_COL = "Date of Birth (as per Records)"
DATE_OF_BIRTH_MONTH_COL = "MM Birth - WE Celebrate"  # Pre-calculated month field (1-12)
DATE_OF_MARRIAGE_COL = "Date of Marriage"
DATE_OF_JOINING_COL = "Employment Details Date of Joining"
DATE_OF_JOINING_WE_COL = "DOJ - WE Celebrate"  # WE Celebrate date
DATE_OF_JOINING_MONTH_COL = "MM Service Completion - WE Celebrate"  # Pre-calculated month field (1-12)
LOCATION_COL = "Place of posting"
LOCATION_ALT_COL = "Base Location  Name"  # Alternate location field (note the double space)
EMPLOYEE_NAME_COL = "Full Name"

# Inventory Workbook Names
# These match the worksheet names in the inventory Excel file
INVENTORY_WORKBOOKS = {
    "birthday": "Birthday",
    "anniversary": "As on 03-10-25",  # Anniversary workbook name (note: lowercase 'on')
    "service_completion": "Service Completion"
}

# Inventory Thresholds
LOW_INVENTORY_THRESHOLD = 40  # Alert when inventory goes below this number

# Inventory Data Field Mappings
INVENTORY_LOCATION_COL = "Location"
INVENTORY_QUANTITY_COL = "Quantity Received"
INVENTORY_WORKBOOK_COL = "_workbook"  # Internal field added during parsing
INVENTORY_QUARTER_COL = "_quarter"  # Internal field added during parsing

# Location Name Normalization
# Map common location name variations to standard names
LOCATION_ALIASES = {
    "YASH IT Part": "Indore-YASH IT Park-SC-DC",
    "YIT": "Indore-YASH IT Park-SC-DC",
    "Yash IT Park": "Indore-YASH IT Park-SC-DC",
    "YASH IT Park": "Indore-YASH IT Park-SC-DC",
    "Mindspace": "Hyderabad-Mindspace I-DC",
    "BTC": "Indore-BTC-CO",
    "Indore YIT":"Indore-YASH IT Park-SC-DC",
    "CIT":"Indore-YASH IT Park-SC-DC",
    "Hyd":"Hyderabad-Mindspace I-DC",
    "Magarpatta":"Pune",
    "MIDC":"Hyderabad-Mindspace I-DC",
    "BNG":"Bangalore",
    "Pune-Hinjewadi III-DC":"Pune",
    "Indore-Crystal IT Park-DC":"Indore-YASH IT Park-SC-DC",
    "Bangalore-Whitefield-DC":"Bangalore",
    "Bangalore-BHIVE-DC":"Bangalore"



    # Add more aliases as needed
}

# When to run monthly inventory updates
INVENTORY_UPDATE_DAY = 1      # 1st of each month
INVENTORY_UPDATE_HOUR = 0     # At midnight (00:00)
SCHEDULER_CHECK_INTERVAL = 3600  # Check every hour (in seconds)