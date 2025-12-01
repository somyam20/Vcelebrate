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

# Milestone date columns (configurable)
DATE_OF_BIRTH_COL = os.getenv("DATE_OF_BIRTH_COL", "Date of birth")
DATE_OF_MARRIAGE_COL = os.getenv("DATE_OF_MARRIAGE_COL", "Date of marriage")
DATE_OF_JOINING_COL = os.getenv("DATE_OF_JOINING_COL", "DOJ-We Celebrate")
LOCATION_COL = os.getenv("LOCATION_COL", "Location")

# Inventory workbook columns (configurable)
INVENTORY_BIRTHDAY_WORKBOOK = os.getenv("INVENTORY_BIRTHDAY_WORKBOOK", "Birthday")
INVENTORY_ANNIVERSARY_WORKBOOK = os.getenv("INVENTORY_ANNIVERSARY_WORKBOOK", "As on 03-10-25")
INVENTORY_SERVICE_COMPLETION_WORKBOOK = os.getenv("INVENTORY_SERVICE_COMPLETION_WORKBOOK", "Service Completion")
INVENTORY_LOCATION_COL = os.getenv("INVENTORY_LOCATION_COL", "Location")
INVENTORY_QUANTITY_COL = os.getenv("INVENTORY_QUANTITY_COL", "Quantity Received")

# Update frequency
INVENTORY_UPDATE_FREQUENCY = os.getenv("INVENTORY_UPDATE_FREQUENCY", "monthly")