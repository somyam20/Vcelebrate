import asyncio
from datetime import datetime
from dateutil.parser import parse
from src.utils.db import (
    async_get_milestone_data, 
    async_update_inventory_quantity,
    async_get_low_inventory_alerts,
    async_get_inventory_data
)
from config.settings import (
    INVENTORY_WORKBOOKS,
    LOW_INVENTORY_THRESHOLD
)
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def extract_month_from_date(date_value):
    """Safely extract month from various date formats"""
    if not date_value or pd.isna(date_value):
        return None
    
    try:
        if isinstance(date_value, (int, float)):
            return int(date_value)
        
        date_str = str(date_value).strip()
        if not date_str or date_str.lower() in ['nan', 'nat', 'none', '']:
            return None
        
        # Try to parse as date
        parsed_date = parse(date_str)
        return parsed_date.month
    except:
        return None


def calculate_milestone_counts(milestone_data, target_month: int, target_year: int):
    """
    Calculate birthday, anniversary, and service completion counts using pandas.
    Returns location-wise counts without LLM involvement.
    """
    logger.info("=" * 80)
    logger.info(f"CALCULATING MILESTONE COUNTS FOR {target_month}/{target_year}")
    logger.info("=" * 80)
    
    # Convert to DataFrame for easier processing
    df = pd.DataFrame(milestone_data)
    
    logger.info(f"Total milestone records: {len(df)}")
    
    # Initialize results
    birthday_counts = {}
    anniversary_counts = {}
    service_counts = {}
    
    # Field mapping for different column name variations
    location_fields = ['Place of posting', 'Base Location Name', 'Base Location  Name', 'Location']
    dob_fields = ['Date of Birth (as per Records)', 'DOB', 'Date of Birth']
    dom_fields = ['Date of Marriage', 'Marriage Date']
    doj_fields = ['Employment Details Date of Joining', 'Date of Joining', 'DOJ']
    
    # Also check for pre-calculated month fields
    dob_month_fields = ['MM Birth - WE Celebrate', 'Birth Month']
    doj_month_fields = ['MM Service Completion - WE Celebrate', 'Service Month']
    
    # Find which fields exist in the data
    location_col = None
    for field in location_fields:
        if field in df.columns:
            location_col = field
            break
    
    dob_col = None
    for field in dob_fields:
        if field in df.columns:
            dob_col = field
            break
    
    dob_month_col = None
    for field in dob_month_fields:
        if field in df.columns:
            dob_month_col = field
            break
    
    dom_col = None
    for field in dom_fields:
        if field in df.columns:
            dom_col = field
            break
    
    doj_col = None
    for field in doj_fields:
        if field in df.columns:
            doj_col = field
            break
    
    doj_month_col = None
    for field in doj_month_fields:
        if field in df.columns:
            doj_month_col = field
            break
    
    logger.info(f"Mapped columns - Location: {location_col}, DOB: {dob_col}, DOM: {dom_col}, DOJ: {doj_col}")
    logger.info(f"Month columns - DOB Month: {dob_month_col}, DOJ Month: {doj_month_col}")
    
    if not location_col:
        logger.error("No location column found!")
        return birthday_counts, anniversary_counts, service_counts
    
    # Process each record
    for idx, row in df.iterrows():
        location = row.get(location_col)
        if not location or pd.isna(location):
            location = "Unknown"
        
        location = str(location).strip()
        
        # Count Birthdays
        if dob_month_col and dob_month_col in row:
            # Use pre-calculated month field
            birth_month = extract_month_from_date(row[dob_month_col])
            if birth_month == target_month:
                birthday_counts[location] = birthday_counts.get(location, 0) + 1
                logger.debug(f"Birthday match: {row.get('Full Name', 'Unknown')} at {location}")
        elif dob_col and dob_col in row:
            # Parse date field
            birth_month = extract_month_from_date(row[dob_col])
            if birth_month == target_month:
                birthday_counts[location] = birthday_counts.get(location, 0) + 1
                logger.debug(f"Birthday match: {row.get('Full Name', 'Unknown')} at {location}")
        
        # Count Anniversaries
        if dom_col and dom_col in row:
            marriage_month = extract_month_from_date(row[dom_col])
            if marriage_month == target_month:
                anniversary_counts[location] = anniversary_counts.get(location, 0) + 1
                logger.debug(f"Anniversary match: {row.get('Full Name', 'Unknown')} at {location}")
        
        # Count Service Completions
        if doj_month_col and doj_month_col in row:
            # Use pre-calculated month field
            service_month = extract_month_from_date(row[doj_month_col])
            if service_month == target_month:
                service_counts[location] = service_counts.get(location, 0) + 1
                logger.debug(f"Service completion match: {row.get('Full Name', 'Unknown')} at {location}")
        elif doj_col and doj_col in row:
            # Parse date field
            service_month = extract_month_from_date(row[doj_col])
            if service_month == target_month:
                service_counts[location] = service_counts.get(location, 0) + 1
                logger.debug(f"Service completion match: {row.get('Full Name', 'Unknown')} at {location}")
    
    logger.info("-" * 80)
    logger.info(f"CALCULATED COUNTS FOR {target_month}/{target_year}:")
    logger.info(f"Birthdays: {sum(birthday_counts.values())} across {len(birthday_counts)} locations")
    logger.info(f"Anniversaries: {sum(anniversary_counts.values())} across {len(anniversary_counts)} locations")
    logger.info(f"Service Completions: {sum(service_counts.values())} across {len(service_counts)} locations")
    logger.info("-" * 80)
    
    return birthday_counts, anniversary_counts, service_counts


async def process_milestone_updates(target_month: int = None, target_year: int = None):
    """
    Process milestone data and update inventory with pre-calculated counts.
    No LLM involved - uses pandas for accurate calculations.
    """
    logger.info("=" * 80)
    logger.info("PROCESSING MILESTONE UPDATES")
    logger.info("=" * 80)
    
    try:
        # Get current date
        current_date = datetime.now()
        target_month = target_month or current_date.month
        target_year = target_year or current_date.year
        
        logger.info(f"Processing for: {target_month}/{target_year} (Current: {current_date.strftime('%m/%Y')})")
        
        # Get milestone data
        milestone_data = await async_get_milestone_data()
        logger.info(f"Retrieved {len(milestone_data)} milestone records")
        
        if not milestone_data:
            logger.warning("No milestone data found")
            return {
                "status": "no_data",
                "message": "No milestone data available",
                "month": target_month,
                "year": target_year
            }
        
        # Calculate counts using pandas (no LLM)
        birthday_counts, anniversary_counts, service_counts = calculate_milestone_counts(
            milestone_data, target_month, target_year
        )
        
        # Update inventory based on calculations
        logger.info("-" * 80)
        logger.info("UPDATING INVENTORY")
        logger.info("-" * 80)
        
        update_results = {
            "birthday": [],
            "anniversary": [],
            "service_completion": []
        }
        
        # Update birthday gifts
        for location, count in birthday_counts.items():
            if count > 0:
                result = await async_update_inventory_quantity(
                    location=location,
                    workbook=INVENTORY_WORKBOOKS["birthday"],
                    quantity_change=-count
                )
                if result:
                    update_results["birthday"].append(result)
                    logger.info(f"Updated Birthday gifts at {location}: -{count}")
        
        # Update anniversary gifts
        for location, count in anniversary_counts.items():
            if count > 0:
                result = await async_update_inventory_quantity(
                    location=location,
                    workbook=INVENTORY_WORKBOOKS["anniversary"],
                    quantity_change=-count
                )
                if result:
                    update_results["anniversary"].append(result)
                    logger.info(f"Updated Anniversary gifts at {location}: -{count}")
        
        # Update service completion gifts
        for location, count in service_counts.items():
            if count > 0:
                result = await async_update_inventory_quantity(
                    location=location,
                    workbook=INVENTORY_WORKBOOKS["service_completion"],
                    quantity_change=-count
                )
                if result:
                    update_results["service_completion"].append(result)
                    logger.info(f"Updated Service Completion gifts at {location}: -{count}")
        
        # Check for low inventory alerts
        logger.info("-" * 80)
        logger.info("CHECKING LOW INVENTORY ALERTS")
        logger.info("-" * 80)
        
        low_inventory_alerts = await async_get_low_inventory_alerts(LOW_INVENTORY_THRESHOLD)
        
        if low_inventory_alerts:
            logger.warning(f"ALERT: {len(low_inventory_alerts)} items below threshold ({LOW_INVENTORY_THRESHOLD})")
            for alert in low_inventory_alerts:
                logger.warning(
                    f"  LOW STOCK: {alert['workbook']} at {alert['location']}: "
                    f"{alert['current_quantity']} (threshold: {alert['threshold']})"
                )
        
        logger.info("=" * 80)
        logger.info(f"COMPLETED: {sum(birthday_counts.values())} birthdays, "
                   f"{sum(anniversary_counts.values())} anniversaries, "
                   f"{sum(service_counts.values())} service completions")
        logger.info("=" * 80)
        
        return {
            "status": "success",
            "month": target_month,
            "year": target_year,
            "birthday_counts": birthday_counts,
            "anniversary_counts": anniversary_counts,
            "service_counts": service_counts,
            "total_birthdays": sum(birthday_counts.values()),
            "total_anniversaries": sum(anniversary_counts.values()),
            "total_service_completions": sum(service_counts.values()),
            "update_results": update_results,
            "low_inventory_alerts": low_inventory_alerts
        }
        
    except Exception as e:
        logger.exception(f"Failed to process milestone updates: {e}")
        raise


async def check_and_update_inventory(target_month: int = None, target_year: int = None):
    """
    Main function to check milestones and update inventory.
    Should be called on the 1st of each month automatically.
    """
    try:
        results = await process_milestone_updates(target_month, target_year)
        return results
    except Exception as e:
        logger.error(f"Inventory update failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }


async def get_monthly_summary(target_month: int = None, target_year: int = None):
    """
    Get a summary of milestones and inventory for a specific month.
    This is what should be called by the query endpoint.
    """
    try:
        current_date = datetime.now()
        target_month = target_month or current_date.month
        target_year = target_year or current_date.year
        
        # Get milestone data
        milestone_data = await async_get_milestone_data()
        
        if not milestone_data:
            return {
                "month": target_month,
                "year": target_year,
                "error": "No milestone data available"
            }
        
        # Calculate counts
        birthday_counts, anniversary_counts, service_counts = calculate_milestone_counts(
            milestone_data, target_month, target_year
        )
        
        # Get current inventory
        inventory_data = await async_get_inventory_data()
        
        # Get low inventory alerts
        low_inventory = await async_get_low_inventory_alerts(LOW_INVENTORY_THRESHOLD)
        
        return {
            "month": target_month,
            "year": target_year,
            "birthdays": {
                "total": sum(birthday_counts.values()),
                "by_location": birthday_counts
            },
            "anniversaries": {
                "total": sum(anniversary_counts.values()),
                "by_location": anniversary_counts
            },
            "service_completions": {
                "total": sum(service_counts.values()),
                "by_location": service_counts
            },
            "low_inventory_alerts": low_inventory,
            "current_inventory": inventory_data
        }
        
    except Exception as e:
        logger.error(f"Failed to get monthly summary: {e}")
        return {
            "month": target_month,
            "year": target_year,
            "error": str(e)
        }