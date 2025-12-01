import asyncio
from datetime import datetime, timedelta
from dateutil.parser import parse
from src.utils.db import async_get_milestone_data, async_update_inventory_quantity
from config.settings import (
    DATE_OF_BIRTH_COL,
    DATE_OF_MARRIAGE_COL, 
    DATE_OF_JOINING_COL,
    LOCATION_COL,
    INVENTORY_BIRTHDAY_WORKBOOK,
    INVENTORY_ANNIVERSARY_WORKBOOK,
    INVENTORY_SERVICE_COMPLETION_WORKBOOK
)
import logging

logger = logging.getLogger(__name__)


async def process_milestone_updates():
    """
    Process milestone data and update inventory accordingly:
    - Birthdays → Update Birthday gifts in inventory
    - Anniversaries → Update Anniversary gifts in inventory  
    - Service completion → Update Service Completion gifts in inventory
    """
    logger.info("=" * 80)
    logger.info("PROCESSING MILESTONE UPDATES")
    logger.info("=" * 80)
    
    try:
        # Get all milestone data
        milestone_data = await async_get_milestone_data()
        logger.info(f"✓ Retrieved {len(milestone_data)} milestone records")
        
        if not milestone_data:
            logger.warning("⚠ No milestone data found")
            return
        
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year
        
        birthday_updates = {}  # location -> count
        anniversary_updates = {}  # location -> count
        service_updates = {}  # location -> count
        
        for record in milestone_data:
            location = record.get(LOCATION_COL, "Unknown")
            
            # Process birthdays
            if DATE_OF_BIRTH_COL in record:
                try:
                    dob_str = record[DATE_OF_BIRTH_COL]
                    if dob_str and str(dob_str).strip():
                        dob = parse(str(dob_str))
                        
                        # Check if birthday has passed this month
                        if dob.month == current_month and dob.day <= current_date.day:
                            birthday_updates[location] = birthday_updates.get(location, 0) + 1
                            logger.info(f"  → Birthday passed: {record.get('Full Name', 'Unknown')} at {location}")
                            
                except Exception as e:
                    logger.debug(f"Failed to parse DOB for record: {e}")
            
            # Process anniversaries
            if DATE_OF_MARRIAGE_COL in record:
                try:
                    dom_str = record[DATE_OF_MARRIAGE_COL]
                    if dom_str and str(dom_str).strip():
                        dom = parse(str(dom_str))
                        
                        # Check if anniversary has passed this month
                        if dom.month == current_month and dom.day <= current_date.day:
                            anniversary_updates[location] = anniversary_updates.get(location, 0) + 1
                            logger.info(f"  → Anniversary passed: {record.get('Full Name', 'Unknown')} at {location}")
                            
                except Exception as e:
                    logger.debug(f"Failed to parse marriage date for record: {e}")
            
            # Process service completion (date of joining)
            if DATE_OF_JOINING_COL in record:
                try:
                    doj_str = record[DATE_OF_JOINING_COL]
                    if doj_str and str(doj_str).strip():
                        doj = parse(str(doj_str))
                        
                        # Check if joining anniversary is this month
                        if doj.month == current_month and doj.day <= current_date.day:
                            service_updates[location] = service_updates.get(location, 0) + 1
                            logger.info(f"  → Service completion: {record.get('Full Name', 'Unknown')} at {location}")
                            
                except Exception as e:
                    logger.debug(f"Failed to parse DOJ for record: {e}")
        
        # Update inventory based on calculations
        logger.info("-" * 80)
        logger.info("UPDATING INVENTORY")
        logger.info("-" * 80)
        
        # Update birthday gifts
        for location, count in birthday_updates.items():
            await async_update_inventory_quantity(
                location=location,
                workbook=INVENTORY_BIRTHDAY_WORKBOOK,
                quantity_change=-count  # Reduce quantity
            )
        
        # Update anniversary gifts
        for location, count in anniversary_updates.items():
            await async_update_inventory_quantity(
                location=location,
                workbook=INVENTORY_ANNIVERSARY_WORKBOOK,
                quantity_change=-count  # Reduce quantity
            )
        
        # Update service completion gifts
        for location, count in service_updates.items():
            await async_update_inventory_quantity(
                location=location,
                workbook=INVENTORY_SERVICE_COMPLETION_WORKBOOK,
                quantity_change=-count  # Reduce quantity
            )
        
        logger.info("=" * 80)
        logger.info(f"✓ Processed {len(birthday_updates)} birthday updates")
        logger.info(f"✓ Processed {len(anniversary_updates)} anniversary updates")
        logger.info(f"✓ Processed {len(service_updates)} service completion updates")
        logger.info("=" * 80)
        
        return {
            "birthday_updates": birthday_updates,
            "anniversary_updates": anniversary_updates,
            "service_updates": service_updates
        }
        
    except Exception as e:
        logger.exception(f"✗ Failed to process milestone updates: {e}")
        raise


async def check_and_update_inventory():
    """
    Wrapper function to check milestones and update inventory.
    Can be called periodically or on-demand.
    """
    try:
        results = await process_milestone_updates()
        return {
            "status": "success",
            "updates": results
        }
    except Exception as e:
        logger.error(f"✗ Inventory update failed: {e}")
        return {
            "status": "error",
            "message": str(e)
        }