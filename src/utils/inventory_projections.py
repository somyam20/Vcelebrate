import asyncio
from datetime import datetime
from dateutil.parser import parse
from src.utils.db import (
    async_get_milestone_data, 
    async_get_inventory_data
)
from config.settings import (
    INVENTORY_WORKBOOKS,
    LOW_INVENTORY_THRESHOLD,
    LOCATION_ALIASES
)
import logging
import pandas as pd
import calendar

logger = logging.getLogger(__name__)


def extract_month_from_date(date_value):
    """Safely extract month from various date formats"""
    if not date_value or pd.isna(date_value):
        return None
    
    try:
        # Handle numeric month values (1-12)
        if isinstance(date_value, (int, float)):
            month_num = int(date_value)
            if 1 <= month_num <= 12:
                return month_num
            return None
        
        date_str = str(date_value).strip()
        if not date_str or date_str.lower() in ['nan', 'nat', 'none', '']:
            return None
        
        # Check if it's just a number string
        try:
            month_num = int(date_str)
            if 1 <= month_num <= 12:
                return month_num
        except:
            pass
        
        # Try to parse as date
        parsed_date = parse(date_str)
        return parsed_date.month
    except:
        return None


def normalize_location(location_name: str) -> str:
    """Normalize location name using LOCATION_ALIASES mapping"""
    if not location_name or pd.isna(location_name):
        return "Unknown"
    
    location_str = str(location_name).strip()
    return LOCATION_ALIASES.get(location_str, location_str)


def calculate_monthly_milestone_counts(milestone_data, target_month: int, target_year: int):
    """
    Calculate birthday, anniversary, and service completion counts for a specific month.
    Returns location-wise counts.
    """
    df = pd.DataFrame(milestone_data)
    
    # Initialize results
    birthday_counts = {}
    anniversary_counts = {}
    service_counts = {}
    
    # Field mapping
    location_fields = ['Place of posting', 'Base Location Name', 'Base Location  Name', 'Location']
    dob_fields = ['Date of Birth (as per Records)', 'DOB', 'Date of Birth']
    dom_fields = ['Date of Marriage', 'Marriage Date']
    doj_fields = ['Employment Details Date of Joining', 'Date of Joining', 'DOJ']
    dob_month_fields = ['MM Birth - WE Celebrate', 'Birth Month']
    doj_month_fields = ['MM Service Completion - WE Celebrate', 'Service Month']
    
    # Find which fields exist
    location_col = next((f for f in location_fields if f in df.columns), None)
    dob_col = next((f for f in dob_fields if f in df.columns), None)
    dob_month_col = next((f for f in dob_month_fields if f in df.columns), None)
    dom_col = next((f for f in dom_fields if f in df.columns), None)
    doj_col = next((f for f in doj_fields if f in df.columns), None)
    doj_month_col = next((f for f in doj_month_fields if f in df.columns), None)
    
    if not location_col:
        logger.error("No location column found!")
        return birthday_counts, anniversary_counts, service_counts
    
    # Process each record
    for idx, row in df.iterrows():
        location = row.get(location_col)
        if not location or pd.isna(location):
            location = "Unknown"
        
        location = normalize_location(str(location).strip())
        
        # Count Birthdays
        if dob_month_col and dob_month_col in row:
            birth_month = extract_month_from_date(row[dob_month_col])
            if birth_month == target_month:
                birthday_counts[location] = birthday_counts.get(location, 0) + 1
        elif dob_col and dob_col in row:
            birth_month = extract_month_from_date(row[dob_col])
            if birth_month == target_month:
                birthday_counts[location] = birthday_counts.get(location, 0) + 1
        
        # Count Anniversaries
        if dom_col and dom_col in row:
            marriage_month = extract_month_from_date(row[dom_col])
            if marriage_month == target_month:
                anniversary_counts[location] = anniversary_counts.get(location, 0) + 1
        
        # Count Service Completions
        if doj_month_col and doj_month_col in row:
            service_month = extract_month_from_date(row[doj_month_col])
            if service_month == target_month:
                service_counts[location] = service_counts.get(location, 0) + 1
        elif doj_col and doj_col in row:
            service_month = extract_month_from_date(row[doj_col])
            if service_month == target_month:
                service_counts[location] = service_counts.get(location, 0) + 1
    
    return birthday_counts, anniversary_counts, service_counts


async def get_current_inventory_by_location(location: str = None):
    """
    Get current inventory quantities by location and gift type.
    Returns dict: {location: {gift_type: quantity}}
    """
    inventory_data = await async_get_inventory_data()
    
    inventory_summary = {}
    
    for item in inventory_data:
        loc = normalize_location(item.get('location', 'Unknown'))
        workbook = item.get('workbook', '')
        data = item.get('data', {})
        
        # Find quantity column
        quantity_col = None
        for key in data.keys():
            if "quantity" in key.lower() and "received" in key.lower():
                quantity_col = key
                break
        
        if quantity_col:
            try:
                quantity = int(data.get(quantity_col, 0)) if data.get(quantity_col) else 0
            except (ValueError, TypeError):
                quantity = 0
            
            # Map workbook to gift type
            gift_type = None
            if "birthday" in workbook.lower():
                gift_type = "birthday"
            elif "as on" in workbook.lower() or "anniversary" in workbook.lower():
                gift_type = "anniversary"
            elif "service" in workbook.lower():
                gift_type = "service_completion"
            
            if gift_type:
                if loc not in inventory_summary:
                    inventory_summary[loc] = {}
                
                # Sum up quantities if multiple entries exist
                inventory_summary[loc][gift_type] = inventory_summary[loc].get(gift_type, 0) + quantity
    
    # If specific location requested, filter
    if location:
        normalized_loc = normalize_location(location)
        return {normalized_loc: inventory_summary.get(normalized_loc, {})}
    
    return inventory_summary


async def calculate_remaining_gifts_next_month(location: str, gift_type: str = None):
    """
    Calculate how many gifts will be left next month at a specific location.
    
    Args:
        location: Location name (e.g., "Indore-YASH IT Park-SC-DC", "indore yit")
        gift_type: Type of gift - "birthday", "anniversary", or "service_completion"
                   If None, calculates for all types
    
    Returns:
        dict with gift types and remaining quantities
    """
    logger.info(f"Calculating remaining gifts for location: {location}, type: {gift_type}")
    
    # Normalize location
    normalized_location = normalize_location(location)
    
    # Get current date
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    
    # Calculate next month
    next_month = current_month + 1 if current_month < 12 else 1
    next_year = current_year if current_month < 12 else current_year + 1
    
    # Get milestone data
    milestone_data = await async_get_milestone_data()
    
    # Get current month's milestone counts
    current_birthday, current_anniversary, current_service = calculate_monthly_milestone_counts(
        milestone_data, current_month, current_year
    )
    
    # Get current inventory
    inventory = await get_current_inventory_by_location(normalized_location)
    
    if normalized_location not in inventory:
        logger.warning(f"No inventory found for location: {normalized_location}")
        return {
            "error": f"No inventory found for location: {location}",
            "normalized_location": normalized_location
        }
    
    location_inventory = inventory[normalized_location]
    
    results = {}
    
    # Calculate for each gift type
    gift_types_to_check = [gift_type] if gift_type else ["birthday", "anniversary", "service_completion"]
    
    for gtype in gift_types_to_check:
        current_stock = location_inventory.get(gtype, 0)
        
        # Get current month usage
        if gtype == "birthday":
            current_month_usage = current_birthday.get(normalized_location, 0)
        elif gtype == "anniversary":
            current_month_usage = current_anniversary.get(normalized_location, 0)
        else:  # service_completion
            current_month_usage = current_service.get(normalized_location, 0)
        
        # Calculate remaining after this month
        remaining_after_current_month = current_stock - current_month_usage
        
        results[gtype] = {
            "current_stock": current_stock,
            "current_month_usage": current_month_usage,
            "remaining_after_current_month": remaining_after_current_month,
            "status": "adequate" if remaining_after_current_month >= LOW_INVENTORY_THRESHOLD else "low"
        }
    
    return {
        "location": location,
        "normalized_location": normalized_location,
        "current_month": calendar.month_name[current_month],
        "current_year": current_year,
        "next_month": calendar.month_name[next_month],
        "next_year": next_year,
        "projections": results
    }


async def calculate_months_until_restock(location: str, gift_type: str, max_months: int = 24):
    """
    Calculate how many months until a gift type needs to be restocked at a location.
    
    Args:
        location: Location name
        gift_type: "birthday", "anniversary", or "service_completion"
        max_months: Maximum months to project (default: 24)
    
    Returns:
        dict with restock prediction details
    """
    logger.info(f"Calculating months until restock for {gift_type} at {location}")
    
    # Normalize location
    normalized_location = normalize_location(location)
    
    # Get current date
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    
    # Get milestone data
    milestone_data = await async_get_milestone_data()
    
    # Get current inventory
    inventory = await get_current_inventory_by_location(normalized_location)
    
    if normalized_location not in inventory:
        return {
            "error": f"No inventory found for location: {location}",
            "normalized_location": normalized_location
        }
    
    location_inventory = inventory[normalized_location]
    current_stock = location_inventory.get(gift_type, 0)
    
    # Track inventory over time
    remaining_stock = current_stock
    monthly_breakdown = []
    months_until_restock = None
    
    for month_offset in range(max_months):
        # Calculate target month/year
        target_month = ((current_month - 1 + month_offset) % 12) + 1
        target_year = current_year + ((current_month - 1 + month_offset) // 12)
        
        # Get milestone counts for this month
        birthday_counts, anniversary_counts, service_counts = calculate_monthly_milestone_counts(
            milestone_data, target_month, target_year
        )
        
        # Get usage for this location
        if gift_type == "birthday":
            usage = birthday_counts.get(normalized_location, 0)
        elif gift_type == "anniversary":
            usage = anniversary_counts.get(normalized_location, 0)
        else:  # service_completion
            usage = service_counts.get(normalized_location, 0)
        
        # Calculate remaining stock after this month
        remaining_stock -= usage
        
        monthly_breakdown.append({
            "month": calendar.month_name[target_month],
            "year": target_year,
            "usage": usage,
            "remaining_stock": remaining_stock
        })
        
        # Check if we've hit the threshold
        if remaining_stock < LOW_INVENTORY_THRESHOLD and months_until_restock is None:
            months_until_restock = month_offset
            break
    
    # If we never hit threshold within max_months
    if months_until_restock is None:
        months_until_restock = max_months
        restock_status = "not_needed_within_projection"
    else:
        restock_status = "restock_needed"
    
    return {
        "location": location,
        "normalized_location": normalized_location,
        "gift_type": gift_type,
        "current_stock": current_stock,
        "threshold": LOW_INVENTORY_THRESHOLD,
        "months_until_restock": months_until_restock,
        "restock_status": restock_status,
        "monthly_breakdown": monthly_breakdown,
        "projection_range": f"{current_month}/{current_year} to {monthly_breakdown[-1]['month']}/{monthly_breakdown[-1]['year']}"
    }


async def get_location_inventory_projection(location: str, months_ahead: int = 12):
    """
    Get comprehensive inventory projection for all gift types at a location.
    
    Args:
        location: Location name
        months_ahead: Number of months to project (default: 12)
    
    Returns:
        Complete projection report for the location
    """
    logger.info(f"Generating comprehensive inventory projection for {location}")
    
    normalized_location = normalize_location(location)
    
    # Get projections for each gift type
    birthday_projection = await calculate_months_until_restock(location, "birthday", months_ahead)
    anniversary_projection = await calculate_months_until_restock(location, "anniversary", months_ahead)
    service_projection = await calculate_months_until_restock(location, "service_completion", months_ahead)
    
    return {
        "location": location,
        "normalized_location": normalized_location,
        "projection_months": months_ahead,
        "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "birthday_gifts": birthday_projection,
        "anniversary_gifts": anniversary_projection,
        "service_completion_gifts": service_projection,
        "summary": {
            "birthday_restock_in_months": birthday_projection.get("months_until_restock"),
            "anniversary_restock_in_months": anniversary_projection.get("months_until_restock"),
            "service_restock_in_months": service_projection.get("months_until_restock"),
            "earliest_restock_needed": min(
                birthday_projection.get("months_until_restock", float('inf')),
                anniversary_projection.get("months_until_restock", float('inf')),
                service_projection.get("months_until_restock", float('inf'))
            )
        }
    }


async def get_all_locations_restock_schedule(max_months: int = 12):
    """
    Get restock schedule for all locations and all gift types.
    
    Returns:
        Comprehensive restock schedule across all locations
    """
    logger.info("Generating restock schedule for all locations")
    
    # Get all inventory locations
    inventory = await get_current_inventory_by_location()
    
    all_locations_schedule = {}
    urgent_restocks = []
    
    for location in inventory.keys():
        location_schedule = await get_location_inventory_projection(location, max_months)
        all_locations_schedule[location] = location_schedule
        
        # Check for urgent restocks (within 3 months)
        summary = location_schedule.get("summary", {})
        if summary.get("earliest_restock_needed", float('inf')) <= 3:
            urgent_restocks.append({
                "location": location,
                "months_until_restock": summary.get("earliest_restock_needed"),
                "details": summary
            })
    
    return {
        "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_locations": len(all_locations_schedule),
        "projection_months": max_months,
        "urgent_restocks": sorted(urgent_restocks, key=lambda x: x["months_until_restock"]),
        "all_locations": all_locations_schedule
    }