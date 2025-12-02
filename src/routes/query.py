import asyncio
import json
from fastapi import APIRouter, HTTPException, Form, Request
from src.utils.db import async_get_all_data
from src.utils.inventory_processor import get_monthly_summary, check_and_update_inventory
from src.utils.inventory_projections import (
    calculate_remaining_gifts_next_month,
    calculate_months_until_restock,
    get_location_inventory_projection,
    get_all_locations_restock_schedule
)
from config.config import get_model_config
from config.settings import CATEGORIES
import logging
import litellm
from datetime import datetime
import calendar

logger = logging.getLogger(__name__)

router = APIRouter()


def get_month_name(month_num: int) -> str:
    """Convert month number to name"""
    return calendar.month_name[month_num]


@router.post("/query")
async def query_data(
    req: Request,
    query: str = Form(...),
    user_metadata: str = Form(...),
    force_update: bool = Form(False)
):
    """
    Query all data across categories using LLM with pre-calculated milestone counts.
    
    The system automatically calculates birthday, anniversary, and service completion
    counts for the current month, so the LLM doesn't need to do date calculations.
    
    Parameters:
    - query: The user's question
    - user_metadata: JSON string with team_id
    - force_update: (Optional) Set to true to trigger inventory update before querying
    
    Special queries:
    - "update inventory" or "trigger update" - Manually updates inventory
    - "inventory status" or "system status" - Shows current system status
    - "remaining gifts [location]" - Shows remaining gifts projection for location
    - "restock schedule [location]" - Shows when restocking is needed
    - "all restock schedules" - Shows restock schedule for all locations
    - Any other query - Normal LLM-powered query
    """
    logger.info("=" * 80)
    logger.info("QUERY REQUEST START")
    logger.info(f"Query: {query}")
    logger.info(f"Force Update: {force_update}")
    logger.info("=" * 80)
    
    # Parse user metadata
    user_metadata_dict = json.loads(user_metadata) if user_metadata else {}
    team_id = user_metadata_dict.get("team_id")
    
    if not team_id:
        logger.error("Missing team_id in user_metadata")
        raise HTTPException(
            status_code=400,
            detail="team_id is required in user_metadata"
        )
    
    # Get current date info
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    month_name = get_month_name(current_month)
    
    # Check for special commands
    query_lower = query.lower().strip()
    
    # Handle "update inventory" command
    if any(keyword in query_lower for keyword in ["update inventory", "trigger update", "run update", "manual update"]):
        logger.info("SPECIAL COMMAND: Manual inventory update triggered")
        try:
            result = await check_and_update_inventory(current_month, current_year)
            
            if result.get("status") == "success":
                response_text = f"""Inventory Update Completed for {month_name} {current_year}

Summary:
- Birthdays: {result.get('total_birthdays', 0)} employees across {len(result.get('birthday_counts', {}))} locations
- Anniversaries: {result.get('total_anniversaries', 0)} employees across {len(result.get('anniversary_counts', {}))} locations
- Service Completions: {result.get('total_service_completions', 0)} employees across {len(result.get('service_counts', {}))} locations

Inventory Updates Applied:
"""
                for gift_type in ['birthday', 'anniversary', 'service_completion']:
                    updates = result.get('update_results', {}).get(gift_type, [])
                    if updates:
                        response_text += f"\n{gift_type.replace('_', ' ').title()}:\n"
                        for update in updates:
                            response_text += f"  - {update['location']}: {update['old_quantity']} → {update['new_quantity']}\n"
                
                alerts = result.get('low_inventory_alerts', [])
                if alerts:
                    response_text += f"\nLow Inventory Alerts ({len(alerts)} items below threshold):\n"
                    for alert in alerts:
                        response_text += f"  - {alert['workbook']} at {alert['location']}: {alert['current_quantity']} units\n"
                else:
                    response_text += "\nNo low inventory alerts."
                
                return {
                    "query": query,
                    "answer": response_text,
                    "command": "inventory_update",
                    "status": "success",
                    "current_date": current_date.strftime('%Y-%m-%d'),
                    "update_details": result
                }
            else:
                return {
                    "query": query,
                    "answer": f"Inventory update failed: {result.get('message', 'Unknown error')}",
                    "command": "inventory_update",
                    "status": "error",
                    "current_date": current_date.strftime('%Y-%m-%d')
                }
                
        except Exception as e:
            logger.exception(f"Failed to update inventory: {e}")
            return {
                "query": query,
                "answer": f"Error updating inventory: {str(e)}",
                "command": "inventory_update",
                "status": "error",
                "current_date": current_date.strftime('%Y-%m-%d')
            }
    
    # Handle "inventory status" or "system status" command
    if any(keyword in query_lower for keyword in ["inventory status", "system status", "check status", "current status"]):
        logger.info("SPECIAL COMMAND: Status check triggered")
        try:
            summary = await get_monthly_summary(current_month, current_year)
            
            response_text = f"""System Status for {month_name} {current_year}

Current Date: {current_date.strftime('%B %d, %Y')}

Milestones This Month:
- Birthdays: {summary.get('birthdays', {}).get('total', 0)} employees
- Anniversaries: {summary.get('anniversaries', {}).get('total', 0)} employees
- Service Completions: {summary.get('service_completions', {}).get('total', 0)} employees

Location Breakdown:
"""
            
            if summary.get('birthdays', {}).get('by_location'):
                response_text += "\nBirthdays by Location:\n"
                for loc, count in summary.get('birthdays', {}).get('by_location', {}).items():
                    response_text += f"  - {loc}: {count}\n"
            
            if summary.get('anniversaries', {}).get('by_location'):
                response_text += "\nAnniversaries by Location:\n"
                for loc, count in summary.get('anniversaries', {}).get('by_location', {}).items():
                    response_text += f"  - {loc}: {count}\n"
            
            if summary.get('service_completions', {}).get('by_location'):
                response_text += "\nService Completions by Location:\n"
                for loc, count in summary.get('service_completions', {}).get('by_location', {}).items():
                    response_text += f"  - {loc}: {count}\n"
            
            alerts = summary.get('low_inventory_alerts', [])
            if alerts:
                response_text += f"\nLow Inventory Alerts ({len(alerts)} items):\n"
                for alert in alerts:
                    response_text += f"  - {alert['workbook']} at {alert['location']}: {alert['current_quantity']} units\n"
            else:
                response_text += "\nNo low inventory alerts. All inventory levels are adequate."
            
            return {
                "query": query,
                "answer": response_text,
                "command": "status_check",
                "status": "success",
                "current_date": current_date.strftime('%Y-%m-%d'),
                "summary": summary
            }
            
        except Exception as e:
            logger.exception(f"Failed to get status: {e}")
            return {
                "query": query,
                "answer": f"Error getting status: {str(e)}",
                "command": "status_check",
                "status": "error",
                "current_date": current_date.strftime('%Y-%m-%d')
            }
    
    # Handle "remaining gifts" projection command
    if any(keyword in query_lower for keyword in ["remaining gifts", "gifts left", "how many gifts left", "gifts remaining"]):
        logger.info("SPECIAL COMMAND: Remaining gifts projection")
        try:
            # Extract location from query (simple extraction)
            location = None
            common_locations = ["indore", "yit", "hyderabad", "bangalore", "pune", "btc"]
            for loc in common_locations:
                if loc in query_lower:
                    location = loc
                    break
            
            if not location:
                return {
                    "query": query,
                    "answer": "Please specify a location (e.g., 'Indore YIT', 'Hyderabad', 'Bangalore')",
                    "command": "remaining_gifts",
                    "status": "error"
                }
            
            # Determine gift type if specified
            gift_type = None
            if "birthday" in query_lower:
                gift_type = "birthday"
            elif "anniversary" in query_lower:
                gift_type = "anniversary"
            elif "service" in query_lower:
                gift_type = "service_completion"
            
            result = await calculate_remaining_gifts_next_month(location, gift_type)
            
            if "error" in result:
                return {
                    "query": query,
                    "answer": result["error"],
                    "command": "remaining_gifts",
                    "status": "error"
                }
            
            response_text = f"""Remaining Gifts Projection for {result['normalized_location']}

Current Month: {result['current_month']} {result['current_year']}
Next Month: {result['next_month']} {result['next_year']}

"""
            
            for gtype, data in result['projections'].items():
                response_text += f"\n{gtype.replace('_', ' ').title()} Gifts:\n"
                response_text += f"  Current Stock: {data['current_stock']}\n"
                response_text += f"  This Month Usage: {data['current_month_usage']}\n"
                response_text += f"  Remaining Next Month: {data['remaining_after_current_month']}\n"
                response_text += f"  Status: {data['status'].upper()}\n"
            
            return {
                "query": query,
                "answer": response_text,
                "command": "remaining_gifts",
                "status": "success",
                "projection_data": result
            }
            
        except Exception as e:
            logger.exception(f"Failed to calculate remaining gifts: {e}")
            return {
                "query": query,
                "answer": f"Error calculating remaining gifts: {str(e)}",
                "command": "remaining_gifts",
                "status": "error"
            }
    
    # Handle "restock schedule" command
    if any(keyword in query_lower for keyword in ["restock", "when to restock", "months until restock", "restock schedule"]):
        logger.info("SPECIAL COMMAND: Restock schedule")
        try:
            # Check if "all locations" requested
            if any(keyword in query_lower for keyword in ["all locations", "all location", "every location"]):
                result = await get_all_locations_restock_schedule()
                
                response_text = f"""Restock Schedule for All Locations

Generated: {result['generated_at']}
Total Locations: {result['total_locations']}
Projection Period: {result['projection_months']} months

"""
                
                if result['urgent_restocks']:
                    response_text += f"\nURGENT RESTOCKS (Within 3 months):\n"
                    for urgent in result['urgent_restocks']:
                        response_text += f"\n{urgent['location']}:\n"
                        response_text += f"  Restock needed in: {urgent['months_until_restock']} month(s)\n"
                        details = urgent['details']
                        response_text += f"  Birthday gifts: {details.get('birthday_restock_in_months')} months\n"
                        response_text += f"  Anniversary gifts: {details.get('anniversary_restock_in_months')} months\n"
                        response_text += f"  Service gifts: {details.get('service_restock_in_months')} months\n"
                
                response_text += f"\n\nFull restock schedule available in projection_data.\n"
                
                return {
                    "query": query,
                    "answer": response_text,
                    "command": "restock_schedule_all",
                    "status": "success",
                    "projection_data": result
                }
            
            # Single location restock schedule
            location = None
            common_locations = ["indore", "yit", "hyderabad", "bangalore", "pune", "btc"]
            for loc in common_locations:
                if loc in query_lower:
                    location = loc
                    break
            
            if not location:
                return {
                    "query": query,
                    "answer": "Please specify a location or use 'all locations'",
                    "command": "restock_schedule",
                    "status": "error"
                }
            
            result = await get_location_inventory_projection(location)
            
            if "error" in result:
                return {
                    "query": query,
                    "answer": result["error"],
                    "command": "restock_schedule",
                    "status": "error"
                }
            
            response_text = f"""Restock Schedule for {result['normalized_location']}

Generated: {result['generated_at']}
Projection Period: {result['projection_months']} months

Summary:
- Birthday Gifts: Restock in {result['summary']['birthday_restock_in_months']} months
- Anniversary Gifts: Restock in {result['summary']['anniversary_restock_in_months']} months
- Service Completion Gifts: Restock in {result['summary']['service_restock_in_months']} months

Earliest Restock Needed: {result['summary']['earliest_restock_needed']} month(s)

Detailed breakdown available in projection_data.
"""
            
            return {
                "query": query,
                "answer": response_text,
                "command": "restock_schedule",
                "status": "success",
                "projection_data": result
            }
            
        except Exception as e:
            logger.exception(f"Failed to calculate restock schedule: {e}")
            return {
                "query": query,
                "answer": f"Error calculating restock schedule: {str(e)}",
                "command": "restock_schedule",
                "status": "error"
            }
    
    # Normal query processing with LLM
    try:
        # Get team's LLM configuration
        async with get_model_config() as config:
            team_config = await config.get_team_model_config(team_id)
            model = team_config["selected_model"]
            provider = team_config["provider"]
            provider_model = f"{provider}/{model}"
            model_config = team_config["config"]
            
            llm_params = {
                "model": provider_model,
                **model_config
            }
            
            auth_token = req.headers.get("Authorization")
            if auth_token:
                llm_params.update({"auth_token": auth_token})
            
            logger.info(f"Using model: {provider_model}")
            
    except Exception as e:
        logger.error(f"Failed to get team configuration: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get team configuration: {str(e)}"
        )
    
    try:
        logger.info(f"Current date: {current_date.strftime('%Y-%m-%d')} ({month_name} {current_year})")
        
        # If force_update is true, update inventory first
        if force_update:
            logger.info("Force update requested - updating inventory before query")
            await check_and_update_inventory(current_month, current_year)
        
        # Step 1: Get pre-calculated monthly summary
        logger.info("STEP 1: Getting pre-calculated monthly summary...")
        monthly_summary = await get_monthly_summary(current_month, current_year)
        
        logger.info(f"Monthly Summary Retrieved:")
        logger.info(f"  - Birthdays: {monthly_summary.get('birthdays', {}).get('total', 0)}")
        logger.info(f"  - Anniversaries: {monthly_summary.get('anniversaries', {}).get('total', 0)}")
        logger.info(f"  - Service Completions: {monthly_summary.get('service_completions', {}).get('total', 0)}")
        
        # Step 2: Retrieve ALL raw data from database
        logger.info("STEP 2: Retrieving all raw data from database...")
        all_data = await async_get_all_data()
        
        total_records = (
            len(all_data.get("milestone", [])) +
            len(all_data.get("welcome_kit", [])) +
            len(all_data.get("inventory", []))
        )
        
        logger.info(f"Retrieved data:")
        logger.info(f"  - Milestone: {len(all_data.get('milestone', []))} records")
        logger.info(f"  - Welcome Kit: {len(all_data.get('welcome_kit', []))} records")
        logger.info(f"  - Inventory: {len(all_data.get('inventory', []))} records")
        logger.info(f"  - Total: {total_records} records")
        
        if total_records == 0:
            logger.warning("No data found in any category")
            raise HTTPException(
                status_code=404,
                detail="No data found in database"
            )
        
        # Step 3: Format data for LLM with pre-calculated summaries
        logger.info("STEP 3: Formatting data for LLM...")
        
        # Limit raw data to avoid token overflow
        milestone_sample = all_data.get("milestone", [])[:100]
        welcome_kit_sample = all_data.get("welcome_kit", [])[:100]
        inventory_sample = all_data.get("inventory", [])[:100]
        
        # Step 4: Build LLM prompt with pre-calculated data
        logger.info("STEP 4: Building LLM prompt with pre-calculated summaries...")
        
        prompt = f"""You are an expert data analyst with access to employee milestone, welcome kit, and inventory data.

CURRENT DATE CONTEXT:
Today's Date: {current_date.strftime('%B %d, %Y')}
Current Month: {month_name} {current_year}
Current Month Number: {current_month}

PRE-CALCULATED MILESTONE SUMMARY FOR {month_name.upper()} {current_year}:
(These counts are already calculated - DO NOT recalculate them)

BIRTHDAYS IN {month_name.upper()}:
Total: {monthly_summary.get('birthdays', {}).get('total', 0)} employees
By Location: {json.dumps(monthly_summary.get('birthdays', {}).get('by_location', {}), indent=2)}

ANNIVERSARIES IN {month_name.upper()}:
Total: {monthly_summary.get('anniversaries', {}).get('total', 0)} employees
By Location: {json.dumps(monthly_summary.get('anniversaries', {}).get('by_location', {}), indent=2)}

SERVICE COMPLETIONS IN {month_name.upper()}:
Total: {monthly_summary.get('service_completions', {}).get('total', 0)} employees
By Location: {json.dumps(monthly_summary.get('service_completions', {}).get('by_location', {}), indent=2)}

LOW INVENTORY ALERTS (Below {monthly_summary.get('low_inventory_alerts', [{}])[0].get('threshold', 40) if monthly_summary.get('low_inventory_alerts') else 40} threshold):
{json.dumps(monthly_summary.get('low_inventory_alerts', []), indent=2)}

CURRENT INVENTORY DATA:
Sample of {len(inventory_sample)} inventory records showing:
- Location: Office location
- Workbook: Type of gift (Birthday, As on 03-10-25 [Anniversary], Service Completion)
- Quantity Received: Current inventory count
{json.dumps(inventory_sample[:10], indent=2, default=str)}

RAW MILESTONE DATA (for reference only):
Sample of {len(milestone_sample)} records with fields:
- Date of Birth (as per Records), MM Birth - WE Celebrate (month number)
- Date of Marriage
- Employment Details Date of Joining, MM Service Completion - WE Celebrate (month number)
- Place of posting, Base Location Name (location fields)
{json.dumps(milestone_sample[:3], indent=2, default=str)}

RAW WELCOME KIT DATA:
Sample of {len(welcome_kit_sample)} records
{json.dumps(welcome_kit_sample[:3], indent=2, default=str)}

ADVANCED FEATURES AVAILABLE:
The system can calculate:
1. Remaining gifts next month at any location
2. Months until restock is needed for any gift type
3. Complete restock schedules across all locations

If the user asks about future inventory, remaining gifts, or when to restock, you can mention these features are available via special queries.

USER QUESTION:
{query}

INSTRUCTIONS:
1. Use the PRE-CALCULATED MILESTONE SUMMARY for any birthday, anniversary, or service completion counts
2. DO NOT recalculate dates or counts - the summary above is authoritative
3. For inventory queries: Look at the inventory data and match by location and workbook type
4. For location matching: Common locations include:
   - "Indore-YASH IT Park-SC-DC" (also called "YASH IT Part", "YIT", etc.)
   - "Hyderabad-Mindspace I-DC"
   - "Indore-BTC-CO"
   And more (check the data for exact names)
5. When the user asks about birthdays, anniversaries, or service completions:
   - Use the pre-calculated summary above
   - Filter by location if they specify one
   - The current month is {month_name} {current_year}
6. For low inventory alerts: Use the LOW INVENTORY ALERTS section above
7. Provide clear, accurate answers based on the pre-calculated data
8. Do not use asterisks or special formatting in your response - use plain text
9. If the user asks about future projections or when to restock, mention they can use special queries like:
   - "remaining gifts [location]" - to see next month projections
   - "restock schedule [location]" - to see when restocking is needed
   - "all restock schedules" - to see restock needs for all locations

IMPORTANT NOTES:
- All milestone counts are pre-calculated for the current month ({month_name} {current_year})
- You should NEVER manually count dates or parse date fields
- Always use the summary data provided above
- If a location name in the query is slightly different, try to match it intelligently with available locations

Provide a clear, detailed, and accurate answer based on the available data."""

        logger.info(f"Built prompt ({len(prompt)} characters)")
        
        # Step 5: Call LLM
        logger.info("STEP 5: Calling LLM...")
        
        messages = [{"role": "user", "content": prompt}]
        
        try:
            # Remove auth_token from llm_params before passing to litellm
            auth_token = llm_params.pop("auth_token", "")
            
            # Call LLM
            response = litellm.completion(
                **llm_params,
                messages=messages
            )
            
            # Extract response
            llm_response = response.choices[0].message.content.strip()
            logger.info(f"LLM responded ({len(llm_response)} characters)")
            
            # Track token usage
            try:
                from src.utils.obs import LLMUsageTracker
                token_tracker = LLMUsageTracker()
                token_tracker.track_response(
                    response=response, 
                    auth_token=auth_token, 
                    model=llm_params.get("model", "")
                )
            except Exception as track_error:
                logger.warning(f"Failed to track token usage: {track_error}")
            
        except Exception as e:
            logger.exception(f"LLM call failed: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"LLM error: {str(e)}"
            )
        
        logger.info("=" * 80)
        logger.info("QUERY REQUEST COMPLETE")
        logger.info("=" * 80)
        
        return {
            "query": query,
            "answer": llm_response,
            "current_date": current_date.strftime('%Y-%m-%d'),
            "current_month": month_name,
            "milestone_summary": {
                "birthdays": monthly_summary.get('birthdays', {}).get('total', 0),
                "anniversaries": monthly_summary.get('anniversaries', {}).get('total', 0),
                "service_completions": monthly_summary.get('service_completions', {}).get('total', 0)
            },
            "data_summary": {
                "milestone_records": len(all_data.get("milestone", [])),
                "welcome_kit_records": len(all_data.get("welcome_kit", [])),
                "inventory_records": len(all_data.get("inventory", [])),
                "total_records": total_records
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Query processing failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Query processing failed: {str(e)}"
        )


@router.get("/categories")
async def get_categories():
    """Get list of available categories"""
    return {
        "categories": CATEGORIES,
        "description": {
            "milestone": "Employee milestone data (birthdays, anniversaries, service completion)",
            "welcome_kit": "Welcome kit data for new employees",
            "inventory": "Location-wise inventory data for gifts and supplies (Birthday, Anniversary, Service Completion)"
        },
        "features": {
            "automatic_calculations": "System automatically calculates milestone counts for current month",
            "real_time_date": "Always aware of current date and month",
            "inventory_updates": "Automated monthly inventory updates on 1st of each month",
            "low_inventory_alerts": "Automatic alerts when inventory drops below 40 units",
            "inventory_projections": "Calculate remaining gifts and restock schedules",
            "future_planning": "Project inventory needs for up to 24 months ahead"
        },
        "special_commands": {
            "update_inventory": "Query with 'update inventory' to manually trigger inventory update",
            "check_status": "Query with 'inventory status' to see current system status",
            "remaining_gifts": "Query with 'remaining gifts [location]' to see next month projection",
            "restock_schedule": "Query with 'restock schedule [location]' to see when restocking is needed",
            "all_restocks": "Query with 'all restock schedules' to see all location restock needs"
        },
        "note": "The /query endpoint automatically accesses all categories and provides pre-calculated milestone counts for the current month"
    }