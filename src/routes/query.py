import asyncio
import json
from fastapi import APIRouter, HTTPException, Form, Request
from src.utils.db import async_get_category_data
from config.config import get_model_config
from config.settings import CATEGORIES
import logging
import litellm

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/query")
async def query_data(
    req: Request,
    category: str = Form(...),
    query: str = Form(...),
    user_metadata: str = Form(...)
):
    """
    Query category-wise data using LLM.
    
    Categories: milestone, welcome_kit, inventory
    
    Usage with curl:
    curl -X POST "http://localhost:8000/api/query" \
         -H "Authorization: Bearer token" \
         -F "category=inventory" \
         -F "query=how many birthday gifts are available?" \
         -F "user_metadata={\"team_id\":\"team123\"}"
    """
    logger.info("=" * 80)
    logger.info("QUERY REQUEST START")
    logger.info(f"Category: {category}")
    logger.info(f"Query: {query}")
    logger.info("=" * 80)
    
    # Validate category
    if category not in CATEGORIES:
        logger.error(f"Invalid category: {category}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Must be one of: {', '.join(CATEGORIES)}"
        )
    
    # Parse user metadata
    user_metadata_dict = json.loads(user_metadata) if user_metadata else {}
    team_id = user_metadata_dict.get("team_id")
    
    if not team_id:
        logger.error("Missing team_id in user_metadata")
        raise HTTPException(
            status_code=400,
            detail="team_id is required in user_metadata"
        )
    
    try:
        # Get team's LLM configuration
        async with get_model_config() as config:
            team_config = await config.get_team_model_config(team_id)
            model = team_config["selected_model"]
            provider = team_config["provider"]
            provider_model = f"{provider}/{model}"
            model_config = team_config["config"]
            
            # Create LLM params
            llm_params = {
                "model": provider_model,
                **model_config
            }
            
            # Get auth token from headers
            auth_token = req.headers.get("Authorization")
            if auth_token:
                llm_params.update({"auth_token": auth_token})
            
            logger.info(f"✓ Using model: {provider_model}")
            
    except Exception as e:
        logger.error(f"Failed to get team configuration: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get team configuration: {str(e)}"
        )
    
    try:
        # Step 1: Retrieve category data from database
        logger.info(f"STEP 1: Retrieving {category} data from database...")
        category_data = await async_get_category_data(category)
        
        if not category_data:
            logger.warning(f"⚠ No data found for category: {category}")
            raise HTTPException(
                status_code=404,
                detail=f"No data found for category: {category}"
            )
        
        logger.info(f"✓ Retrieved {len(category_data)} records")
        
        # Step 2: Format data for LLM
        logger.info("STEP 2: Formatting data for LLM...")
        
        # Convert data to a readable format
        formatted_data = []
        for idx, record in enumerate(category_data[:100]):  # Limit to first 100 records
            if category == "inventory":
                formatted_data.append({
                    "location": record.get("location"),
                    "workbook": record.get("workbook"),
                    "data": json.loads(record["data"]) if isinstance(record["data"], str) else record["data"]
                })
            else:
                formatted_data.append(
                    json.loads(record["data"]) if isinstance(record["data"], str) else record["data"]
                )
        
        data_text = json.dumps(formatted_data, indent=2, default=str)
        logger.info(f"✓ Formatted {len(formatted_data)} records ({len(data_text)} characters)")
        
        # Step 3: Build LLM prompt
        logger.info("STEP 3: Building LLM prompt...")
        
        prompt = f"""You are an expert data analyst. Analyze the following {category} data and answer the user's question.

DATA:
{data_text}

USER QUESTION:
{query}

Provide a clear, detailed answer based on the data above. If you need to perform calculations or aggregations, do so accurately. Format your response in a professional, easy-to-read manner."""

        logger.info(f"✓ Built prompt ({len(prompt)} characters)")
        
        # Step 4: Call LLM
        logger.info("STEP 4: Calling LLM...")
        
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
            logger.info(f"✓ LLM responded ({len(llm_response)} characters)")
            
            # Track token usage (if you have tracker)
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
            logger.exception(f"✗ LLM call failed: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"LLM error: {str(e)}"
            )
        
        logger.info("=" * 80)
        logger.info("QUERY REQUEST COMPLETE")
        logger.info("=" * 80)
        
        return {
            "category": category,
            "query": query,
            "answer": llm_response,
            "records_analyzed": len(formatted_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"✗ Query processing failed: {e}")
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
            "inventory": "Location-wise inventory data for gifts and supplies"
        }
    }