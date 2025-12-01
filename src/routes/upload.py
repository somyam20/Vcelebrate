import asyncio
from fastapi import APIRouter, Form, HTTPException
from src.utils.db import async_save_category_data, async_init_db
from src.utils.s3_utils import async_download_to_bytes, extract_filename_from_url
from src.utils.dynamic_parser import async_parse_excel_dynamic
from src.utils.inventory_processor import check_and_update_inventory
from config.settings import CATEGORIES
import logging
import io

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload")
async def upload_file(
    s3_url: str = Form(...), 
    category: str = Form(...)
):
    """
    Upload S3 URL with category name, extract data dynamically and save to DB.
    
    Categories: milestone, welcome_kit, inventory
    
    For inventory files, multiple worksheets will be processed automatically.
    """
    logger.info("=" * 80)
    logger.info("UPLOAD REQUEST START")
    logger.info(f"S3 URL: {s3_url}")
    logger.info(f"Category: {category}")
    logger.info("=" * 80)
    
    # Validate inputs
    if not s3_url or not category:
        logger.error("Missing required fields")
        raise HTTPException(
            status_code=400, 
            detail="Both s3_url and category are required"
        )

    if not s3_url.startswith(("https://", "s3://")):
        logger.error(f"Invalid S3 URL format: {s3_url}")
        raise HTTPException(
            status_code=400,
            detail="s3_url must be a valid S3 URL (starting with https:// or s3://)"
        )
    
    # Validate category
    if category not in CATEGORIES:
        logger.error(f"Invalid category: {category}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Must be one of: {', '.join(CATEGORIES)}"
        )

    # Initialize DB
    await async_init_db()

    try:
        # Step 1: Download file from S3
        logger.info("STEP 1: Downloading from S3...")
        data_bytes = await async_download_to_bytes(s3_url)
        logger.info(f"✓ Downloaded {len(data_bytes)} bytes")
        
        # Extract filename
        filename = extract_filename_from_url(s3_url)
        logger.info(f"✓ Filename: {filename}")
        
        # Step 2: Parse file dynamically
        logger.info("STEP 2: Parsing file with dynamic headers...")
        parsed_data = await async_parse_excel_dynamic(
            io.BytesIO(data_bytes),
            filename,
            category
        )
        
        # Step 3: Save to database
        logger.info("STEP 3: Saving to database...")
        
        total_rows = 0
        
        # Check if this is an inventory file with multiple sheets
        if category == "inventory" and "sheets" in parsed_data:
            logger.info(f"→ Processing {len(parsed_data['sheets'])} inventory worksheets...")
            
            for sheet in parsed_data['sheets']:
                workbook = sheet['workbook']
                headers = sheet['headers']
                data = sheet['data']
                
                # Extract quarter from data if present
                quarter = None
                for row in data:
                    if len(row) > 0 and row[0]:
                        # Check if first column contains quarter info
                        first_val = str(row[0])
                        if any(q in first_val for q in ["Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]):
                            quarter = first_val
                            break
                
                logger.info(f"  → Saving workbook '{workbook}' (quarter: {quarter}), {len(data)} rows...")
                
                await async_save_category_data(
                    category=category,
                    s3_url=s3_url,
                    headers=headers,
                    data=data,
                    workbook=workbook,
                    quarter=quarter
                )
                
                total_rows += len(data)
                logger.info(f"  ✓ Saved {len(data)} rows for workbook '{workbook}'")
            
            response = {
                "message": "uploaded successfully",
                "category": category,
                "s3_url": s3_url,
                "workbooks_processed": len(parsed_data['sheets']),
                "total_rows_processed": total_rows,
                "workbooks": [sheet['workbook'] for sheet in parsed_data['sheets']]
            }
        else:
            # Single data file (milestone or welcome_kit)
            headers = parsed_data["headers"]
            data = parsed_data["data"]
            
            await async_save_category_data(category, s3_url, headers, data)
            total_rows = len(data)
            logger.info(f"✓ Saved {total_rows} rows to {category} table")
            
            response = {
                "message": "uploaded successfully",
                "category": category,
                "s3_url": s3_url,
                "rows_processed": total_rows,
                "headers": headers
            }
        
        # Step 4: If this is milestone or inventory upload, trigger inventory updates
        if category in ["milestone", "inventory"]:
            logger.info("STEP 4: Triggering inventory updates...")
            update_result = await check_and_update_inventory()
            response["inventory_updates"] = update_result
            logger.info(f"✓ Inventory updates completed: {update_result['status']}")
        
        logger.info("=" * 80)
        logger.info("UPLOAD REQUEST COMPLETE")
        logger.info("=" * 80)
        
        return response

    except Exception as e:
        logger.exception(f"✗ Upload failed: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Upload failed: {str(e)}"
        )