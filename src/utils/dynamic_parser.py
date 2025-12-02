import asyncio
import pandas as pd
import logging
from config.settings import BIRTHDAY_GIFTS_ROW, ANNIVERSARY_GIFTS_ROW, LOCATION_ALIASES
from .db import async_save_category_data
 
logger = logging.getLogger(__name__)
 
 
async def async_parse_excel_dynamic(file_obj, filename: str, category: str, s3_url: str = None):
    """Parse Excel/CSV file with dynamic header extraction"""
    logger.info(f"Starting async parse for file: {filename}, category: {category}")
   
    # For inventory category with special anniversary/birthday processing
    if category == "inventory" and (filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls")):
        logger.info("Detected inventory workbook - checking for special processing")
       
        try:
            # Read all sheet names to check for special sheets
            excel_file = pd.ExcelFile(file_obj)
            sheet_names = excel_file.sheet_names
           
            # Check if this contains the special "As On 03-10-25" sheet
            special_sheet = None
            for sheet_name in sheet_names:
                if "as on" in sheet_name.lower() and "03-10-25" in sheet_name:
                    special_sheet = sheet_name
                    break
           
            if special_sheet:
                logger.info(f"Found special anniversary/birthday sheet: {special_sheet}")
                # Use the async version with database saving
                result = await parse_anniversary_birthday_sheet(file_obj, special_sheet, "As on 03-10-25", s3_url)
                return {"sheets": result}
        except Exception as e:
            logger.warning(f"Failed special processing, falling back to standard: {e}")
   
    return await asyncio.to_thread(parse_excel_dynamic, file_obj, filename, category)
 
 
def parse_excel_dynamic(file_obj, filename: str, category: str):
    """
    Parse Excel or CSV file and extract headers dynamically.
    For inventory files, extract data from multiple worksheets.
    """
    logger.info("=" * 80)
    logger.info(f"PARSING FILE: {filename} (Category: {category})")
    logger.info("=" * 80)
   
    try:
        lower = filename.lower()
       
        # For inventory category, we need to handle multiple sheets
        if category == "inventory" and (lower.endswith(".xlsx") or lower.endswith(".xls")):
            logger.info("Detected inventory workbook - processing multiple sheets")
            return parse_inventory_workbook(file_obj, filename)
       
        # For other categories or CSV files
        if lower.endswith(".xlsx") or lower.endswith(".xls"):
            df = None
           
            logger.info("Attempting to detect header row...")
            # Try to detect header row automatically
            for header_row in [0, 1, 2]:
                try:
                    temp_df = pd.read_excel(file_obj, header=header_row)
                   
                    # Check if we have valid headers
                    unnamed_count = sum(1 for col in temp_df.columns if str(col).startswith("Unnamed"))
                    if unnamed_count < len(temp_df.columns) * 0.5:
                        df = temp_df
                        logger.info(f"✓ Found valid headers at row {header_row}")
                        break
                   
                    file_obj.seek(0)
                except Exception as e:
                    logger.debug(f"Failed to read with header_row={header_row}: {e}")
                    file_obj.seek(0)
           
            if df is None:
                df = pd.read_excel(file_obj, header=0)
                logger.warning("Using default header row (0)")
           
            # Drop unnamed leading columns
            while len(df.columns) > 0 and str(df.columns[0]).startswith("Unnamed"):
                logger.info(f"Dropping unnamed first column: {df.columns[0]}")
                df = df.iloc[:, 1:]
               
        elif lower.endswith(".csv"):
            logger.info("Reading CSV file")
            df = pd.read_csv(file_obj)
            logger.info("✓ CSV file read successfully")
        else:
            raise ValueError(f"Unsupported file type: {filename}")
       
        # Extract headers
        headers = [str(col).strip() for col in df.columns]
        logger.info(f"✓ Extracted {len(headers)} headers: {headers[:5]}..." if len(headers) > 5 else f"✓ Extracted {len(headers)} headers: {headers}")
       
        # Replace NaN, NaT, and inf values with None
        import numpy as np
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.replace([np.nan, np.inf, -np.inf], None)
       
        logger.info("✓ Cleaned NaN and inf values")
       
        # Convert dataframe to list of lists
        data = df.values.tolist()
       
        # Clean data - remove completely empty rows
        data = [row for row in data if any(val is not None and str(val).strip() for val in row if val is not None)]
       
        logger.info(f"✓ Parsed {len(data)} rows of data")
        if data:
            logger.info(f"Sample first row: {data[0][:3]}..." if len(data[0]) > 3 else f"Sample first row: {data[0]}")
        logger.info("=" * 80)
       
        return {
            "headers": headers,
            "data": data,
            "row_count": len(data),
            "column_count": len(headers),
            "workbook": None,
            "quarter": None
        }
       
    except Exception as e:
        logger.exception(f"✗ Failed to parse file: {e}")
        raise
 
 
def parse_inventory_workbook(file_obj, filename: str):
    """
    Parse inventory Excel file with multiple worksheets.
    For "As On 03-10-25" workbook, extract specific rows for birthday and anniversary gifts.
   
    Special handling:
    - Row 5 (index 4): Birthday gifts quantities by location
    - Row 8 (index 7): Anniversary gifts quantities by location
    """
    logger.info("=" * 80)
    logger.info("PARSING INVENTORY WORKBOOK WITH MULTIPLE SHEETS")
    logger.info("=" * 80)
   
    try:
        # Read all sheet names
        excel_file = pd.ExcelFile(file_obj)
        sheet_names = excel_file.sheet_names
        logger.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
       
        # Target workbook names
        target_workbooks = [
            "Birthday",
            "As On 03-10-25",
            "As on 03-10-25",
            "Service Completion"
        ]
       
        all_sheets_data = []
       
        for sheet_name in sheet_names:
            # Check if this sheet is one of our target workbooks
            sheet_name_clean = sheet_name.strip()
            is_target = False
            matched_name = None
           
            logger.info(f"Checking sheet: '{sheet_name}'")
           
            for target in target_workbooks:
                if sheet_name_clean.lower() == target.lower():
                    is_target = True
                    # Use the standardized name
                    if "birthday" in target.lower():
                        matched_name = "Birthday"
                    elif "as on" in target.lower():
                        matched_name = "As on 03-10-25"
                    elif "service" in target.lower():
                        matched_name = "Service Completion"
                    break
           
            if not is_target:
                logger.info(f"  ⊘ Skipping sheet: {sheet_name} (not a target workbook)")
                continue
           
            logger.info(f"  → Processing sheet: '{sheet_name}' (mapped to: '{matched_name}')")
           
            # Special handling for "As On 03-10-25" workbook
            if matched_name == "As on 03-10-25":
                logger.info(f"  ★ Special processing for Anniversary/Birthday gifts workbook")
                # Note: This is called from sync context, so we can't use async here
                # The async version should be called from the main parsing function
                result = parse_anniversary_birthday_sheet_sync(file_obj, sheet_name, matched_name)
                all_sheets_data.extend(result)
                continue
           
            # Standard processing for other sheets
            df = None
            for header_row in [0, 1, 2]:
                try:
                    file_obj.seek(0)
                    temp_df = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_row)
                   
                    # Check if we have valid headers
                    unnamed_count = sum(1 for col in temp_df.columns if str(col).startswith("Unnamed"))
                    if unnamed_count < len(temp_df.columns) * 0.5:
                        df = temp_df
                        logger.info(f"    ✓ Found valid headers at row {header_row}")
                        break
                except Exception as e:
                    logger.debug(f"    Failed with header_row={header_row}: {e}")
           
            if df is None:
                logger.info(f"    Using default header row (0)")
                file_obj.seek(0)
                df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
           
            # Extract quarter from first column if it exists
            quarter = None
            if len(df.columns) > 0:
                first_col_name = str(df.columns[0])
                if "quarter" in first_col_name.lower():
                    if len(df) > 0:
                        quarter = str(df.iloc[0, 0])
                        logger.info(f"    ✓ Detected quarter: {quarter}")
           
            # Drop unnamed leading columns
            while len(df.columns) > 0 and str(df.columns[0]).startswith("Unnamed"):
                logger.info(f"    Dropping unnamed column: {df.columns[0]}")
                df = df.iloc[:, 1:]
           
            # Extract headers
            headers = [str(col).strip() for col in df.columns]
            logger.info(f"    ✓ Extracted {len(headers)} headers")
           
            # Replace NaN, NaT, and inf values with None
            import numpy as np
            df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
            df = df.replace([np.nan, np.inf, -np.inf], None)
           
            # Convert dataframe to list of lists
            data = df.values.tolist()
           
            # Clean data
            data = [row for row in data if any(val is not None and str(val).strip() if val is not None else False for val in row)]
           
            logger.info(f"    ✓ Parsed {len(data)} rows from sheet '{sheet_name}'")
           
            # Store this sheet's data
            all_sheets_data.append({
                "workbook": matched_name,
                "headers": headers,
                "data": data,
                "row_count": len(data),
                "column_count": len(headers),
                "quarter": quarter
            })
       
        logger.info("=" * 80)
        logger.info(f"✓ Successfully parsed {len(all_sheets_data)} workbook sheets:")
        for sheet in all_sheets_data:
            logger.info(f"    - {sheet['workbook']}: {sheet['row_count']} rows, quarter: {sheet.get('quarter', 'N/A')}")
        logger.info("=" * 80)
       
        if len(all_sheets_data) == 0:
            logger.warning("⚠ WARNING: No target worksheets found!")
            logger.warning(f"Available sheets: {sheet_names}")
            logger.warning(f"Looking for: {target_workbooks}")
       
        return {"sheets": all_sheets_data}
       
    except Exception as e:
        logger.exception(f"✗ Failed to parse inventory workbook: {e}")
        raise
 
 
def normalize_location(location_name: str) -> str:
    """Normalize location name using LOCATION_ALIASES mapping"""
    if not location_name or pd.isna(location_name):
        return "Unknown"
   
    location_str = str(location_name).strip()
    return LOCATION_ALIASES.get(location_str, location_str)
 
 
async def parse_anniversary_birthday_sheet(file_obj, sheet_name: str, matched_name: str, s3_url: str = None):
    """
    Special parser for "As On 03-10-25" workbook.
    Extracts birthday gifts from row 5 and anniversary gifts from row 8.
    Maps locations using LOCATION_ALIASES and saves to database.
   
    Returns a list with two sheet data objects:
    1. Birthday gifts data
    2. Anniversary gifts data
    """
    logger.info(f"    ★★★ SPECIAL ROW-BASED EXTRACTION WITH LOCATION MAPPING ★★★")
    logger.info(f"    Birthday gifts row: {BIRTHDAY_GIFTS_ROW} (index {BIRTHDAY_GIFTS_ROW-1})")
    logger.info(f"    Anniversary gifts row: {ANNIVERSARY_GIFTS_ROW} (index {ANNIVERSARY_GIFTS_ROW-1})")
   
    try:
        file_obj.seek(0)
        # Read without header to get raw data
        df_raw = pd.read_excel(file_obj, sheet_name=sheet_name, header=None)
       
        logger.info(f"    Raw sheet dimensions: {df_raw.shape[0]} rows × {df_raw.shape[1]} columns")
       
        # Read with header to get column names
        file_obj.seek(0)
        df_header = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        headers = [str(col).strip() for col in df_header.columns]
       
        logger.info(f"    Headers: {headers}")
       
        result_sheets = []
       
        # Extract Birthday gifts (row 5, index 4)
        if len(df_raw) >= BIRTHDAY_GIFTS_ROW:
            birthday_row = df_raw.iloc[BIRTHDAY_GIFTS_ROW - 1].tolist()
            logger.info(f"    ✓ Extracted Birthday row {BIRTHDAY_GIFTS_ROW}: {birthday_row[:5]}...")
           
            # Map locations and create structured data
            mapped_birthday_data = []
            for i, (header, value) in enumerate(zip(headers, birthday_row)):
                if value is not None and str(value).strip() and not pd.isna(value):
                    normalized_location = normalize_location(header)
                    mapped_birthday_data.append({
                        "original_location": header,
                        "normalized_location": normalized_location,
                        "quantity": value,
                        "gift_type": "Birthday"
                    })
                    logger.info(f"      Location mapping: '{header}' → '{normalized_location}' (qty: {value})")
           
            # Save to database if s3_url provided
            if s3_url and mapped_birthday_data:
                logger.info(f"    Saving {len(mapped_birthday_data)} Birthday gift records to database...")
                birthday_headers = ["original_location", "normalized_location", "quantity", "gift_type"]
                birthday_data_rows = [[item["original_location"], item["normalized_location"], item["quantity"], item["gift_type"]] for item in mapped_birthday_data]
                await async_save_category_data("inventory", s3_url, birthday_headers, birthday_data_rows, "Birthday", None)
                logger.info(f"    ✓ Saved Birthday gifts to database")
           
            result_sheets.append({
                "workbook": "Birthday",
                "headers": headers,
                "data": [birthday_row],
                "mapped_data": mapped_birthday_data,
                "row_count": 1,
                "column_count": len(headers),
                "quarter": None,
                "special_row": BIRTHDAY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Birthday gifts dataset with {len(mapped_birthday_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df_raw)} rows, cannot extract Birthday row {BIRTHDAY_GIFTS_ROW}")
       
        # Extract Anniversary gifts (row 8, index 7)
        if len(df_raw) >= ANNIVERSARY_GIFTS_ROW:
            anniversary_row = df_raw.iloc[ANNIVERSARY_GIFTS_ROW - 1].tolist()
            logger.info(f"    ✓ Extracted Anniversary row {ANNIVERSARY_GIFTS_ROW}: {anniversary_row[:5]}...")
           
            # Map locations and create structured data
            mapped_anniversary_data = []
            for i, (header, value) in enumerate(zip(headers, anniversary_row)):
                if value is not None and str(value).strip() and not pd.isna(value):
                    normalized_location = normalize_location(header)
                    mapped_anniversary_data.append({
                        "original_location": header,
                        "normalized_location": normalized_location,
                        "quantity": value,
                        "gift_type": "Anniversary"
                    })
                    logger.info(f"      Location mapping: '{header}' → '{normalized_location}' (qty: {value})")
           
            # Save to database if s3_url provided
            if s3_url and mapped_anniversary_data:
                logger.info(f"    Saving {len(mapped_anniversary_data)} Anniversary gift records to database...")
                anniversary_headers = ["original_location", "normalized_location", "quantity", "gift_type"]
                anniversary_data_rows = [[item["original_location"], item["normalized_location"], item["quantity"], item["gift_type"]] for item in mapped_anniversary_data]
                await async_save_category_data("inventory", s3_url, anniversary_headers, anniversary_data_rows, "As on 03-10-25", None)
                logger.info(f"    ✓ Saved Anniversary gifts to database")
           
            result_sheets.append({
                "workbook": "As on 03-10-25",
                "headers": headers,
                "data": [anniversary_row],
                "mapped_data": mapped_anniversary_data,
                "row_count": 1,
                "column_count": len(headers),
                "quarter": None,
                "special_row": ANNIVERSARY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Anniversary gifts dataset with {len(mapped_anniversary_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df_raw)} rows, cannot extract Anniversary row {ANNIVERSARY_GIFTS_ROW}")
       
        logger.info(f"    ★★★ SPECIAL EXTRACTION COMPLETE: {len(result_sheets)} datasets created ★★★")
        return result_sheets
       
    except Exception as e:
        logger.exception(f"    ✗ Failed special row extraction: {e}")
        raise
 
 
def parse_anniversary_birthday_sheet_sync(file_obj, sheet_name: str, matched_name: str):
    """
    Synchronous version of parse_anniversary_birthday_sheet for use in sync contexts.
    This version doesn't save to database - just extracts and maps the data.
    """
    logger.info(f"    ★★★ SYNC SPECIAL ROW-BASED EXTRACTION WITH LOCATION MAPPING ★★★")
    logger.info(f"    Birthday gifts row: {BIRTHDAY_GIFTS_ROW} (index {BIRTHDAY_GIFTS_ROW-1})")
    logger.info(f"    Anniversary gifts row: {ANNIVERSARY_GIFTS_ROW} (index {ANNIVERSARY_GIFTS_ROW-1})")
   
    try:
        file_obj.seek(0)
        # Read without header to get raw data
        df_raw = pd.read_excel(file_obj, sheet_name=sheet_name, header=None)
       
        logger.info(f"    Raw sheet dimensions: {df_raw.shape[0]} rows × {df_raw.shape[1]} columns")
       
        # Read with header to get column names
        file_obj.seek(0)
        df_header = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        headers = [str(col).strip() for col in df_header.columns]
       
        logger.info(f"    Headers: {headers}")
       
        result_sheets = []
       
        # Extract Birthday gifts (row 5, index 4)
        if len(df_raw) >= BIRTHDAY_GIFTS_ROW:
            birthday_row = df_raw.iloc[BIRTHDAY_GIFTS_ROW - 1].tolist()
            logger.info(f"    ✓ Extracted Birthday row {BIRTHDAY_GIFTS_ROW}: {birthday_row[:5]}...")
           
            # Map locations and create structured data
            mapped_birthday_data = []
            for i, (header, value) in enumerate(zip(headers, birthday_row)):
                if value is not None and str(value).strip() and not pd.isna(value):
                    normalized_location = normalize_location(header)
                    mapped_birthday_data.append({
                        "original_location": header,
                        "normalized_location": normalized_location,
                        "quantity": value,
                        "gift_type": "Birthday"
                    })
                    logger.info(f"      Location mapping: '{header}' → '{normalized_location}' (qty: {value})")
           
            result_sheets.append({
                "workbook": "Birthday",
                "headers": headers,
                "data": [birthday_row],
                "mapped_data": mapped_birthday_data,
                "row_count": 1,
                "column_count": len(headers),
                "quarter": None,
                "special_row": BIRTHDAY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Birthday gifts dataset with {len(mapped_birthday_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df_raw)} rows, cannot extract Birthday row {BIRTHDAY_GIFTS_ROW}")
       
        # Extract Anniversary gifts (row 8, index 7)
        if len(df_raw) >= ANNIVERSARY_GIFTS_ROW:
            anniversary_row = df_raw.iloc[ANNIVERSARY_GIFTS_ROW - 1].tolist()
            logger.info(f"    ✓ Extracted Anniversary row {ANNIVERSARY_GIFTS_ROW}: {anniversary_row[:5]}...")
           
            # Map locations and create structured data
            mapped_anniversary_data = []
            for i, (header, value) in enumerate(zip(headers, anniversary_row)):
                if value is not None and str(value).strip() and not pd.isna(value):
                    normalized_location = normalize_location(header)
                    mapped_anniversary_data.append({
                        "original_location": header,
                        "normalized_location": normalized_location,
                        "quantity": value,
                        "gift_type": "Anniversary"
                    })
                    logger.info(f"      Location mapping: '{header}' → '{normalized_location}' (qty: {value})")
           
            result_sheets.append({
                "workbook": "As on 03-10-25",
                "headers": headers,
                "data": [anniversary_row],
                "mapped_data": mapped_anniversary_data,
                "row_count": 1,
                "column_count": len(headers),
                "quarter": None,
                "special_row": ANNIVERSARY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Anniversary gifts dataset with {len(mapped_anniversary_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df_raw)} rows, cannot extract Anniversary row {ANNIVERSARY_GIFTS_ROW}")
       
        logger.info(f"    ★★★ SYNC SPECIAL EXTRACTION COMPLETE: {len(result_sheets)} datasets created ★★★")
        return result_sheets
       
    except Exception as e:
        logger.exception(f"    ✗ Failed sync special row extraction: {e}")
        raise