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
            
            # Check for special sheets
            special_anniversary_sheet = None
            service_completion_sheet = None
            
            for sheet_name in sheet_names:
                if "as on" in sheet_name.lower() and ("03-10-25" in sheet_name or "30-09" in sheet_name or "30.09" in sheet_name):
                    special_anniversary_sheet = sheet_name
                elif "service completion" in sheet_name.lower():
                    service_completion_sheet = sheet_name
            
            result_sheets = []
            
            # Process anniversary/birthday sheet
            if special_anniversary_sheet:
                logger.info(f"Found special anniversary/birthday sheet: {special_anniversary_sheet}")
                anniversary_result = await parse_anniversary_birthday_sheet(file_obj, special_anniversary_sheet, "As on 03-10-25", s3_url)
                result_sheets.extend(anniversary_result)
            
            # Process service completion sheet
            if service_completion_sheet:
                logger.info(f"Found service completion sheet: {service_completion_sheet}")
                service_result = await parse_service_completion_sheet(file_obj, service_completion_sheet, "Service Completion", s3_url)
                result_sheets.append(service_result)
            
            if result_sheets:
                return {"sheets": result_sheets}
                
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
    
    Special handling:
    - "As On 03-10-25" workbook: Extract rows 5 (birthday) and 8 (anniversary) with location columns
    - "Service Completion" workbook: Extract Quarter, Location, and Quantity Received columns
    - Other workbooks: Standard extraction
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
                result = parse_anniversary_birthday_sheet_sync(file_obj, sheet_name, matched_name)
                all_sheets_data.extend(result)
                continue
            
            # Special handling for "Service Completion" workbook
            if matched_name == "Service Completion":
                logger.info(f"  ★ Special processing for Service Completion workbook")
                result = parse_service_completion_sheet_sync(file_obj, sheet_name, matched_name)
                all_sheets_data.append(result)
                continue
            
            # Standard processing for other sheets (Birthday, etc.)
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
    
    FIXED: Now correctly reads location names from the actual column headers
    instead of treating them as data rows.
    """
    logger.info(f"    ★★★ SPECIAL ROW-BASED EXTRACTION WITH LOCATION MAPPING ★★★")
    logger.info(f"    Birthday gifts row: {BIRTHDAY_GIFTS_ROW} (index {BIRTHDAY_GIFTS_ROW-1})")
    logger.info(f"    Anniversary gifts row: {ANNIVERSARY_GIFTS_ROW} (index {ANNIVERSARY_GIFTS_ROW-1})")
    
    try:
        file_obj.seek(0)
        # Read the sheet with the FIRST row as headers (row 0)
        # This will give us the actual location names as column headers
        df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        
        # Get the column names (these are the location names)
        location_headers = [str(col).strip() for col in df.columns]
        logger.info(f"    Location headers (columns): {location_headers}")
        
        # Filter out columns that start with "Unnamed" or are "Description" or "Event"
        valid_locations = []
        valid_indices = []
        for idx, header in enumerate(location_headers):
            if not header.startswith("Unnamed") and header not in ["Description", "Event"]:
                valid_locations.append(header)
                valid_indices.append(idx)
        
        logger.info(f"    Valid location columns: {valid_locations}")
        
        result_sheets = []
        
        # Extract Birthday gifts (row 5, but in DataFrame it's index 4 since we used header=0)
        # The actual row 5 in Excel becomes index 4 in the DataFrame (0-indexed after header)
        birthday_df_index = BIRTHDAY_GIFTS_ROW - 1 - 1  # -1 for 1-based to 0-based, -1 for header row
        
        if len(df) > birthday_df_index:
            logger.info(f"    Extracting Birthday row at DataFrame index {birthday_df_index}")
            
            mapped_birthday_data = []
            for idx, location in zip(valid_indices, valid_locations):
                quantity = df.iloc[birthday_df_index, idx]
                
                # Skip if quantity is None, NaN, or empty
                if quantity is not None and not pd.isna(quantity) and str(quantity).strip():
                    try:
                        quantity_int = int(float(quantity))
                        if quantity_int > 0:  # Only include non-zero quantities
                            normalized_location = normalize_location(location)
                            mapped_birthday_data.append({
                                "original_location": location,
                                "normalized_location": normalized_location,
                                "quantity": quantity_int,
                                "gift_type": "Birthday"
                            })
                            logger.info(f"      Location mapping: '{location}' → '{normalized_location}' (qty: {quantity_int})")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"      Skipping invalid quantity for {location}: {quantity}")
            
            # Save to database if s3_url provided
            if s3_url and mapped_birthday_data:
                logger.info(f"    Saving {len(mapped_birthday_data)} Birthday gift records to database...")
                birthday_headers = ["original_location", "normalized_location", "quantity", "gift_type"]
                birthday_data_rows = [[item["original_location"], item["normalized_location"], item["quantity"], item["gift_type"]] for item in mapped_birthday_data]
                await async_save_category_data("inventory", s3_url, birthday_headers, birthday_data_rows, "Birthday", None)
                logger.info(f"    ✓ Saved Birthday gifts to database")
            
            result_sheets.append({
                "workbook": "Birthday",
                "headers": valid_locations,
                "data": [[df.iloc[birthday_df_index, idx] for idx in valid_indices]],
                "mapped_data": mapped_birthday_data,
                "row_count": 1,
                "column_count": len(valid_locations),
                "quarter": None,
                "special_row": BIRTHDAY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Birthday gifts dataset with {len(mapped_birthday_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df)} rows, cannot extract Birthday row {BIRTHDAY_GIFTS_ROW}")
        
        # Extract Anniversary gifts (row 8, DataFrame index 6)
        anniversary_df_index = ANNIVERSARY_GIFTS_ROW - 1 - 1
        
        if len(df) > anniversary_df_index:
            logger.info(f"    Extracting Anniversary row at DataFrame index {anniversary_df_index}")
            
            mapped_anniversary_data = []
            for idx, location in zip(valid_indices, valid_locations):
                quantity = df.iloc[anniversary_df_index, idx]
                
                # Skip if quantity is None, NaN, or empty
                if quantity is not None and not pd.isna(quantity) and str(quantity).strip():
                    try:
                        quantity_int = int(float(quantity))
                        if quantity_int > 0:  # Only include non-zero quantities
                            normalized_location = normalize_location(location)
                            mapped_anniversary_data.append({
                                "original_location": location,
                                "normalized_location": normalized_location,
                                "quantity": quantity_int,
                                "gift_type": "Anniversary"
                            })
                            logger.info(f"      Location mapping: '{location}' → '{normalized_location}' (qty: {quantity_int})")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"      Skipping invalid quantity for {location}: {quantity}")
            
            # Save to database if s3_url provided
            if s3_url and mapped_anniversary_data:
                logger.info(f"    Saving {len(mapped_anniversary_data)} Anniversary gift records to database...")
                anniversary_headers = ["original_location", "normalized_location", "quantity", "gift_type"]
                anniversary_data_rows = [[item["original_location"], item["normalized_location"], item["quantity"], item["gift_type"]] for item in mapped_anniversary_data]
                await async_save_category_data("inventory", s3_url, anniversary_headers, anniversary_data_rows, "As on 03-10-25", None)
                logger.info(f"    ✓ Saved Anniversary gifts to database")
            
            result_sheets.append({
                "workbook": "As on 03-10-25",
                "headers": valid_locations,
                "data": [[df.iloc[anniversary_df_index, idx] for idx in valid_indices]],
                "mapped_data": mapped_anniversary_data,
                "row_count": 1,
                "column_count": len(valid_locations),
                "quarter": None,
                "special_row": ANNIVERSARY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Anniversary gifts dataset with {len(mapped_anniversary_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df)} rows, cannot extract Anniversary row {ANNIVERSARY_GIFTS_ROW}")
        
        logger.info(f"    ★★★ SPECIAL EXTRACTION COMPLETE: {len(result_sheets)} datasets created ★★★")
        return result_sheets
        
    except Exception as e:
        logger.exception(f"    ✗ Failed special row extraction: {e}")
        raise


def parse_anniversary_birthday_sheet_sync(file_obj, sheet_name: str, matched_name: str):
    """
    Synchronous version of parse_anniversary_birthday_sheet for use in sync contexts.
    This version doesn't save to database - just extracts and maps the data.
    
    FIXED: Now correctly reads location names from the actual column headers.
    """
    logger.info(f"    ★★★ SYNC SPECIAL ROW-BASED EXTRACTION WITH LOCATION MAPPING ★★★")
    logger.info(f"    Birthday gifts row: {BIRTHDAY_GIFTS_ROW} (index {BIRTHDAY_GIFTS_ROW-1})")
    logger.info(f"    Anniversary gifts row: {ANNIVERSARY_GIFTS_ROW} (index {ANNIVERSARY_GIFTS_ROW-1})")
    
    try:
        file_obj.seek(0)
        # Read the sheet with the FIRST row as headers (row 0)
        df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        
        # Get the column names (these are the location names)
        location_headers = [str(col).strip() for col in df.columns]
        logger.info(f"    Location headers (columns): {location_headers}")
        
        # Filter out columns that start with "Unnamed" or are "Description" or "Event"
        valid_locations = []
        valid_indices = []
        for idx, header in enumerate(location_headers):
            if not header.startswith("Unnamed") and header not in ["Description", "Event"]:
                valid_locations.append(header)
                valid_indices.append(idx)
        
        logger.info(f"    Valid location columns: {valid_locations}")
        
        result_sheets = []
        
        # Extract Birthday gifts
        birthday_df_index = BIRTHDAY_GIFTS_ROW - 1 - 1
        
        if len(df) > birthday_df_index:
            logger.info(f"    Extracting Birthday row at DataFrame index {birthday_df_index}")
            
            mapped_birthday_data = []
            for idx, location in zip(valid_indices, valid_locations):
                quantity = df.iloc[birthday_df_index, idx]
                
                if quantity is not None and not pd.isna(quantity) and str(quantity).strip():
                    try:
                        quantity_int = int(float(quantity))
                        if quantity_int > 0:
                            normalized_location = normalize_location(location)
                            mapped_birthday_data.append({
                                "original_location": location,
                                "normalized_location": normalized_location,
                                "quantity": quantity_int,
                                "gift_type": "Birthday"
                            })
                            logger.info(f"      Location mapping: '{location}' → '{normalized_location}' (qty: {quantity_int})")
                    except (ValueError, TypeError):
                        logger.warning(f"      Skipping invalid quantity for {location}: {quantity}")
            
            result_sheets.append({
                "workbook": "Birthday",
                "headers": valid_locations,
                "data": [[df.iloc[birthday_df_index, idx] for idx in valid_indices]],
                "mapped_data": mapped_birthday_data,
                "row_count": 1,
                "column_count": len(valid_locations),
                "quarter": None,
                "special_row": BIRTHDAY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Birthday gifts dataset with {len(mapped_birthday_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df)} rows, cannot extract Birthday row {BIRTHDAY_GIFTS_ROW}")
        
        # Extract Anniversary gifts
        anniversary_df_index = ANNIVERSARY_GIFTS_ROW - 1 - 1
        
        if len(df) > anniversary_df_index:
            logger.info(f"    Extracting Anniversary row at DataFrame index {anniversary_df_index}")
            
            mapped_anniversary_data = []
            for idx, location in zip(valid_indices, valid_locations):
                quantity = df.iloc[anniversary_df_index, idx]
                
                if quantity is not None and not pd.isna(quantity) and str(quantity).strip():
                    try:
                        quantity_int = int(float(quantity))
                        if quantity_int > 0:
                            normalized_location = normalize_location(location)
                            mapped_anniversary_data.append({
                                "original_location": location,
                                "normalized_location": normalized_location,
                                "quantity": quantity_int,
                                "gift_type": "Anniversary"
                            })
                            logger.info(f"      Location mapping: '{location}' → '{normalized_location}' (qty: {quantity_int})")
                    except (ValueError, TypeError):
                        logger.warning(f"      Skipping invalid quantity for {location}: {quantity}")
            
            result_sheets.append({
                "workbook": "As on 03-10-25",
                "headers": valid_locations,
                "data": [[df.iloc[anniversary_df_index, idx] for idx in valid_indices]],
                "mapped_data": mapped_anniversary_data,
                "row_count": 1,
                "column_count": len(valid_locations),
                "quarter": None,
                "special_row": ANNIVERSARY_GIFTS_ROW
            })
            logger.info(f"    ✓ Created Anniversary gifts dataset with {len(mapped_anniversary_data)} location mappings")
        else:
            logger.warning(f"    ⚠ Sheet has only {len(df)} rows, cannot extract Anniversary row {ANNIVERSARY_GIFTS_ROW}")
        
        logger.info(f"    ★★★ SYNC SPECIAL EXTRACTION COMPLETE: {len(result_sheets)} datasets created ★★★")
        return result_sheets
        
    except Exception as e:
        logger.exception(f"    ✗ Failed sync special row extraction: {e}")
        raise


def parse_service_completion_sheet_sync(file_obj, sheet_name: str, matched_name: str):
    """
    Parse Service Completion workbook.
    Extracts Quarter, Location, and Quantity Received columns.
    Groups data by quarter and normalizes location names.
    """
    logger.info(f"    ★★★ SERVICE COMPLETION WORKBOOK EXTRACTION ★★★")
    
    try:
        file_obj.seek(0)
        # Read the sheet with header row
        df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        logger.info(f"    Columns: {list(df.columns)}")
        
        # Find the relevant columns (case-insensitive)
        quarter_col = None
        location_col = None
        quantity_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'quarter' in col_lower:
                quarter_col = col
            elif 'location' in col_lower:
                location_col = col
            elif 'quantity' in col_lower and 'received' in col_lower:
                quantity_col = col
        
        if not all([quarter_col, location_col, quantity_col]):
            logger.error(f"    ✗ Missing required columns!")
            logger.error(f"    Quarter column: {quarter_col}")
            logger.error(f"    Location column: {location_col}")
            logger.error(f"    Quantity Received column: {quantity_col}")
            raise ValueError("Service Completion sheet missing required columns: Quarter, Location, or Quantity Received")
        
        logger.info(f"    ✓ Found required columns:")
        logger.info(f"      Quarter: '{quarter_col}'")
        logger.info(f"      Location: '{location_col}'")
        logger.info(f"      Quantity Received: '{quantity_col}'")
        
        # Clean the data
        import numpy as np
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Filter out rows with empty essential data
        df_clean = df[
            df[quarter_col].notna() & 
            df[location_col].notna() & 
            df[quantity_col].notna()
        ].copy()
        
        logger.info(f"    ✓ Cleaned data: {len(df_clean)} valid rows (from {len(df)} total)")
        
        # Normalize locations and prepare structured data
        mapped_data = []
        for idx, row in df_clean.iterrows():
            quarter = str(row[quarter_col]).strip()
            original_location = str(row[location_col]).strip()
            
            try:
                quantity_received = int(float(row[quantity_col]))
            except (ValueError, TypeError):
                logger.warning(f"      Skipping row {idx}: invalid quantity '{row[quantity_col]}'")
                continue
            
            # Normalize location
            normalized_location = normalize_location(original_location)
            
            mapped_data.append({
                "quarter": quarter,
                "original_location": original_location,
                "normalized_location": normalized_location,
                "quantity_received": quantity_received,
                "gift_type": "Service Completion"
            })
            
            logger.debug(f"      {quarter} | '{original_location}' → '{normalized_location}' | Qty: {quantity_received}")
        
        logger.info(f"    ✓ Processed {len(mapped_data)} service completion records")
        
        # Group by quarter for summary
        quarters = {}
        for record in mapped_data:
            q = record['quarter']
            if q not in quarters:
                quarters[q] = 0
            quarters[q] += record['quantity_received']
        
        logger.info(f"    Summary by quarter:")
        for q, total in sorted(quarters.items()):
            logger.info(f"      {q}: {total} total gifts")
        
        # Convert mapped_data to the format expected by the database
        headers = ["quarter", "original_location", "normalized_location", "quantity_received", "gift_type"]
        data_rows = [[
            item["quarter"],
            item["original_location"],
            item["normalized_location"],
            item["quantity_received"],
            item["gift_type"]
        ] for item in mapped_data]
        
        logger.info(f"    ★★★ SERVICE COMPLETION EXTRACTION COMPLETE ★★★")
        
        return {
            "workbook": "Service Completion",
            "headers": headers,
            "data": data_rows,
            "mapped_data": mapped_data,
            "row_count": len(data_rows),
            "column_count": len(headers),
            "quarter": "Multiple",  # Since this workbook contains multiple quarters
            "quarters_summary": quarters
        }
        
    except Exception as e:
        logger.exception(f"    ✗ Failed service completion extraction: {e}")
        raise


async def parse_service_completion_sheet(file_obj, sheet_name: str, matched_name: str, s3_url: str = None):
    """
    Async version of Service Completion parser with database saving.
    Extracts Quarter, Location, and Quantity Received columns.
    """
    logger.info(f"    ★★★ ASYNC SERVICE COMPLETION WORKBOOK EXTRACTION ★★★")
    
    try:
        file_obj.seek(0)
        df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        logger.info(f"    Columns: {list(df.columns)}")
        
        # Find the relevant columns
        quarter_col = None
        location_col = None
        quantity_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'quarter' in col_lower:
                quarter_col = col
            elif 'location' in col_lower:
                location_col = col
            elif 'quantity' in col_lower and 'received' in col_lower:
                quantity_col = col
        
        if not all([quarter_col, location_col, quantity_col]):
            logger.error(f"    ✗ Missing required columns!")
            raise ValueError("Service Completion sheet missing required columns")
        
        logger.info(f"    ✓ Found required columns:")
        logger.info(f"      Quarter: '{quarter_col}'")
        logger.info(f"      Location: '{location_col}'")
        logger.info(f"      Quantity Received: '{quantity_col}'")
        
        # Clean the data
        import numpy as np
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Filter valid rows
        df_clean = df[
            df[quarter_col].notna() & 
            df[location_col].notna() & 
            df[quantity_col].notna()
        ].copy()
        
        logger.info(f"    ✓ Cleaned data: {len(df_clean)} valid rows")
        
        # Normalize locations and prepare data
        mapped_data = []
        for idx, row in df_clean.iterrows():
            quarter = str(row[quarter_col]).strip()
            original_location = str(row[location_col]).strip()
            
            try:
                quantity_received = int(float(row[quantity_col]))
            except (ValueError, TypeError):
                logger.warning(f"      Skipping row {idx}: invalid quantity")
                continue
            
            normalized_location = normalize_location(original_location)
            
            mapped_data.append({
                "quarter": quarter,
                "original_location": original_location,
                "normalized_location": normalized_location,
                "quantity_received": quantity_received,
                "gift_type": "Service Completion"
            })
        
        logger.info(f"    ✓ Processed {len(mapped_data)} records")
        
        # Save to database if s3_url provided
        if s3_url and mapped_data:
            logger.info(f"    Saving {len(mapped_data)} Service Completion records to database...")
            headers = ["quarter", "original_location", "normalized_location", "quantity_received", "gift_type"]
            data_rows = [[
                item["quarter"],
                item["original_location"],
                item["normalized_location"],
                item["quantity_received"],
                item["gift_type"]
            ] for item in mapped_data]
            
            # Note: We'll save this as "Service Completion" workbook with "Multiple" quarters
            await async_save_category_data("inventory", s3_url, headers, data_rows, "Service Completion", "Multiple")
            logger.info(f"    ✓ Saved Service Completion data to database")
        
        # Group by quarter for summary
        quarters = {}
        for record in mapped_data:
            q = record['quarter']
            if q not in quarters:
                quarters[q] = 0
            quarters[q] += record['quantity_received']
        
        logger.info(f"    Summary by quarter:")
        for q, total in sorted(quarters.items()):
            logger.info(f"      {q}: {total} total gifts")
        
        # Prepare return data
        headers = ["quarter", "original_location", "normalized_location", "quantity_received", "gift_type"]
        data_rows = [[
            item["quarter"],
            item["original_location"],
            item["normalized_location"],
            item["quantity_received"],
            item["gift_type"]
        ] for item in mapped_data]
        
        logger.info(f"    ★★★ ASYNC SERVICE COMPLETION EXTRACTION COMPLETE ★★★")
        
        return {
            "workbook": "Service Completion",
            "headers": headers,
            "data": data_rows,
            "mapped_data": mapped_data,
            "row_count": len(data_rows),
            "column_count": len(headers),
            "quarter": "Multiple",
            "quarters_summary": quarters
        }
        
    except Exception as e:
        logger.exception(f"    ✗ Failed async service completion extraction: {e}")
        raise