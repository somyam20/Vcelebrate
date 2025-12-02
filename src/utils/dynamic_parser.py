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
            
            # Process all relevant sheets
            result_sheets = []
            
            for sheet_name in sheet_names:
                logger.info(f"Processing sheet: {sheet_name}")
                
                # Check for anniversary/birthday sheet
                if "as on" in sheet_name.lower():
                    logger.info(f"Found special anniversary/birthday sheet: {sheet_name}")
                    anniversary_result = await parse_anniversary_birthday_sheet(file_obj, sheet_name, "As on 03-10-25", s3_url)
                    result_sheets.extend(anniversary_result)
                
                # Check for service completion sheet
                elif "service completion" in sheet_name.lower():
                    logger.info(f"Found service completion sheet: {sheet_name}")
                    service_result = await parse_service_completion_sheet(file_obj, sheet_name, "Service Completion", s3_url)
                    result_sheets.append(service_result)
                
                # Check for other inventory sheets
                elif any(target.lower() in sheet_name.lower() for target in ["birthday", "inventory"]):
                    logger.info(f"Found inventory sheet: {sheet_name}")
                    # Process as standard inventory sheet
                    try:
                        file_obj.seek(0)
                        df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
                        
                        # Filter out unnamed columns
                        valid_cols = [col for col in df.columns if not str(col).startswith("Unnamed")]
                        df = df[valid_cols]
                        
                        headers = [str(col).strip() for col in df.columns]
                        
                        import numpy as np
                        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
                        df = df.replace([np.nan, np.inf, -np.inf], None)
                        
                        data = df.values.tolist()
                        data = [row for row in data if any(val is not None and str(val).strip() if val is not None else False for val in row)]
                        
                        if s3_url and data:
                            await async_save_category_data("inventory", s3_url, headers, data, sheet_name, None)
                        
                        result_sheets.append({
                            "workbook": sheet_name,
                            "headers": headers,
                            "data": data,
                            "row_count": len(data),
                            "column_count": len(headers),
                            "quarter": None
                        })
                        logger.info(f"Processed standard inventory sheet: {sheet_name}")
                    except Exception as e:
                        logger.warning(f"Failed to process sheet {sheet_name}: {e}")
            
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
    Extracts data based on Event column (Birthday, Anniversary, Service Completion).
    Maps locations using LOCATION_ALIASES and saves to database.
    """
    logger.info(f"    ★★★ EVENT-BASED EXTRACTION WITH LOCATION MAPPING ★★★")
    
    try:
        # Try different header rows to find Event column
        df = None
        event_col = None
        
        for header_row in [1, 0, 2]:
            try:
                file_obj.seek(0)
                temp_df = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_row)
                
                # Check if Event column exists
                for col in temp_df.columns:
                    if "event" in str(col).lower():
                        df = temp_df
                        event_col = col
                        logger.info(f"    ✓ Found Event column at header row {header_row}")
                        break
                
                if event_col:
                    break
            except Exception as e:
                logger.debug(f"    Failed with header_row={header_row}: {e}")
        
        if df is None or event_col is None:
            logger.error("    ✗ Event column not found in any header row!")
            raise ValueError("Event column not found in sheet")
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        logger.info(f"    Columns: {list(df.columns)}")
        
        # Get location columns
        location_cols = []
        for col in df.columns:
            col_str = str(col)
            if not any(x.lower() in col_str.lower() for x in ["Unnamed", "Description", "Event", "Total"]) and not col_str.startswith("Unnamed"):
                location_cols.append(col)
        
        logger.info(f"    Location columns: {location_cols}")
        
        # Extract data by event type
        result_sheets = []
        event_types = {
            "Birthday": ["Birthday", "Birthday-Speakers", "Birthday - Previous"],
            "Anniversary": ["Anniversary"],
            "Service Completion": ["Service Completion"]
        }
        
        for workbook_name, event_keywords in event_types.items():
            mapped_data = []
            
            for idx, row in df.iterrows():
                event_value = str(row[event_col]).strip() if pd.notna(row[event_col]) else ""
                
                # Check if this row matches any of the event keywords
                if any(keyword.lower() in event_value.lower() for keyword in event_keywords):
                    # Extract quantities for each location
                    for location_col in location_cols:
                        quantity = row[location_col]
                        
                        if quantity is not None and not pd.isna(quantity):
                            try:
                                quantity_int = int(float(quantity))
                                if quantity_int > 0:
                                    normalized_location = normalize_location(location_col)
                                    mapped_data.append({
                                        "original_location": location_col,
                                        "normalized_location": normalized_location,
                                        "quantity": quantity_int,
                                        "gift_type": workbook_name
                                    })
                                    logger.info(f"      {workbook_name}: '{location_col}' → '{normalized_location}' (qty: {quantity_int})")
                            except (ValueError, TypeError):
                                pass
            
            # Save to database if data found
            if s3_url and mapped_data:
                logger.info(f"    Saving {len(mapped_data)} {workbook_name} records to database...")
                for item in mapped_data:
                    headers = ["original_location", "normalized_location", "quantity", "gift_type"]
                    data_row = [[item["original_location"], item["normalized_location"], item["quantity"], item["gift_type"]]]
                    await async_save_category_data("inventory", s3_url, headers, data_row, workbook_name, None)
                logger.info(f"    ✓ Saved {len(mapped_data)} {workbook_name} records")
            
            if mapped_data:
                result_sheets.append({
                    "workbook": workbook_name,
                    "headers": location_cols,
                    "data": [],
                    "mapped_data": mapped_data,
                    "row_count": len(mapped_data),
                    "column_count": len(location_cols),
                    "quarter": None
                })
                logger.info(f"    ✓ Created {workbook_name} dataset with {len(mapped_data)} records")
        
        logger.info(f"    ★★★ SPECIAL EXTRACTION COMPLETE: {len(result_sheets)} datasets created ★★★")
        return result_sheets
        
    except Exception as e:
        logger.exception(f"    ✗ Failed special row extraction: {e}")
        raise


def parse_anniversary_birthday_sheet_sync(file_obj, sheet_name: str, matched_name: str):
    """
    Synchronous version - extracts data based on Event column.
    """
    logger.info(f"    ★★★ SYNC EVENT-BASED EXTRACTION WITH LOCATION MAPPING ★★★")
    
    try:
        # Try different header rows to find Event column
        df = None
        event_col = None
        
        for header_row in [1, 0, 2]:
            try:
                file_obj.seek(0)
                temp_df = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_row)
                
                # Check if Event column exists
                for col in temp_df.columns:
                    if "event" in str(col).lower():
                        df = temp_df
                        event_col = col
                        logger.info(f"    ✓ Found Event column at header row {header_row}")
                        break
                
                if event_col:
                    break
            except Exception as e:
                logger.debug(f"    Failed with header_row={header_row}: {e}")
        
        if df is None or event_col is None:
            logger.error("    ✗ Event column not found in any header row!")
            raise ValueError("Event column not found")
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        
        # Get location columns
        location_cols = []
        for col in df.columns:
            col_str = str(col)
            if not any(x.lower() in col_str.lower() for x in ["Unnamed", "Description", "Event", "Total"]) and not col_str.startswith("Unnamed"):
                location_cols.append(col)
        
        logger.info(f"    Location columns: {location_cols}")
        
        result_sheets = []
        event_types = {
            "Birthday": ["Birthday", "Birthday-Speakers", "Birthday - Previous"],
            "Anniversary": ["Anniversary"],
            "Service Completion": ["Service Completion"]
        }
        
        for workbook_name, event_keywords in event_types.items():
            mapped_data = []
            
            for idx, row in df.iterrows():
                event_value = str(row[event_col]).strip() if pd.notna(row[event_col]) else ""
                
                if any(keyword.lower() in event_value.lower() for keyword in event_keywords):
                    for location_col in location_cols:
                        quantity = row[location_col]
                        
                        if quantity is not None and not pd.isna(quantity):
                            try:
                                quantity_int = int(float(quantity))
                                if quantity_int > 0:
                                    normalized_location = normalize_location(location_col)
                                    mapped_data.append({
                                        "original_location": location_col,
                                        "normalized_location": normalized_location,
                                        "quantity": quantity_int,
                                        "gift_type": workbook_name
                                    })
                                    logger.info(f"      {workbook_name}: '{location_col}' → '{normalized_location}' (qty: {quantity_int})")
                            except (ValueError, TypeError):
                                pass
            
            if mapped_data:
                result_sheets.append({
                    "workbook": workbook_name,
                    "headers": location_cols,
                    "data": [],
                    "mapped_data": mapped_data,
                    "row_count": len(mapped_data),
                    "column_count": len(location_cols),
                    "quarter": None
                })
                logger.info(f"    ✓ Created {workbook_name} dataset with {len(mapped_data)} records")
        
        logger.info(f"    ★★★ SYNC EXTRACTION COMPLETE: {len(result_sheets)} datasets created ★★★")
        return result_sheets
        
    except Exception as e:
        logger.exception(f"    ✗ Failed sync special row extraction: {e}")
        raise


def parse_standard_inventory_sheet(file_obj, sheet_name: str, matched_name: str):
    """
    Fallback parser for inventory sheets that don't match special patterns.
    Processes them as standard data sheets.
    """
    logger.info(f"    ★ STANDARD INVENTORY SHEET PROCESSING: {sheet_name} ★")
    
    try:
        file_obj.seek(0)
        df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
        
        logger.info(f"    Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns")
        logger.info(f"    Columns: {list(df.columns)}")
        
        # Extract headers
        headers = [str(col).strip() for col in df.columns]
        
        # Clean data
        import numpy as np
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Convert to list of lists
        data = df.values.tolist()
        
        # Clean data - remove completely empty rows
        data = [row for row in data if any(val is not None and str(val).strip() if val is not None else False for val in row)]
        
        logger.info(f"    ✓ Processed {len(data)} rows as standard inventory sheet")
        
        return {
            "workbook": matched_name,
            "headers": headers,
            "data": data,
            "row_count": len(data),
            "column_count": len(headers),
            "quarter": None
        }
        
    except Exception as e:
        logger.exception(f"    ✗ Failed standard inventory sheet processing: {e}")
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
        
        # Find the relevant columns (case-insensitive and flexible matching)
        quarter_col = None
        location_col = None
        quantity_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            # More flexible quarter matching
            if any(q in col_lower for q in ['quarter', 'qtr', 'q1', 'q2', 'q3', 'q4']):
                quarter_col = col
            # More flexible location matching
            elif any(loc in col_lower for loc in ['location', 'place', 'office', 'site', 'branch']):
                location_col = col
            # More flexible quantity matching
            elif any(qty in col_lower for qty in ['quantity', 'qty', 'count', 'number', 'received', 'available']):
                quantity_col = col
        
        # If we still don't have all columns, try to use any available columns
        if not quarter_col and len(df.columns) > 0:
            # Use first column as quarter if no quarter column found
            quarter_col = df.columns[0]
            logger.warning(f"    ⚠ No quarter column found, using first column: '{quarter_col}'")
        
        if not location_col and len(df.columns) > 1:
            # Use second column as location if no location column found
            location_col = df.columns[1]
            logger.warning(f"    ⚠ No location column found, using second column: '{location_col}'")
        
        if not quantity_col and len(df.columns) > 2:
            # Use third column as quantity if no quantity column found
            quantity_col = df.columns[2]
            logger.warning(f"    ⚠ No quantity column found, using third column: '{quantity_col}'")
        
        if not all([quarter_col, location_col, quantity_col]):
            logger.error(f"    ✗ Cannot identify required columns!")
            logger.error(f"    Available columns: {list(df.columns)}")
            logger.error(f"    Quarter column: {quarter_col}")
            logger.error(f"    Location column: {location_col}")
            logger.error(f"    Quantity column: {quantity_col}")
            # Instead of raising an error, let's process it as a standard sheet
            logger.warning(f"    ⚠ Processing as standard inventory sheet instead")
            return parse_standard_inventory_sheet(file_obj, sheet_name, matched_name)
        
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
        
        # Find the relevant columns (flexible matching)
        quarter_col = None
        location_col = None
        quantity_col = None
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if any(q in col_lower for q in ['quarter', 'qtr', 'q1', 'q2', 'q3', 'q4']):
                quarter_col = col
            elif any(loc in col_lower for loc in ['location', 'place', 'office', 'site', 'branch']):
                location_col = col
            elif any(qty in col_lower for qty in ['quantity', 'qty', 'count', 'number', 'received', 'available']):
                quantity_col = col
        
        # Fallback to first available columns if specific ones not found
        if not quarter_col and len(df.columns) > 0:
            quarter_col = df.columns[0]
            logger.warning(f"    ⚠ Using first column as quarter: '{quarter_col}'")
        
        if not location_col and len(df.columns) > 1:
            location_col = df.columns[1]
            logger.warning(f"    ⚠ Using second column as location: '{location_col}'")
        
        if not quantity_col and len(df.columns) > 2:
            quantity_col = df.columns[2]
            logger.warning(f"    ⚠ Using third column as quantity: '{quantity_col}'")
        
        if not all([quarter_col, location_col, quantity_col]):
            logger.error(f"    ✗ Cannot identify required columns!")
            logger.error(f"    Available columns: {list(df.columns)}")
            raise ValueError("Service Completion sheet processing failed - insufficient columns")
        
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
        
        # Save each location record separately to database if s3_url provided
        if s3_url and mapped_data:
            logger.info(f"    Saving {len(mapped_data)} Service Completion records to database...")
            for item in mapped_data:
                headers = ["quarter", "original_location", "normalized_location", "quantity_received", "gift_type"]
                data_row = [[
                    item["quarter"],
                    item["original_location"],
                    item["normalized_location"],
                    item["quantity_received"],
                    item["gift_type"]
                ]]
                await async_save_category_data("inventory", s3_url, headers, data_row, "Service Completion", item["quarter"])
            logger.info(f"    ✓ Saved {len(mapped_data)} Service Completion location records to database")
        
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
        # Fallback to standard processing
        logger.warning(f"    ⚠ Falling back to standard inventory processing")
        try:
            file_obj.seek(0)
            df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
            headers = [str(col).strip() for col in df.columns]
            
            import numpy as np
            df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
            df = df.replace([np.nan, np.inf, -np.inf], None)
            
            data = df.values.tolist()
            data = [row for row in data if any(val is not None and str(val).strip() if val is not None else False for val in row)]
            
            # Save to database if s3_url provided
            if s3_url and data:
                logger.info(f"    Saving {len(data)} fallback records to database...")
                await async_save_category_data("inventory", s3_url, headers, data, "Service Completion", None)
                logger.info(f"    ✓ Saved fallback data to database")
            
            return {
                "workbook": "Service Completion",
                "headers": headers,
                "data": data,
                "row_count": len(data),
                "column_count": len(headers),
                "quarter": None
            }
        except Exception as fallback_error:
            logger.exception(f"    ✗ Fallback processing also failed: {fallback_error}")
            raise