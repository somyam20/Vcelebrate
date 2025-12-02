import asyncio
import pandas as pd
import logging

logger = logging.getLogger(__name__)


async def async_parse_excel_dynamic(file_obj, filename: str, category: str):
    """Parse Excel/CSV file with dynamic header extraction"""
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
            return parse_inventory_workbook(file_obj, filename)
        
        # For other categories or CSV files
        if lower.endswith(".xlsx") or lower.endswith(".xls"):
            df = None
            
            # Try to detect header row automatically
            for header_row in [0, 1, 2]:
                try:
                    temp_df = pd.read_excel(file_obj, header=header_row)
                    
                    # Check if we have valid headers
                    unnamed_count = sum(1 for col in temp_df.columns if str(col).startswith("Unnamed"))
                    if unnamed_count < len(temp_df.columns) * 0.5:
                        df = temp_df
                        logger.info(f"Found valid headers at row {header_row}")
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
            df = pd.read_csv(file_obj)
            logger.info("Read CSV file")
        else:
            raise ValueError(f"Unsupported file type: {filename}")
        
        # Extract headers
        headers = [str(col).strip() for col in df.columns]
        logger.info(f"Extracted {len(headers)} headers: {headers}")
        
        # Replace NaN, NaT, and inf values with None
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        
        import numpy as np
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Convert dataframe to list of lists
        data = df.values.tolist()
        
        # Clean data - remove completely empty rows
        data = [row for row in data if any(val is not None and str(val).strip() for val in row if val is not None)]
        
        logger.info(f"Parsed {len(data)} rows of data")
        logger.info(f"Sample row 0: {data[0] if data else 'No data'}")
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
        logger.exception(f"Failed to parse file: {e}")
        raise


def parse_inventory_workbook(file_obj, filename: str):
    """
    Parse inventory Excel file with multiple worksheets.
    Returns a list of parsed sheets with their metadata.
    
    Detects these workbook names:
    - Birthday
    - As On 03-10-25 (Anniversary gifts)
    - Service Completion
    """
    logger.info("=" * 80)
    logger.info("PARSING INVENTORY WORKBOOK WITH MULTIPLE SHEETS")
    logger.info("=" * 80)
    
    try:
        # Read all sheet names
        excel_file = pd.ExcelFile(file_obj)
        sheet_names = excel_file.sheet_names
        logger.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
        
        # Target workbook names - now includes all three types
        target_workbooks = [
            "Birthday",
            "As On 03-10-25",  # Anniversary workbook
            "As on 03-10-25",  # Case variation
            "Service Completion"
        ]
        
        all_sheets_data = []
        
        for sheet_name in sheet_names:
            # Check if this sheet is one of our target workbooks
            # Use case-insensitive comparison
            sheet_name_clean = sheet_name.strip()
            is_target = False
            matched_name = None
            
            for target in target_workbooks:
                if sheet_name_clean.lower() == target.lower():
                    is_target = True
                    # Use the standardized name
                    if "birthday" in target.lower():
                        matched_name = "Birthday"
                    elif "as on" in target.lower() or "as on" in target.lower():
                        matched_name = "As on 03-10-25"
                    elif "service" in target.lower():
                        matched_name = "Service Completion"
                    break
            
            if not is_target:
                logger.info(f"Skipping sheet: {sheet_name}")
                continue
            
            logger.info(f"Processing sheet: {sheet_name} (mapped to: {matched_name})")
            
            # Read the sheet with header detection
            df = None
            for header_row in [0, 1, 2]:
                try:
                    file_obj.seek(0)
                    temp_df = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_row)
                    
                    # Check if we have valid headers
                    unnamed_count = sum(1 for col in temp_df.columns if str(col).startswith("Unnamed"))
                    if unnamed_count < len(temp_df.columns) * 0.5:
                        df = temp_df
                        logger.info(f"  Found valid headers at row {header_row}")
                        break
                except Exception as e:
                    logger.debug(f"  Failed with header_row={header_row}: {e}")
            
            if df is None:
                logger.info(f"  Using default header row (0)")
                file_obj.seek(0)
                df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
            
            # Extract quarter from first column if it exists
            quarter = None
            if len(df.columns) > 0:
                first_col_name = str(df.columns[0])
                if "quarter" in first_col_name.lower():
                    # Quarter is in the first column - extract from first data row
                    if len(df) > 0:
                        quarter = str(df.iloc[0, 0])
                        logger.info(f"  Detected quarter from data: {quarter}")
            
            # Drop unnamed leading columns
            while len(df.columns) > 0 and str(df.columns[0]).startswith("Unnamed"):
                logger.info(f"  Dropping unnamed first column: {df.columns[0]}")
                df = df.iloc[:, 1:]
            
            # If first column is Quarter, keep it
            if len(df.columns) > 0 and "quarter" in str(df.columns[0]).lower():
                logger.info(f"  Keeping Quarter column: {df.columns[0]}")
            
            # Extract headers
            headers = [str(col).strip() for col in df.columns]
            logger.info(f"  Extracted {len(headers)} headers: {headers}")
            
            # Replace NaN, NaT, and inf values with None
            df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
            
            import numpy as np
            df = df.replace([np.nan, np.inf, -np.inf], None)
            
            # Convert dataframe to list of lists
            data = df.values.tolist()
            
            # Clean data - remove completely empty rows
            data = [row for row in data if any(val is not None and str(val).strip() if val is not None else False for val in row)]
            
            logger.info(f"  Parsed {len(data)} rows from sheet '{sheet_name}'")
            
            # If we didn't find quarter in first column, check if it's in the Quarter column
            if not quarter and "Quarter" in headers:
                quarter_idx = headers.index("Quarter")
                for row in data:
                    if len(row) > quarter_idx and row[quarter_idx]:
                        quarter = str(row[quarter_idx])
                        logger.info(f"  Detected quarter from Quarter column: {quarter}")
                        break
            
            # Store this sheet's data with the matched/standardized name
            all_sheets_data.append({
                "workbook": matched_name,  # Use standardized name
                "headers": headers,
                "data": data,
                "row_count": len(data),
                "column_count": len(headers),
                "quarter": quarter
            })
        
        logger.info("=" * 80)
        logger.info(f"Successfully parsed {len(all_sheets_data)} workbook sheets:")
        for sheet in all_sheets_data:
            logger.info(f"  - {sheet['workbook']}: {sheet['row_count']} rows, quarter: {sheet.get('quarter', 'N/A')}")
        logger.info("=" * 80)
        
        if len(all_sheets_data) == 0:
            logger.warning("WARNING: No target worksheets found!")
            logger.warning(f"Available sheets were: {sheet_names}")
            logger.warning(f"Looking for: {target_workbooks}")
        
        return {"sheets": all_sheets_data}
        
    except Exception as e:
        logger.exception(f"Failed to parse inventory workbook: {e}")
        raise