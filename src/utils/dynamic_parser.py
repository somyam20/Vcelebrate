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
                        logger.info(f"✓ Found valid headers at row {header_row}")
                        break
                    
                    file_obj.seek(0)
                except Exception as e:
                    logger.debug(f"Failed to read with header_row={header_row}: {e}")
                    file_obj.seek(0)
            
            if df is None:
                df = pd.read_excel(file_obj, header=0)
                logger.warning("⚠ Using default header row (0)")
            
            # Drop unnamed leading columns
            while len(df.columns) > 0 and str(df.columns[0]).startswith("Unnamed"):
                logger.info(f"→ Dropping unnamed first column: {df.columns[0]}")
                df = df.iloc[:, 1:]
                
        elif lower.endswith(".csv"):
            df = pd.read_csv(file_obj)
            logger.info("✓ Read CSV file")
        else:
            raise ValueError(f"Unsupported file type: {filename}")
        
        # Extract headers
        headers = [str(col).strip() for col in df.columns]
        logger.info(f"✓ Extracted {len(headers)} headers: {headers}")
        
        # Replace NaN, NaT, and inf values with None
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        
        import numpy as np
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Convert dataframe to list of lists
        data = df.values.tolist()
        
        # Clean data - remove completely empty rows
        data = [row for row in data if any(val is not None and str(val).strip() for val in row if val is not None)]
        
        logger.info(f"✓ Parsed {len(data)} rows of data")
        logger.info(f"✓ Sample row 0: {data[0] if data else 'No data'}")
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
    Returns a list of parsed sheets with their metadata.
    """
    logger.info("=" * 80)
    logger.info("PARSING INVENTORY WORKBOOK WITH MULTIPLE SHEETS")
    logger.info("=" * 80)
    
    try:
        # Read all sheet names
        excel_file = pd.ExcelFile(file_obj)
        sheet_names = excel_file.sheet_names
        logger.info(f"✓ Found {len(sheet_names)} sheets: {sheet_names}")
        
        # Target workbook names
        target_workbooks = ["Birthday", "As on 03-10-25", "Service Completion"]
        
        all_sheets_data = []
        
        for sheet_name in sheet_names:
            # Check if this sheet is one of our target workbooks
            if sheet_name not in target_workbooks:
                logger.info(f"→ Skipping sheet: {sheet_name}")
                continue
            
            logger.info(f"→ Processing sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(file_obj, sheet_name=sheet_name, header=0)
            
            # Extract quarter from first column if it exists
            quarter = None
            if len(df.columns) > 0:
                first_col_name = str(df.columns[0])
                if "quarter" in first_col_name.lower() or first_col_name == "Quarter":
                    # Quarter is a column - we'll extract it from data
                    pass
            
            # Drop unnamed leading columns
            while len(df.columns) > 0 and str(df.columns[0]).startswith("Unnamed"):
                logger.info(f"  → Dropping unnamed first column: {df.columns[0]}")
                df = df.iloc[:, 1:]
            
            # Extract headers
            headers = [str(col).strip() for col in df.columns]
            logger.info(f"  ✓ Extracted {len(headers)} headers: {headers}")
            
            # Replace NaN, NaT, and inf values with None
            df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
            
            import numpy as np
            df = df.replace([np.nan, np.inf, -np.inf], None)
            
            # Convert dataframe to list of lists
            data = df.values.tolist()
            
            # Clean data - remove completely empty rows
            data = [row for row in data if any(val is not None and str(val).strip() if val is not None else False for val in row)]
            
            logger.info(f"  ✓ Parsed {len(data)} rows from sheet '{sheet_name}'")
            
            # Store this sheet's data
            all_sheets_data.append({
                "workbook": sheet_name,
                "headers": headers,
                "data": data,
                "row_count": len(data),
                "column_count": len(headers)
            })
        
        logger.info("=" * 80)
        logger.info(f"✓ Successfully parsed {len(all_sheets_data)} workbook sheets")
        logger.info("=" * 80)
        
        return {"sheets": all_sheets_data}
        
    except Exception as e:
        logger.exception(f"✗ Failed to parse inventory workbook: {e}")
        raise