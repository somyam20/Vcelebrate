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
    No normalization - keep original headers as-is.
    """
    logger.info("=" * 80)
    logger.info(f"PARSING FILE: {filename} (Category: {category})")
    logger.info("=" * 80)
    
    try:
        lower = filename.lower()
        
        # Read Excel or CSV
        if lower.endswith(".xlsx") or lower.endswith(".xls"):
            # Try different header rows for Excel
            df = None
            
            # First, try to detect header row automatically
            for header_row in [0, 1, 2]:
                try:
                    temp_df = pd.read_excel(file_obj, header=header_row)
                    
                    # Check if we have valid headers (not mostly unnamed)
                    unnamed_count = sum(1 for col in temp_df.columns if str(col).startswith("Unnamed"))
                    if unnamed_count < len(temp_df.columns) * 0.5:  # Less than 50% unnamed
                        df = temp_df
                        logger.info(f"✓ Found valid headers at row {header_row}")
                        break
                    
                    # Reset file pointer for next attempt
                    file_obj.seek(0)
                except Exception as e:
                    logger.debug(f"Failed to read with header_row={header_row}: {e}")
                    file_obj.seek(0)
            
            if df is None:
                # Fallback: read with default header
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
        
        # Replace NaN, NaT, and inf values with None (null in JSON)
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        
        # Also replace numpy NaN values
        import numpy as np
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Convert dataframe to list of lists (rows)
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
            "column_count": len(headers)
        }
        
    except Exception as e:
        logger.exception(f"✗ Failed to parse file: {e}")
        raise