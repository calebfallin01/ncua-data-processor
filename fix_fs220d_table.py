import os
import requests
import logging
import pandas as pd
import re
from dotenv import load_dotenv
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

def fix_fs220d_table():
    """Fix the fs220d table with proper column types"""
    # Supabase credentials
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
    
    # Set up headers for API requests
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    
    # Extract date from zip file
    year, month = extract_date_from_zip()
    if not year or not month:
        logging.warning("Using default date 2024-12")
        year, month = "2024", "12"
    
    table_name = f"fs220d_{year}_{month}"
    logging.info(f"Working with table: {table_name}")
    
    # Check if sample file exists
    sample_file_path = 'output/extracted_data/FS220D_sample.csv'
    if not os.path.exists(sample_file_path):
        logging.error(f"Sample file {sample_file_path} not found")
        return False
    
    # First drop the existing table
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    
    # Execute SQL
    try:
        response = requests.post(
            f"{url}/rest/v1/rpc/execute_sql",
            headers=headers,
            json={"sql_query": drop_table_sql}
        )
        
        if response.status_code in [200, 204]:
            logging.info(f"Dropped table {table_name}")
        else:
            logging.error(f"Failed to drop table: {response.text}")
            return False
        
        # Read the CSV file to get structure
        df = pd.read_csv(sample_file_path)
        
        # Before creating SQL, convert all numeric columns to text in the DataFrame
        for col in df.columns:
            if df[col].dtype == 'int64' or df[col].dtype == 'float64':
                df[col] = df[col].astype(str)
                
        # Create SQL for table with all columns as TEXT
        columns = []
        
        for col in df.columns:
            clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
            
            # Check column type - use TEXT for most columns for safety
            dtype = df[col].dtype
            
            if col.lower() in ['phone', 'fax', 'phonenumber'] or 'phone' in col.lower() or 'fax' in col.lower():
                sql_type = "TEXT"  # Use TEXT for these
            elif pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
                sql_type = "TEXT"  # Use TEXT for all numeric types
            elif pd.api.types.is_datetime64_dtype(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT"
            
            columns.append(f"{clean_col} {sql_type}")
        
        # Create table SQL
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            {', '.join(columns)}
        );
        """
        
        # Execute SQL to create table
        response = requests.post(
            f"{url}/rest/v1/rpc/execute_sql",
            headers=headers,
            json={"sql_query": create_table_sql}
        )
        
        if response.status_code in [200, 204]:
            logging.info(f"Created table {table_name} with all TEXT types")
            
            # Insert sample data
            # Clean column names
            clean_columns = {}
            for col in df.columns:
                clean_columns[col] = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
            
            df = df.rename(columns=clean_columns)
            
            # Convert to records and handle NaN values
            records = df.head(10).to_dict('records')
            for record in records:
                for key, value in list(record.items()):
                    if pd.isna(value) or value == 'nan' or value == 'NaN' or value == 'None':
                        record[key] = None
            
            # Insert the records
            # Add a retry mechanism
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    # Add resolution=ignore-duplicates to avoid errors if records already exist
                    insert_headers = headers.copy()
                    insert_headers["Prefer"] = "return=minimal,resolution=ignore-duplicates"
                    
                    response = requests.post(
                        f"{url}/rest/v1/{table_name}",
                        headers=insert_headers,
                        json=records,
                        timeout=30  # Add timeout
                    )
                    
                    if response.status_code in [200, 201, 204]:
                        logging.info(f"Inserted sample data into {table_name}")
                        success = True
                        return True
                    else:
                        error_text = response.text if response.text else "No error message returned"
                        logging.error(f"Failed to insert data (status {response.status_code}): {error_text}")
                        retry_count += 1
                        
                        # Log the first record for debugging
                        if records and retry_count == max_retries:
                            logging.error(f"First record: {records[0]}")
                        
                        # Wait before retrying
                        if retry_count < max_retries:
                            logging.info(f"Retrying insert (attempt {retry_count + 1}/{max_retries})...")
                            time.sleep(2)
                except Exception as e:
                    logging.error(f"Error during insert: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        logging.info(f"Retrying insert (attempt {retry_count + 1}/{max_retries})...")
                        time.sleep(2)
            
            return success
        else:
            logging.error(f"Failed to create table: {response.text}")
            return False
    
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return False

def extract_date_from_zip():
    """Extract date (year, month) from zip filename in input directory"""
    try:
        # Find zip files in input directory
        input_dir = "input"
        zip_files = [f for f in os.listdir(input_dir) if f.endswith('.zip') and not f.startswith('.')]
        
        if not zip_files:
            logging.warning("No zip files found in input directory")
            return None, None
        
        # Get the first zip file (assuming we process one at a time)
        zip_file = zip_files[0]
        logging.info(f"Found zip file: {zip_file}")
        
        # Extract date using regex
        match = re.search(r'(\d{4})-(\d{2})', zip_file)
        if match:
            year = match.group(1)
            month = match.group(2)
            logging.info(f"Extracted date: {year}-{month}")
            return year, month
        else:
            logging.warning(f"Could not extract date from zip filename: {zip_file}")
            return None, None
            
    except Exception as e:
        logging.error(f"Error extracting date from zip: {str(e)}")
        return None, None

if __name__ == "__main__":
    result = fix_fs220d_table()
    if result:
        logging.info("FS220D table fix completed successfully")
    else:
        logging.error("FS220D table fix failed") 