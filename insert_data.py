import os
import requests
import logging
import time
import pandas as pd
import glob
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/insert_data.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

class SupabaseDataInserter:
    def __init__(self):
        """Initialize Supabase connection details"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
            
        # Set up headers for API requests
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"  # For better performance
        }
        
        # Base endpoint for Supabase REST API
        self.rest_endpoint = f"{self.url}/rest/v1"
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
    
    def insert_all_data(self):
        """Insert all NCUA data into Supabase tables"""
        try:
            # Extract date from zip file in input directory
            date_info = self.extract_date_from_zip()
            if not date_info:
                logging.warning("Could not extract date from zip file, using default date")
                year, month = "2024", "03"
            else:
                year, month = date_info
            
            logging.info(f"Inserting data for period: {year}-{month}")
            
            # Find all extracted data files
            txt_files = glob.glob(os.path.join('output', 'extracted_data', '*.txt'))
            
            for txt_file in txt_files:
                try:
                    # Skip readme and other non-data files
                    if "Readme.txt" in txt_file or "Report1.txt" in txt_file:
                        continue
                    
                    file_name = os.path.basename(txt_file)
                    base_name = os.path.splitext(file_name)[0]
                    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name.lower())
                    table_name = f"{table_name}_{year}_{month}"
                    
                    logging.info(f"Processing {file_name} into table {table_name}")
                    
                    # Check if the table exists
                    if self.check_table_exists(table_name):
                        # Read the data
                        df = self.read_data_file(txt_file)
                        
                        if df is not None and not df.empty:
                            # Insert the data
                            self.insert_dataframe(table_name, df)
                            logging.info(f"Completed inserting data from {file_name} into {table_name}")
                        else:
                            logging.warning(f"Could not read data from {file_name}")
                    else:
                        logging.warning(f"Table {table_name} does not exist. Skipping.")
                
                except Exception as e:
                    logging.error(f"Error processing {txt_file}: {str(e)}")
                    # Continue with next file
            
            logging.info("Data insertion completed")
        
        except Exception as e:
            logging.error(f"Error in insert_all_data function: {str(e)}")
            raise
    
    def check_table_exists(self, table_name):
        """Check if a table exists in Supabase"""
        try:
            headers = self.headers.copy()
            headers["Accept"] = "application/json"
            
            response = requests.get(f"{self.rest_endpoint}/{table_name}?limit=0",
                                   headers=headers)
            
            return response.status_code == 200
        
        except Exception as e:
            logging.error(f"Error checking if table exists: {str(e)}")
            return False
    
    def read_data_file(self, file_path):
        """Read a data file with various encodings and formats"""
        try:
            # Try reading as CSV with different encodings and separators
            encodings = ['utf-8', 'latin1', 'cp1252', 'ISO-8859-1']
            separators = [',', '\t', '|', ';']
            
            for encoding in encodings:
                for sep in separators:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding, sep=sep, low_memory=False)
                        if len(df.columns) > 1:  # Ensure we have more than one column
                            logging.info(f"Successfully read file as {sep}-separated with {encoding} encoding")
                            return df
                    except Exception:
                        continue
            
            logging.warning(f"Could not read file {file_path} with any standard format")
            return None
        
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return None
    
    def insert_dataframe(self, table_name, df):
        """Insert DataFrame data into a table"""
        try:
            # Clean column names
            clean_columns = {}
            for col in df.columns:
                clean_columns[col] = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
            
            df = df.rename(columns=clean_columns)
            
            # Convert all numeric columns to strings to avoid any data type issues
            for col in df.columns:
                if df[col].dtype == 'int64' or df[col].dtype == 'float64':
                    # Always convert numeric columns to strings for maximum reliability
                    df[col] = df[col].astype(str)
            
            # Batch insert to avoid timeouts and rate limits
            total_rows = len(df)
            batch_size = 40  # Smaller batch size for reliability
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                
                batch_df = df.iloc[start_idx:end_idx]
                records = batch_df.to_dict('records')
                
                # Clean NaN values
                for record in records:
                    for key, value in list(record.items()):
                        if pd.isna(value):
                            record[key] = None
                
                # Handle failures with retries
                max_retries = 3
                success = False
                
                for retry in range(max_retries):
                    try:
                        # Add upsert preference to avoid duplicate key errors
                        insert_headers = self.headers.copy()
                        insert_headers["Prefer"] = "return=minimal,resolution=ignore-duplicates"
                        
                        response = requests.post(f"{self.rest_endpoint}/{table_name}",
                                               headers=insert_headers,
                                               json=records)
                        
                        if response.status_code in [200, 201, 204]:
                            logging.info(f"Inserted batch {start_idx//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size} into {table_name}")
                            success = True
                            break
                        else:
                            logging.warning(f"Failed to insert batch {start_idx}-{end_idx}: {response.text}")
                            time.sleep(1)  # Wait before retry
                    
                    except Exception as e:
                        logging.warning(f"Error in batch {start_idx}-{end_idx}, retry {retry+1}/{max_retries}: {str(e)}")
                        time.sleep(1)  # Wait before retry
                
                if not success:
                    logging.error(f"Failed to insert batch {start_idx}-{end_idx} after {max_retries} retries")
                
                # Sleep between batches to avoid rate limiting
                time.sleep(0.5)
            
            logging.info(f"Inserted {total_rows} rows into {table_name}")
            
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {str(e)}")
            raise

    def extract_date_from_zip(self):
        """Extract date (year, month) from zip filename in input directory"""
        try:
            # Find zip files in input directory
            input_dir = "input"
            zip_files = [f for f in os.listdir(input_dir) if f.endswith('.zip') and not f.startswith('.')]
            
            if not zip_files:
                logging.warning("No zip files found in input directory")
                return None
            
            # Get the first zip file (assuming we process one at a time)
            zip_file = zip_files[0]
            logging.info(f"Found zip file: {zip_file}")
            
            # Extract date using regex
            match = re.search(r'(\d{4})-(\d{2})', zip_file)
            if match:
                year = match.group(1)
                month = match.group(2)
                logging.info(f"Extracted date: {year}-{month}")
                return (year, month)
            else:
                logging.warning(f"Could not extract date from zip filename: {zip_file}")
                return None
            
        except Exception as e:
            logging.error(f"Error extracting date from zip: {str(e)}")
            return None

def main():
    try:
        # Initialize the data inserter
        inserter = SupabaseDataInserter()
        
        # Insert all data
        inserter.insert_all_data()
        
        logging.info("NCUA data insertion completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 