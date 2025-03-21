import os
import requests
import json
import logging
import time
from dotenv import load_dotenv
import pandas as pd
import glob
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/upload_data.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

class SupabaseAPIUploader:
    def __init__(self):
        """Initialize Supabase API connection details"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
            
        # Set up headers for API requests
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"  # For better performance on inserts
        }
        
        # Base endpoint for Supabase REST API
        self.rest_endpoint = f"{self.url}/rest/v1"
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
    
    def upload_csv_data(self):
        """Upload CSV data to Supabase tables"""
        try:
            # Find all sample CSV files
            csv_files = glob.glob(os.path.join('output', 'extracted_data', '*_sample.csv'))
            
            # First, create table to store file information
            self.ensure_files_table_exists()
            
            # Record the zip file
            self.add_file_record("call-report-data-2024-03.zip", 
                               f"{self.url}/storage/v1/object/public/ncua_data/call-report-data-2024-03.zip")
            
            # Process each CSV file
            for csv_file in csv_files:
                try:
                    # Generate table name from filename
                    base_name = os.path.basename(csv_file)
                    table_name = re.sub(r'_sample\.csv$', '', base_name)
                    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())
                    table_name = f"{table_name}_2024_03"
                    
                    # Upload the data
                    self.upload_file_to_table(csv_file, table_name)
                    
                    logging.info(f"Successfully processed {csv_file}")
                except Exception as e:
                    logging.error(f"Error processing {csv_file}: {str(e)}")
                    # Continue with other files
            
            logging.info("Completed data upload to Supabase")
            
        except Exception as e:
            logging.error(f"Error in upload_csv_data: {str(e)}")
            raise
    
    def ensure_files_table_exists(self):
        """Ensure the files table exists"""
        try:
            # Try to query the files table
            response = requests.get(f"{self.rest_endpoint}/files?limit=1", headers=self.headers)
            
            if response.status_code == 200:
                logging.info("Files table already exists")
                return True
            
            # If table doesn't exist, create it using a sample record
            # The Supabase REST API will automatically create the table with appropriate columns
            data = {
                "name": "_init_file_record.txt",
                "url": "https://example.com/init_file",
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            
            response = requests.post(f"{self.rest_endpoint}/files", headers=self.headers, json=data)
            
            if response.status_code == 201:
                logging.info("Created files table")
                
                # Try to delete the init record
                try:
                    requests.delete(
                        f"{self.rest_endpoint}/files?name=eq._init_file_record.txt", 
                        headers=self.headers
                    )
                except:
                    pass
                
                return True
            else:
                logging.error(f"Failed to create files table: {response.text}")
                return False
            
        except Exception as e:
            logging.error(f"Error in ensure_files_table_exists: {str(e)}")
            return False
    
    def add_file_record(self, filename, url):
        """Add a record to the files table"""
        try:
            # Check if file already exists
            response = requests.get(
                f"{self.rest_endpoint}/files?name=eq.{filename}", 
                headers=self.headers
            )
            
            if response.status_code == 200 and len(response.json()) > 0:
                logging.info(f"File {filename} already exists in files table")
                return True
            
            # Add file record
            data = {
                "name": filename,
                "url": url,
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            
            response = requests.post(f"{self.rest_endpoint}/files", headers=self.headers, json=data)
            
            if response.status_code == 201:
                logging.info(f"Added file record for {filename}")
                return True
            else:
                logging.error(f"Failed to add file record: {response.text}")
                return False
            
        except Exception as e:
            logging.error(f"Error in add_file_record: {str(e)}")
            return False
    
    def upload_file_to_table(self, csv_file, table_name):
        """Upload a CSV file to a Supabase table"""
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Clean column names
            df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col.lower()) for col in df.columns]
            
            # Convert DataFrame to records
            records = df.to_dict('records')
            
            # Clean records (handle NaN values)
            for record in records:
                for key, value in list(record.items()):
                    if pd.isna(value):
                        record[key] = None
            
            # Create table if it doesn't exist by inserting a sample record
            # First check if table exists
            table_exists = False
            
            try:
                response = requests.get(f"{self.rest_endpoint}/{table_name}?limit=1", headers=self.headers)
                table_exists = response.status_code == 200
            except:
                table_exists = False
            
            if not table_exists:
                # Create table with a sample record
                if records:
                    sample_record = records[0].copy()
                    
                    # Set all values to None to avoid potential data type issues
                    for key in sample_record:
                        sample_record[key] = None
                    
                    # Insert sample record to create table
                    create_headers = self.headers.copy()
                    create_headers["Prefer"] = "resolution=ignore-duplicates,return=minimal"
                    
                    response = requests.post(
                        f"{self.rest_endpoint}/{table_name}", 
                        headers=create_headers, 
                        json=sample_record
                    )
                    
                    if response.status_code == 201:
                        logging.info(f"Created table {table_name}")
                    else:
                        logging.error(f"Failed to create table {table_name}: {response.text}")
                        raise Exception(f"Failed to create table {table_name}: {response.text}")
            
            # Insert data in batches
            batch_size = 50
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # Insert batch
                response = requests.post(
                    f"{self.rest_endpoint}/{table_name}", 
                    headers=self.headers, 
                    json=batch
                )
                
                if response.status_code == 201:
                    logging.info(f"Inserted batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size} into {table_name}")
                else:
                    logging.error(f"Failed to insert batch into {table_name}: {response.text}")
                    raise Exception(f"Failed to insert batch into {table_name}: {response.text}")
                
                # Sleep to avoid rate limits
                time.sleep(0.5)
            
            logging.info(f"Successfully uploaded {csv_file} to {table_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error in upload_file_to_table for {csv_file}: {str(e)}")
            raise

def main():
    try:
        # Create uploader
        uploader = SupabaseAPIUploader()
        
        # Upload data
        uploader.upload_csv_data()
        
        logging.info("Data upload completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 