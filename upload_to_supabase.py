import os
import pandas as pd
import logging
import time
from supabase import create_client, Client
from dotenv import load_dotenv
import re
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/upload_to_supabase.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

class SupabaseUploader:
    def __init__(self):
        """Initialize Supabase connection"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
            
        try:
            self.supabase: Client = create_client(self.url, self.key)
            logging.info("Successfully connected to Supabase")
        except Exception as e:
            logging.error(f"Error connecting to Supabase: {str(e)}")
            raise
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
    
    def upload_file_to_storage(self, file_path, bucket_name="ncua_data"):
        """Upload a file to Supabase Storage"""
        file_name = os.path.basename(file_path)
        
        try:
            # Create bucket if it doesn't exist
            try:
                # Check if bucket exists
                buckets = self.supabase.storage.list_buckets()
                bucket_exists = False
                for bucket in buckets:
                    if bucket.get('name') == bucket_name:
                        bucket_exists = True
                        break
                
                if not bucket_exists:
                    self.supabase.storage.create_bucket(bucket_name)
                    logging.info(f"Created bucket '{bucket_name}'")
                    time.sleep(1)  # Wait for bucket to be created
                else:
                    logging.info(f"Bucket '{bucket_name}' already exists")
                
            except Exception as e:
                logging.error(f"Error checking/creating bucket: {str(e)}")
                raise
            
            # Upload the file
            with open(file_path, "rb") as f:
                file_bytes = f.read()
                self.supabase.storage.from_(bucket_name).upload(path=file_name, file=file_bytes, file_options={"content-type": "application/octet-stream"})
            
            # Get public URL
            public_url = self.supabase.storage.from_(bucket_name).get_public_url(file_name)
            logging.info(f"Uploaded file to storage: {public_url}")
            
            # Store file metadata
            self.create_files_table()
            self.supabase.table("files").insert({"name": file_name, "url": public_url}).execute()
            logging.info(f"Added file entry to files table: {file_name}")
            
            return public_url
            
        except Exception as e:
            logging.error(f"Error uploading file: {str(e)}")
            raise
    
    def create_files_table(self):
        """Create the files table if it doesn't exist"""
        try:
            # Create table directly with insert
            try:
                # Try to select from the table to see if it exists
                self.supabase.table("files").select("*").limit(1).execute()
                logging.info("Files table already exists")
            except Exception as e:
                # If table doesn't exist, create a sample record to create it
                if 'relation "files" does not exist' in str(e):
                    sample_record = {
                        "name": "sample.txt",
                        "url": "https://example.com/sample.txt"
                    }
                    self.supabase.table("files").insert(sample_record).execute()
                    logging.info("Created files table")
                    
                    # Delete the sample record
                    try:
                        self.supabase.table("files").delete().eq("name", "sample.txt").execute()
                    except:
                        pass
                else:
                    logging.warning(f"Error checking files table: {str(e)}")
                    # Continue anyway
        except Exception as e:
            logging.error(f"Error creating files table: {str(e)}")
            # Continue even if there's an error, as the table might already exist
    
    def upload_csv_to_table(self, csv_file, table_name):
        """Upload a CSV file to a Supabase table"""
        try:
            logging.info(f"Processing {csv_file} into table {table_name}")
            
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Clean column names
            df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col.lower()) for col in df.columns]
            
            # Create the table
            self.create_table_from_dataframe(df, table_name)
            
            # Insert data in batches
            self.insert_dataframe(df, table_name)
            
            logging.info(f"Successfully uploaded {csv_file} to table {table_name}")
            
        except Exception as e:
            logging.error(f"Error uploading {csv_file}: {str(e)}")
            raise
    
    def create_table_from_dataframe(self, df, table_name):
        """Create a table based on DataFrame structure"""
        try:
            # First try to query the table to see if it exists
            try:
                self.supabase.table(table_name).select("*").limit(1).execute()
                logging.info(f"Table {table_name} already exists")
                return
            except Exception as e:
                if 'relation "' in str(e) and '" does not exist' in str(e):
                    # Table doesn't exist, create it with a sample record
                    sample_record = {}
                    for col in df.columns:
                        sample_record[col] = None
                    
                    self.supabase.table(table_name).insert(sample_record).execute()
                    logging.info(f"Created table {table_name}")
                    
                    # Delete the sample record
                    try:
                        response = self.supabase.table(table_name).select("*").limit(1).execute()
                        if response.data and len(response.data) > 0:
                            id_val = response.data[0].get('id')
                            if id_val:
                                self.supabase.table(table_name).delete().eq('id', id_val).execute()
                    except:
                        pass
                else:
                    logging.error(f"Error checking table {table_name}: {str(e)}")
                    raise
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {str(e)}")
            # Continue since the table might already exist
    
    def insert_dataframe(self, df, table_name):
        """Insert DataFrame data into a table"""
        try:
            # Convert DataFrame to records
            records = df.to_dict('records')
            
            # Clean up NaN values
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            # Insert in batches to avoid timeouts
            batch_size = 50
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # Try with retries
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        self.supabase.table(table_name).insert(batch).execute()
                        logging.info(f"Inserted batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size}")
                        break
                    except Exception as e:
                        if retry < max_retries - 1:
                            logging.warning(f"Retry {retry+1}/{max_retries} for batch {i//batch_size + 1}: {str(e)}")
                            time.sleep(1)
                        else:
                            logging.error(f"Failed to insert batch after {max_retries} retries: {str(e)}")
                            raise
                
                # Sleep between batches to avoid rate limiting
                time.sleep(0.5)
            
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {str(e)}")
            raise
    
    def upload_directory(self, directory_path):
        """Upload all CSV files in a directory to Supabase"""
        try:
            # Make sure the directory exists
            if not os.path.exists(directory_path):
                raise ValueError(f"Directory {directory_path} does not exist")
            
            # Upload the source ZIP file
            zip_files = glob.glob(os.path.join(directory_path, '..', '..', 'input', '*.zip'))
            if zip_files:
                self.upload_file_to_storage(zip_files[0])
            
            # Find all CSV files
            csv_files = glob.glob(os.path.join(directory_path, '*_sample.csv'))
            
            for csv_file in csv_files:
                # Generate table name from filename
                base_name = os.path.basename(csv_file)
                table_name = re.sub(r'_sample\.csv$', '', base_name)
                table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())
                
                # Add date suffix
                table_name = f"{table_name}_2024_03"
                
                # Upload to Supabase
                self.upload_csv_to_table(csv_file, table_name)
            
            logging.info(f"Successfully uploaded all CSV files from {directory_path}")
            
        except Exception as e:
            logging.error(f"Error uploading directory {directory_path}: {str(e)}")
            raise

def main():
    try:
        # Initialize uploader
        uploader = SupabaseUploader()
        
        # Upload extracted sample data
        extracted_data_dir = os.path.join('output', 'extracted_data')
        
        if os.path.exists(extracted_data_dir):
            uploader.upload_directory(extracted_data_dir)
            logging.info("Successfully uploaded NCUA data to Supabase")
        else:
            logging.error(f"Directory not found: {extracted_data_dir}")
    
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 