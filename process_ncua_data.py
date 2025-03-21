import os
import zipfile
import pandas as pd
import logging
import time
from supabase import create_client, Client
from dotenv import load_dotenv
import tempfile
import re
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/process_ncua_data.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

class NCUADataProcessor:
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
            
        # Create output directory if it doesn't exist
        if not os.path.exists('output'):
            os.makedirs('output')
    
    def _upload_file_to_storage(self, file_path, bucket_name="ncua_data"):
        """Upload a file to Supabase Storage"""
        file_name = os.path.basename(file_path)
        
        try:
            # Create bucket if it doesn't exist
            try:
                self.supabase.storage.from_(bucket_name).list()
                logging.info(f"Bucket '{bucket_name}' already exists")
            except Exception as e:
                if "The resource was not found" in str(e) or "Bucket not found" in str(e):
                    try:
                        # Create bucket
                        self.supabase.storage.create_bucket(bucket_name, {'public': True})
                        logging.info(f"Created bucket '{bucket_name}'")
                        # Wait a moment for the bucket to be fully created
                        time.sleep(1)
                    except Exception as create_error:
                        logging.error(f"Error creating bucket: {str(create_error)}")
                        raise
                else:
                    logging.error(f"Error checking bucket: {str(e)}")
                    raise
            
            # Upload file
            with open(file_path, "rb") as f:
                result = self.supabase.storage.from_(bucket_name).upload(file_name, f, {'content-type': 'application/zip'})
            
            # Get public URL
            public_url = self.supabase.storage.from_(bucket_name).get_public_url(file_name)
            logging.info(f"Uploaded file to storage: {public_url}")
            
            return public_url
            
        except Exception as e:
            logging.error(f"Error uploading file: {str(e)}")
            raise
    
    def _create_table(self, df, table_name):
        """Create a table in Supabase using pandas DataFrame"""
        try:
            logging.info(f"Creating table: {table_name}")
            
            # Generate column definitions based on DataFrame
            columns = []
            for col_name, dtype in df.dtypes.items():
                # Clean column name
                clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col_name.lower())
                
                # Map pandas dtype to SQL type
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "integer"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "float"
                elif pd.api.types.is_datetime64_dtype(dtype):
                    sql_type = "timestamp"
                else:
                    sql_type = "text"
                
                columns.append(f"{clean_col} {sql_type}")
            
            # Try to create the table directly with a sample record
            try:
                # Prepare a sample record
                sample_record = {}
                for col in df.columns:
                    clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
                    sample_record[clean_col] = None
                
                # Insert sample record to create table
                response = self.supabase.table(table_name).insert(sample_record).execute()
                logging.info(f"Created table {table_name} with initial record")
                
                # Delete the sample record if it has an ID
                try:
                    data = response.data
                    if data and len(data) > 0 and 'id' in data[0]:
                        self.supabase.table(table_name).delete().eq('id', data[0]['id']).execute()
                except Exception as e:
                    logging.warning(f"Could not delete sample record: {str(e)}")
                
            except Exception as e:
                if 'duplicate key value' in str(e) or 'already exists' in str(e):
                    logging.info(f"Table {table_name} already exists")
                else:
                    logging.error(f"Error creating table {table_name}: {str(e)}")
                    raise
        
        except Exception as e:
            logging.error(f"Error in create_table: {str(e)}")
            raise
    
    def _insert_data(self, df, table_name):
        """Insert data into a Supabase table"""
        try:
            # Clean column names
            df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col.lower()) for col in df.columns]
            
            # Convert DataFrame to records
            records = df.to_dict('records')
            logging.info(f"Inserting {len(records)} records into {table_name}")
            
            # Insert data in batches
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # Clean up data in batch (handle NaN, None, etc.)
                clean_batch = []
                for record in batch:
                    clean_record = {}
                    for key, value in record.items():
                        if pd.isna(value):
                            clean_record[key] = None
                        else:
                            clean_record[key] = value
                    clean_batch.append(clean_record)
                
                retry_count = 0
                while retry_count < 3:
                    try:
                        response = self.supabase.table(table_name).insert(clean_batch).execute()
                        logging.info(f"Inserted batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size}")
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count >= 3:
                            logging.error(f"Failed to insert batch after 3 retries: {str(e)}")
                            raise
                        logging.warning(f"Retrying batch insert ({retry_count}/3): {str(e)}")
                        time.sleep(1)
                
                # Sleep between batches to avoid rate limiting
                time.sleep(0.5)
            
            logging.info(f"Successfully inserted all data into {table_name}")
        
        except Exception as e:
            logging.error(f"Error in insert_data: {str(e)}")
            raise
    
    def process_zip_file(self, zip_file_path):
        """Extract and process a ZIP file containing NCUA data"""
        logging.info(f"Processing ZIP file: {zip_file_path}")
        
        # First, upload the zip file to storage
        try:
            public_url = self._upload_file_to_storage(zip_file_path)
            
            # Create file entry in the files table
            try:
                self._create_files_table()
                file_name = os.path.basename(zip_file_path)
                self.supabase.table("files").insert({"name": file_name, "url": public_url}).execute()
                logging.info(f"Added file entry to files table: {file_name}")
            except Exception as e:
                logging.error(f"Error adding file entry: {str(e)}")
                # Continue even if we can't add to files table
        except Exception as e:
            logging.error(f"Error uploading zip file: {str(e)}")
            # Continue processing locally even if upload fails
        
        # Extract date information from filename
        match = re.search(r'(\d{4})-(\d{2})', os.path.basename(zip_file_path))
        if match:
            year, month = match.group(1), match.group(2)
            logging.info(f"Processing data for year {year}, month {month}")
        else:
            year, month = None, None
            logging.warning("Could not extract date information from filename")
        
        # Create a temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            logging.info(f"Extracting to temporary directory: {temp_dir}")
            
            # Extract the ZIP file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Process extracted files
            for file_name in os.listdir(temp_dir):
                if file_name.endswith('.txt'):
                    file_path = os.path.join(temp_dir, file_name)
                    logging.info(f"Processing file: {file_name}")
                    
                    # Try to read the file with different encodings and separators
                    df = self._read_data_file(file_path)
                    
                    if df is not None and not df.empty:
                        # Generate table name
                        base_name = os.path.splitext(file_name)[0]
                        if year and month:
                            table_name = f"{base_name.lower().replace('-', '_').replace(' ', '_')}_{year}_{month}"
                        else:
                            table_name = f"{base_name.lower().replace('-', '_').replace(' ', '_')}"
                        
                        # Create table and insert data
                        self._create_table(df, table_name)
                        self._insert_data(df, table_name)
                        logging.info(f"Successfully processed {file_name} into table {table_name}")
        
        # Move the processed zip file to output directory
        output_path = os.path.join('output', os.path.basename(zip_file_path))
        shutil.move(zip_file_path, output_path)
        logging.info(f"Moved processed zip file to: {output_path}")
    
    def _read_data_file(self, file_path):
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
            
            # If all else fails, try to read the first few lines to diagnose
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                sample = f.read(1000)
                logging.warning(f"Could not parse file as structured data. Sample: {sample}")
            
            return None
        
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return None
    
    def _create_files_table(self):
        """Create the files table for storing file metadata"""
        try:
            # Try to create the table directly with a sample record
            try:
                # Create the files table
                sample_record = {
                    "name": "sample.txt",
                    "url": "https://example.com/sample.txt"
                }
                
                # Insert sample record to create table
                response = self.supabase.table("files").insert(sample_record).execute()
                logging.info("Created files table with initial record")
                
                # Delete the sample record
                try:
                    data = response.data
                    if data and len(data) > 0 and 'id' in data[0]:
                        self.supabase.table("files").delete().eq('id', data[0]['id']).execute()
                        logging.info("Deleted sample record from files table")
                except Exception as e:
                    logging.warning(f"Could not delete sample record: {str(e)}")
                
            except Exception as e:
                if 'duplicate key value' in str(e) or 'already exists' in str(e):
                    logging.info("Files table already exists")
                else:
                    logging.error(f"Error creating files table: {str(e)}")
                    raise
        
        except Exception as e:
            logging.error(f"Error in create_files_table: {str(e)}")
            raise

def main():
    try:
        # Initialize processor
        processor = NCUADataProcessor()
        
        # Process the zip file
        zip_file = os.path.join('input', 'call-report-data-2024-03.zip')
        
        if os.path.exists(zip_file):
            processor.process_zip_file(zip_file)
            logging.info("Successfully processed NCUA data")
        else:
            logging.error(f"Zip file not found: {zip_file}")
    
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 