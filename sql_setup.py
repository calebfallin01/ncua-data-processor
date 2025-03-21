import os
import requests
import json
import logging
import time
from dotenv import load_dotenv
import pandas as pd
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/sql_setup.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

class SupabaseDirectSQL:
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
            "Content-Type": "application/json"
        }
        
        # Base endpoint for Supabase REST API
        self.rest_endpoint = f"{self.url}/rest/v1"
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
    
    def setup_all_tables(self):
        """Set up all tables needed for NCUA data"""
        try:
            # Create the files table
            self.create_files_table()
            
            # Process sample CSV files to create data tables
            self.process_csv_files()
            
            logging.info("Successfully set up all tables in Supabase")
            
        except Exception as e:
            logging.error(f"Error setting up tables: {str(e)}")
            raise
    
    def create_files_table(self):
        """Create the files table for storing file metadata"""
        try:
            # Create the files table
            table_name = "files"
            schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT NOW()
            );
            """
            
            self.execute_sql(schema)
            logging.info(f"Created table {table_name}")
            
            # Check if zip file is already in the files table
            self.insert_file_metadata("call-report-data-2024-03.zip", 
                                     f"{self.url}/storage/v1/object/public/ncua_data/call-report-data-2024-03.zip")
            
        except Exception as e:
            logging.error(f"Error creating files table: {str(e)}")
            raise
    
    def insert_file_metadata(self, filename, url):
        """Insert file metadata into the files table"""
        try:
            # Check if file already exists
            query_url = f"{self.rest_endpoint}/files?name=eq.{filename}"
            response = requests.get(query_url, headers=self.headers)
            
            if response.status_code == 200 and len(response.json()) > 0:
                logging.info(f"File {filename} already exists in files table")
                return
            
            # Insert new record
            data = {
                "name": filename,
                "url": url
            }
            
            insert_url = f"{self.rest_endpoint}/files"
            response = requests.post(insert_url, headers=self.headers, json=data)
            
            if response.status_code == 201:
                logging.info(f"Added file {filename} to files table")
            else:
                logging.error(f"Failed to insert file metadata: {response.text}")
                raise Exception(f"Failed to insert file metadata: {response.text}")
            
        except Exception as e:
            logging.error(f"Error inserting file metadata: {str(e)}")
            raise
    
    def process_csv_files(self):
        """Process CSV files to create and populate tables"""
        try:
            # Find all sample CSV files
            csv_files = glob.glob(os.path.join('output', 'extracted_data', '*_sample.csv'))
            
            for csv_file in csv_files:
                # Read CSV file
                df = pd.read_csv(csv_file)
                
                # Generate table name from filename
                base_name = os.path.basename(csv_file)
                table_name = base_name.replace('_sample.csv', '').lower().replace(' ', '_').replace('-', '_')
                table_name = f"{table_name}_2024_03"
                
                # Create table
                self.create_table_from_dataframe(df, table_name)
                
                # Insert data
                self.insert_csv_data(csv_file, table_name)
                
                logging.info(f"Processed {csv_file} into table {table_name}")
                
        except Exception as e:
            logging.error(f"Error processing CSV files: {str(e)}")
            raise
    
    def create_table_from_dataframe(self, df, table_name):
        """Create a table based on DataFrame structure"""
        try:
            # Generate column definitions
            columns = []
            for col in df.columns:
                # Clean column name
                clean_col = col.lower().replace(' ', '_').replace('-', '_')
                
                # Get data type
                dtype = df[col].dtype
                
                # Map pandas dtype to SQL type
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "NUMERIC"
                elif pd.api.types.is_datetime64_dtype(dtype):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "TEXT"
                
                columns.append(f"{clean_col} {sql_type}")
            
            # Create SQL statement
            schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                {', '.join(columns)}
            );
            """
            
            self.execute_sql(schema)
            logging.info(f"Created table {table_name}")
            
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {str(e)}")
            raise
    
    def insert_csv_data(self, csv_file, table_name):
        """Insert CSV data into a table"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Clean column names
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Convert DataFrame to records
            records = df.to_dict('records')
            
            # Clean records (handle NaN values)
            for record in records:
                for key, value in list(record.items()):
                    if pd.isna(value):
                        record[key] = None
            
            # Insert data in batches
            batch_size = 50
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                insert_url = f"{self.rest_endpoint}/{table_name}"
                response = requests.post(insert_url, headers=self.headers, json=batch)
                
                if response.status_code == 201:
                    logging.info(f"Inserted batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size} into {table_name}")
                else:
                    logging.error(f"Failed to insert batch: {response.text}")
                    raise Exception(f"Failed to insert batch: {response.text}")
                
                # Sleep between batches to avoid rate limiting
                time.sleep(0.5)
            
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {str(e)}")
            raise
    
    def execute_sql(self, sql_query):
        """Execute a SQL query directly against the database"""
        try:
            # Use the Supabase service_role key to execute SQL
            headers = {
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json"
            }
            
            # Use the REST API to execute SQL via rpc
            url = f"{self.url}/rest/v1/rpc/execute_sql"
            data = {"query": sql_query}
            
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 200:
                logging.info("SQL query executed successfully")
                return response.json()
            else:
                logging.error(f"Failed to execute SQL query: {response.text}")
                raise Exception(f"Failed to execute SQL query: {response.text}")
            
        except Exception as e:
            logging.error(f"Error executing SQL query: {str(e)}")
            raise

def main():
    try:
        # Initialize Supabase direct SQL
        supabase_sql = SupabaseDirectSQL()
        
        # Set up all tables
        supabase_sql.setup_all_tables()
        
        logging.info("Successfully set up NCUA data in Supabase")
        
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 