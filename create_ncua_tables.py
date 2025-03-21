import os
import requests
import logging
import time
import pandas as pd
import glob
import re
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/create_ncua_tables.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

class SupabaseTableManager:
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
            "Prefer": "return=minimal"
        }
        
        # Base endpoint for Supabase REST API
        self.rest_endpoint = f"{self.url}/rest/v1"
        
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Load column type definitions from description files
        self.column_type_definitions = self.load_column_type_definitions()
    
    def load_column_type_definitions(self):
        """Load column type definitions from Acct-Desc* files"""
        column_types = {}
        
        # Try to get the zip filename to look in the zip-specific directory
        zip_name = self.get_zip_filename()
        if zip_name:
            zip_name_without_ext = os.path.splitext(zip_name)[0]
            zip_specific_dir = os.path.join('output', 'extracted_data', zip_name_without_ext)
            
            # Path to description files in zip-specific directory
            if os.path.exists(zip_specific_dir):
                zip_desc_files = [
                    os.path.join(zip_specific_dir, 'AcctDesc.txt'),
                    os.path.join(zip_specific_dir, 'Acct-DescGrants.txt'),
                    os.path.join(zip_specific_dir, 'Acct-DescTradeNames.txt')
                ]
                
                # Try zip-specific files first
                has_zip_desc_files = False
                for file_path in zip_desc_files:
                    if os.path.exists(file_path):
                        has_zip_desc_files = True
                        self._load_column_types_from_file(file_path, column_types)
                
                if has_zip_desc_files:
                    logging.info(f"Loaded column types from zip-specific directory: {zip_specific_dir}")
                else:
                    # Fall back to main directory
                    logging.warning(f"No description files found in {zip_specific_dir}, falling back to main directory")
                    self._load_column_types_from_main_dir(column_types)
            else:
                # Fall back to main directory
                logging.warning(f"Zip-specific directory {zip_specific_dir} not found, falling back to main directory")
                self._load_column_types_from_main_dir(column_types)
        else:
            # Fall back to main directory
            self._load_column_types_from_main_dir(column_types)
        
        # Manually ensure critical join fields have consistent types
        if 'CU_NUMBER' in column_types and column_types['CU_NUMBER'].lower() == 'int':
            logging.info("Ensuring CU_NUMBER is set to INTEGER type for consistent joins")
            column_types['CU_NUMBER'] = 'int'
            column_types['CU-NUMBER'] = 'int'
            column_types['CUNUMBER'] = 'int'
        
        logging.info(f"Loaded {len(column_types)} column type definitions")
        return column_types
    
    def _load_column_types_from_main_dir(self, column_types):
        """Helper method to load column types from the main extracted_data directory"""
        # Path to description files in main directory
        desc_files = [
            'output/extracted_data/AcctDesc.txt',
            'output/extracted_data/Acct-DescGrants.txt',
            'output/extracted_data/Acct-DescTradeNames.txt'
        ]
        
        for file_path in desc_files:
            self._load_column_types_from_file(file_path, column_types)
    
    def _load_column_types_from_file(self, file_path, column_types):
        """Helper method to load column types from a specific file"""
        if os.path.exists(file_path):
            logging.info(f"Loading column type definitions from {file_path}")
            try:
                with open(file_path, 'r') as file:
                    csv_reader = csv.reader(file)
                    # Skip header
                    next(csv_reader)
                    
                    for row in csv_reader:
                        if len(row) >= 3:  # Ensure there are enough columns
                            field_name = row[0].strip().strip('"')  # Field name
                            field_type = row[1].strip().strip('"')  # Field type
                            
                            # Store field type information
                            column_types[field_name.upper()] = field_type
                            
                            # Also store with CU_NUMBER format
                            if '_' in field_name:
                                alt_name = field_name.replace('_', '-')
                                column_types[alt_name.upper()] = field_type
                            elif '-' in field_name:
                                alt_name = field_name.replace('-', '_')
                                column_types[alt_name.upper()] = field_type
            
            except Exception as e:
                logging.error(f"Error parsing column types from {file_path}: {str(e)}")
        else:
            logging.warning(f"Description file not found: {file_path}")
    
    def get_sql_type_for_column(self, column_name, pandas_dtype):
        """Determine SQL type for a column based on description files or pandas dtype"""
        # Critical join fields that should have consistent types across all tables
        critical_join_fields = {
            'cu_number': 'INTEGER',
            'join_number': 'INTEGER',
            'cycle_date': 'TIMESTAMP'
        }
        
        # Check if this is a critical join field
        col_lower = column_name.lower()
        if col_lower in critical_join_fields:
            logging.debug(f"Using consistent type {critical_join_fields[col_lower]} for join field {column_name}")
            return critical_join_fields[col_lower]
        
        # Special case for phone/fax columns - always use TEXT
        if any(keyword in col_lower for keyword in ['phone', 'fax', 'phonenumber']):
            return "TEXT"
        
        # First try to get the type from our definitions
        column_key = column_name.upper().strip()
        
        if column_key in self.column_type_definitions:
            ncua_type = self.column_type_definitions[column_key].lower()
            
            # Map NCUA types to PostgreSQL types
            if ncua_type == 'int':
                return "INTEGER"
            elif ncua_type == 'smallint':
                return "SMALLINT"
            elif ncua_type == 'bigint':
                return "BIGINT"
            elif ncua_type == 'varchar':
                return "TEXT"
            elif ncua_type == 'char':
                return "TEXT"
            elif ncua_type == 'date' or ncua_type == 'smalldatetime':
                return "TIMESTAMP"
            elif ncua_type == 'decimal' or ncua_type == 'float':
                return "NUMERIC"
            else:
                # Default to TEXT for unknown types
                return "TEXT"
        
        # If not found in definitions, use pandas dtype as fallback
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "NUMERIC"
        elif pd.api.types.is_datetime64_dtype(pandas_dtype):
            return "TIMESTAMP"
        else:
            return "TEXT"
    
    def create_all_tables(self):
        """Create all tables for NCUA data"""
        try:
            # Extract date from zip file in input folder
            date_info = self.extract_date_from_zip()
            if not date_info:
                logging.warning("Could not extract date from zip file, using default date")
                year = "2024"
                month = "03"
            else:
                year, month = date_info
            
            logging.info(f"Creating tables for period: {year}-{month}")
            
            # Create the files table first
            self.create_files_table()
            
            # Process the CSV sample files
            self.create_tables_from_csv_files(year, month)
            
            # Register the zip file in the files table
            zip_filename = self.get_zip_filename()
            if zip_filename:
                self.add_file_to_files_table(zip_filename, 
                                          f"{self.url}/storage/v1/object/public/ncua_data/{zip_filename}")
            
            logging.info(f"Successfully created all tables for {year}-{month}")
        
        except Exception as e:
            logging.error(f"Error creating tables: {str(e)}")
            raise
    
    def create_fs220d_special_table(self, year, month):
        """Create FS220D table with all TEXT columns to prevent integer overflow issues"""
        try:
            table_name = f"fs220d_{year}_{month}"
            logging.info(f"Creating special FS220D table: {table_name}")
            
            # Check if sample file exists
            sample_file_path = 'output/extracted_data/FS220D_sample.csv'
            if not os.path.exists(sample_file_path):
                logging.error(f"Sample file {sample_file_path} not found")
                return False
            
            # First drop the existing table
            drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
            
            # Execute SQL
            result = self.execute_sql(drop_table_sql)
            if not result:
                logging.error(f"Failed to drop table {table_name}")
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
                
                # Use TEXT for all columns, except timestamps
                dtype = df[col].dtype
                if pd.api.types.is_datetime64_dtype(dtype):
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
            result = self.execute_sql(create_table_sql)
            if not result:
                logging.error(f"Failed to create table {table_name}")
                return False
                
            logging.info(f"Created table {table_name} with all TEXT types")
            
            # No sample data insertion - removed
            return True
            
        except Exception as e:
            logging.error(f"Error creating FS220D special table: {str(e)}")
            return False
    
    def execute_sql(self, sql_query):
        """Execute a SQL query using the correct parameter name"""
        try:
            data = {"sql_query": sql_query}
            response = requests.post(f"{self.url}/rest/v1/rpc/execute_sql",
                                    headers=self.headers,
                                    json=data)
            
            if response.status_code in [200, 204]:
                logging.info(f"SQL executed successfully: {sql_query[:50]}...")
                return True
            else:
                logging.error(f"SQL execution failed: {response.text}")
                return False
        
        except Exception as e:
            logging.error(f"Error executing SQL: {str(e)}")
            return False
    
    def create_files_table(self):
        """Create the files table"""
        sql = """
        CREATE TABLE IF NOT EXISTS files (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            uploaded_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        result = self.execute_sql(sql)
        if result:
            logging.info("Files table created successfully")
        else:
            logging.warning("Failed to create files table using SQL, trying direct API approach")
            self.create_table_via_insert("files", {"name": "temp", "url": "http://example.com"})
    
    def add_file_to_files_table(self, file_name, file_url):
        """Add a file record to the files table"""
        try:
            # First check if the file already exists
            response = requests.get(f"{self.rest_endpoint}/files?name=eq.{file_name}",
                                   headers=self.headers)
            
            if response.status_code == 200 and len(response.json()) > 0:
                logging.info(f"File {file_name} already exists in files table")
                return True
            
            # Insert the file record
            data = {
                "name": file_name,
                "url": file_url,
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            
            response = requests.post(f"{self.rest_endpoint}/files",
                                    headers=self.headers,
                                    json=data)
            
            if response.status_code == 201:
                logging.info(f"File {file_name} added to files table")
                return True
            else:
                logging.error(f"Failed to add file to files table: {response.text}")
                return False
            
        except Exception as e:
            logging.error(f"Error adding file to files table: {str(e)}")
            return False
    
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
    
    def get_zip_filename(self):
        """Get the zip filename from input directory"""
        try:
            input_dir = "input"
            zip_files = [f for f in os.listdir(input_dir) if f.endswith('.zip') and not f.startswith('.')]
            
            if not zip_files:
                return None
            
            return zip_files[0]
        except Exception as e:
            logging.error(f"Error getting zip filename: {str(e)}")
            return None
    
    def create_tables_from_csv_files(self, year, month):
        """Create tables based on CSV sample files"""
        # First try to find files in the zip-specific directory
        zip_name = self.get_zip_filename()
        if zip_name:
            zip_name_without_ext = os.path.splitext(zip_name)[0]
            zip_specific_dir = os.path.join('output', 'extracted_data', zip_name_without_ext)
            
            if os.path.exists(zip_specific_dir):
                csv_files = glob.glob(os.path.join(zip_specific_dir, '*_sample.csv'))
                if csv_files:
                    logging.info(f"Using sample files from zip-specific directory: {zip_specific_dir}")
                else:
                    # Fall back to the main extracted_data directory
                    logging.warning(f"No sample files found in {zip_specific_dir}, falling back to main directory")
                    csv_files = glob.glob(os.path.join('output', 'extracted_data', '*_sample.csv'))
            else:
                # Fall back to the main extracted_data directory
                logging.warning(f"Zip-specific directory {zip_specific_dir} not found, falling back to main directory")
                csv_files = glob.glob(os.path.join('output', 'extracted_data', '*_sample.csv'))
        else:
            # If we can't determine the zip filename, use the main directory
            csv_files = glob.glob(os.path.join('output', 'extracted_data', '*_sample.csv'))
        
        for csv_file in csv_files:
            try:
                # Generate table name
                base_name = os.path.basename(csv_file)
                table_name = re.sub(r'_sample\.csv$', '', base_name)
                table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())
                table_name = f"{table_name}_{year}_{month}"
                
                logging.info(f"Processing table {table_name} from {base_name}")
                
                # Read CSV to get column structure
                df = pd.read_csv(csv_file)
                
                # Create the table
                self.create_table_from_dataframe(table_name, df)
                
                # No sample data insertion - removed
                
                logging.info(f"Created table {table_name}")
                
            except Exception as e:
                logging.error(f"Error processing {csv_file}: {str(e)}")
                # Continue with next file
    
    def create_table_from_dataframe(self, table_name, df):
        """Create a table based on DataFrame structure with column types from description files"""
        # Generate column definitions
        columns = []
        
        for col, dtype in df.dtypes.items():
            # Clean column name
            clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
            
            # Special case for phone/fax columns - always use TEXT
            if any(keyword in col.lower() for keyword in ['phone', 'fax', 'phonenumber']):
                sql_type = "TEXT"
            else:
                # Get SQL type from our helper function that checks description files
                sql_type = self.get_sql_type_for_column(col, dtype)
            
            columns.append(f"{clean_col} {sql_type}")
        
        # Generate SQL for table creation
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            {', '.join(columns)}
        );
        """
        
        # Try to create the table using SQL
        result = self.execute_sql(sql)
        if result:
            # Add a small delay after table creation to prevent 404 errors
            time.sleep(1)
        else:
            # If SQL execution fails, try alternative method using direct insert
            logging.warning(f"Failed to create table {table_name} using SQL, trying direct API approach")
            
            # Generate a sample record
            sample_record = {}
            for col in df.columns:
                clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
                sample_record[clean_col] = None
            
            self.create_table_via_insert(table_name, sample_record)
    
    def create_table_via_insert(self, table_name, sample_record):
        """Create a table by inserting a sample record"""
        try:
            # Add resolution=ignore-duplicates to avoid errors if the record already exists
            insert_headers = self.headers.copy()
            insert_headers["Prefer"] = "return=minimal,resolution=ignore-duplicates"
            
            response = requests.post(f"{self.rest_endpoint}/{table_name}",
                                    headers=insert_headers,
                                    json=sample_record)
            
            if response.status_code in [200, 201, 204]:
                logging.info(f"Table {table_name} created via sample insert")
                
                # Try to clean up the sample record
                try:
                    requests.delete(f"{self.rest_endpoint}/{table_name}?limit=1",
                                  headers=self.headers)
                except:
                    pass  # Ignore errors in cleanup
                
                return True
            else:
                logging.error(f"Failed to create table via insert: {response.text}")
                return False
            
        except Exception as e:
            logging.error(f"Error creating table via insert: {str(e)}")
            return False
    
    def insert_dataframe(self, table_name, df):
        """Insert DataFrame data into a table"""
        try:
            # Clean column names
            clean_columns = {}
            for col in df.columns:
                clean_columns[col] = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
            
            df = df.rename(columns=clean_columns)
            
            # Convert data based on SQL types
            for col in df.columns:
                sql_type = self.get_sql_type_for_column(col, df[col].dtype)
                
                # Convert critical columns to proper types
                if col.lower() == 'cu_number' or col.lower() == 'join_number':
                    # Ensure these columns are numeric for consistent joining
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except:
                        logging.warning(f"Could not convert {col} to numeric, using as-is")
                elif sql_type == "TEXT":
                    # Convert numeric columns to string if they should be text
                    if df[col].dtype == 'int64' or df[col].dtype == 'float64':
                        df[col] = df[col].astype(str)
            
            # Convert to records and handle NaN values
            records = df.to_dict('records')
            for record in records:
                for key, value in list(record.items()):
                    if pd.isna(value) or value == 'nan' or value == 'NaN' or value == 'None':
                        record[key] = None
            
            # Add retry mechanism with longer delays for errors
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    # Insert the records
                    insert_headers = self.headers.copy()
                    insert_headers["Prefer"] = "return=minimal,resolution=ignore-duplicates"
                    
                    response = requests.post(f"{self.rest_endpoint}/{table_name}",
                                           headers=insert_headers,
                                           json=records,
                                           timeout=30)  # Add timeout
                    
                    if response.status_code in [200, 201, 204]:
                        logging.info(f"Inserted {len(records)} records into {table_name}")
                        success = True
                        return True
                    else:
                        error_text = response.text if response.text else "No error message returned"
                        logging.error(f"Failed to insert data (status {response.status_code}): {error_text}")
                        
                        # Log the first record for debugging
                        if records and retry_count == max_retries - 1:
                            logging.error(f"First record: {records[0]}")
                            
                        retry_count += 1
                        if retry_count < max_retries:
                            logging.info(f"Retrying insert (attempt {retry_count + 1}/{max_retries})...")
                            time.sleep(2)
                
                except Exception as e:
                    logging.error(f"Error during insert: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        logging.info(f"Retrying insert (attempt {retry_count + 1}/{max_retries})...")
                        time.sleep(2)
            
            if not success:
                logging.error(f"Failed to insert data after {max_retries} attempts")
            return success
            
        except Exception as e:
            logging.error(f"Error inserting data: {str(e)}")
            return False
    
    def fix_problematic_tables(self, year, month):
        """Fix any tables that have issues with data types"""
        # Known problematic tables
        branch_table = f"credit_union_branch_information_{year}_{month}"
        
        # Always attempt to fix the branch table with all TEXT columns
        logging.info(f"Fixing table {branch_table} with proper column types")
        
        # Read the CSV file - use correct capitalization
        csv_files = glob.glob(os.path.join('output', 'extracted_data', '*[Bb]ranch*_sample.csv'))
        if csv_files:
            csv_file = csv_files[0]
            logging.info(f"Using branch information file: {csv_file}")
            
            df = pd.read_csv(csv_file)
            
            # Drop the existing table
            drop_sql = f"DROP TABLE IF EXISTS {branch_table};"
            self.execute_sql(drop_sql)
            
            # Create columns with correct types (all TEXT for numeric fields)
            columns = []
            for col in df.columns:
                clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col.lower())
                sql_type = "TEXT"  # Use TEXT for all columns to be safe
                columns.append(f"{clean_col} {sql_type}")
            
            # Create table SQL
            create_sql = f"""
            CREATE TABLE {branch_table} (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                {', '.join(columns)}
            );
            """
            
            # Execute SQL to create table
            result = self.execute_sql(create_sql)
            if result:
                logging.info(f"Recreated {branch_table} with all TEXT types")
                # No sample data insertion - removed
        else:
            logging.error(f"Cannot fix {branch_table} - no branch information file found in extracted_data directory")

def main():
    try:
        # Initialize the table manager
        manager = SupabaseTableManager()
        
        # Create all tables
        manager.create_all_tables()
        
        logging.info("NCUA database setup completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 