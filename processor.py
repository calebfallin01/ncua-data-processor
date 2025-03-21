import os
import pandas as pd
import logging
from datetime import datetime
from database import DatabaseManager
import zipfile
import shutil
import time
import re
import traceback
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Simplified format
    handlers=[
        logging.FileHandler('logs/processor.log'),
        logging.StreamHandler()
    ]
)

# Disable verbose logging from HTTP libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

class DataProcessor:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.db = DatabaseManager()
        self.processed_files = set()
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        print(f"Processor initialized: {input_dir} → {output_dir}")

    def _should_process_file(self, filename):
        """Check if a file should be processed"""
        # Ignore hidden files and system files
        if filename.startswith('.') or filename.startswith('~'):
            return False
        # Only process .zip and .txt files
        return filename.endswith('.zip') or filename.endswith('.txt')

    def _extract_date_info(self, filename):
        """Extract year and month information from filename"""
        # Example filename: call-report-data-2024-03.zip
        match = re.search(r'(\d{4})-(\d{2})', filename)
        if match:
            year = match.group(1)
            month = match.group(2)  # Keep as string to preserve leading zero
            return year, month
        return None, None

    def _get_table_name(self, base_name, year, month):
        """Generate a table name that includes month information"""
        # Clean the base name and add month info
        clean_base = base_name.strip().lower().replace('-', '_').replace(' ', '_')
        return f"{clean_base}_{year}_{month}"

    def process_files(self):
        """Process all files in the input directory"""
        print("Starting file processing")
        
        # Check for ZIP files
        for file in os.listdir(self.input_dir):
            if not self._should_process_file(file):
                continue
                
            if file.endswith('.zip'):
                print(f"Found ZIP file: {file}")
                self._process_zip(file)
                return  # Process one ZIP file at a time
                
        # Process individual files
        for file in os.listdir(self.input_dir):
            if not self._should_process_file(file):
                continue
                
            if file in self.processed_files:
                continue
                
            file_path = os.path.join(self.input_dir, file)
            if os.path.isfile(file_path):
                print(f"Processing file: {file}")
                self._process_file(file)
                self.processed_files.add(file)

    def process_specific_zip(self, zip_filename):
        """Process a specific ZIP file by name"""
        zip_path = os.path.join(self.input_dir, zip_filename)
        if os.path.isfile(zip_path) and zip_filename.endswith('.zip'):
            self._process_zip(zip_filename)
        else:
            print(f"Error: ZIP file '{zip_filename}' not found or not a ZIP file")

    def _process_zip(self, zip_file):
        """Process a ZIP file containing multiple data files"""
        zip_path = os.path.join(self.input_dir, zip_file)
        print(f"\n==== Processing: {zip_file} ====")
        
        # Extract date information
        year, month = self._extract_date_info(zip_file)
        if not year or not month:
            logging.error(f"Could not extract date information from filename: {zip_file}")
            return
            
        print(f"Period: {year}-{month}")
        
        # Create a temporary directory for extraction
        temp_dir = os.path.join(self.input_dir, 'temp_extract')
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        
        success_count = 0
        error_count = 0
        
        try:
            # Extract the ZIP file
            print("Extracting files...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Process each extracted file
            files_to_process = [f for f in os.listdir(temp_dir) if self._should_process_file(f)]
            print(f"Found {len(files_to_process)} files to process")
            
            for i, file in enumerate(files_to_process):
                if file.endswith('.txt'):
                    print(f"\n---- [{i+1}/{len(files_to_process)}] Processing: {file} ----")
                    try:
                        file_path = os.path.join(temp_dir, file)
                        self._process_file(file, year, month)
                        self.processed_files.add(file)
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        error_details = traceback.format_exc()
                        logging.error(f"Error processing file {file}: {str(e)}")
                        logging.error(f"Error details: {error_details}")
                        print(f"  → Skipping file due to error: {str(e)}")
                        # Continue with next file instead of raising
                        continue
            
            # Move processed ZIP to output directory only if at least one file was processed
            if success_count > 0:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_zip = os.path.join(self.output_dir, f"{os.path.splitext(zip_file)[0]}_{timestamp}.zip")
                shutil.move(zip_path, output_zip)
                print(f"Completed processing ZIP file: {zip_file} - {success_count} files processed successfully, {error_count} files with errors")
            else:
                print(f"ZIP file processing failed: {zip_file} - All {error_count} files had errors")
            
        except Exception as e:
            error_details = traceback.format_exc()
            logging.error(f"Fatal error processing ZIP file: {str(e)}")
            logging.error(f"Error details: {error_details}")
            print(f"  → Fatal error in ZIP extraction or processing: {str(e)}")
            return
        finally:
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def _process_file(self, file, year=None, month=None):
        """Process a single file"""
        try:
            # Use the correct file path based on whether we're processing from temp directory or input directory
            if os.path.exists(os.path.join(self.input_dir, 'temp_extract', file)):
                file_path = os.path.join(self.input_dir, 'temp_extract', file)
            else:
                file_path = os.path.join(self.input_dir, file)
                
            # Special handling for known problematic files
            is_branch_info = 'branch' in file.lower()
            is_fs220l = 'fs220l' in file.lower()
            
            # Try to read as CSV first with appropriate settings for problematic files
            try:
                # For FS220L, use dtype=object to avoid integer overflow
                if is_fs220l:
                    df = pd.read_csv(file_path, encoding='utf-8', low_memory=False, dtype='object')
                else:
                    df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
                print(f"  → Read {len(df):,} rows")
                print(f"  → Columns: {len(df.columns)}")
            except UnicodeDecodeError:
                try:
                    if is_fs220l:
                        df = pd.read_csv(file_path, encoding='latin1', low_memory=False, dtype='object')
                    else:
                        df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
                    print(f"  → Read {len(df):,} rows (latin1 encoding)")
                except:
                    logging.info("Failed to read as CSV, trying as TSV")
                    try:
                        if is_fs220l:
                            df = pd.read_csv(file_path, sep='\t', encoding='utf-8', low_memory=False, dtype='object')
                        else:
                            df = pd.read_csv(file_path, sep='\t', encoding='utf-8', low_memory=False)
                        print(f"  → Read {len(df):,} rows (TSV format)")
                    except:
                        try:
                            if is_fs220l:
                                df = pd.read_csv(file_path, sep='\t', encoding='latin1', low_memory=False, dtype='object')
                            else:
                                df = pd.read_csv(file_path, sep='\t', encoding='latin1', low_memory=False)
                            print(f"  → Read {len(df):,} rows (TSV format, latin1 encoding)")
                        except:
                            logging.info("Failed to read as TSV, trying as plain text")
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                                logging.info(f"File content preview: {content[:200]}...")
                                raise Exception("Could not read file as structured data")

            # Make sure the DataFrame has data
            if len(df) == 0:
                raise Exception("File contains no data rows")
                
            if len(df.columns) == 0:
                raise Exception("File contains no columns")
                
            # Clean column names to avoid SQL issues
            df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]
                
            # Generate schema and create table
            base_name = os.path.splitext(file)[0]
            if year and month:
                table_name = self._get_table_name(base_name, year, month)
            else:
                table_name = base_name.lower().replace('-', '_')
            
            # Clean all object type columns - replace NaN with None and handle string encoding
            for col in df.select_dtypes(include=['object']).columns:
                # Special handling for Branch Information to clean problematic characters
                if is_branch_info:
                    df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x).strip().replace('\x00', ''))
                else:
                    df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x).strip())
                
            # For FS220L, convert numeric columns to strings first to avoid overflow
            if is_fs220l:
                for col in df.select_dtypes(include=['int64', 'float64']).columns:
                    df[col] = df[col].astype(str)
                    
            # Generate the schema
            try:
                schema = self._generate_schema(df, table_name)
                logging.debug(f"Generated schema for table {table_name}: {schema}")
            except Exception as schema_error:
                logging.error(f"Error generating schema: {str(schema_error)}")
                error_details = traceback.format_exc()
                logging.error(f"Schema error details: {error_details}")
                
                # Create a simplified schema with everything as TEXT
                logging.info("Attempting to create a simplified schema with all columns as TEXT")
                simple_schema = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY"
                for col in df.columns:
                    clean_col = col.strip().lower().replace(' ', '_').replace('-', '_')
                    simple_schema += f", {clean_col} TEXT"
                simple_schema += ");"
                
                schema = simple_schema
                logging.debug(f"Using simplified schema: {schema}")
            
            # Create table in database
            table_created = self.db.create_tables(table_name, schema)
            if table_created is False:
                print(f"  → Warning: Table creation for {table_name} had issues but will attempt to continue")
            else:
                print(f"  → Created table: {table_name}")
            
            # Check if data already exists in the table
            try:
                # Use a direct REST approach to count records to avoid JSON parsing issues
                # First try with REST API which is more robust
                url = self.db.url
                key = self.db.key
                headers = {
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Prefer": "count=exact"
                }
                
                try:
                    response = requests.get(
                        f"{url}/rest/v1/{table_name}?select=id&limit=0",
                        headers=headers
                    )
                    
                    record_count = 0
                    if response.status_code == 200:
                        content_range = response.headers.get('content-range')
                        if content_range:
                            record_count = int(content_range.split('/')[-1])
                except Exception as e:
                    logging.warning(f"Error checking count with REST API: {str(e)}")
                    # Fall back to Supabase client with more cautious approach
                    try:
                        count_result = self.db.supabase.table(table_name).select("count", count="exact").execute()
                        record_count = count_result.count if hasattr(count_result, 'count') else 0
                    except Exception as count_error:
                        logging.warning(f"Error getting count from Supabase client: {str(count_error)}")
                        record_count = 0  # Assume 0 records if we can't check
                
                if record_count > 0:
                    print(f"  → Table already has {record_count:,} records, skipping insertion")
                else:
                    # Insert data
                    insert_success = self.db.insert_data(table_name, df)
                    if insert_success:
                        print(f"  → Data insertion complete ({len(df):,} rows)")
                    else:
                        print(f"  → Warning: Data insertion had some issues but completed partially")
                        
            except Exception as insert_error:
                logging.error(f"Error checking/inserting data into {table_name}: {str(insert_error)}")
                error_details = traceback.format_exc()
                logging.error(f"Insert error details: {error_details}")
                raise insert_error
            
            # Move processed file to output directory
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(self.output_dir, f"{os.path.splitext(file)[0]}_{timestamp}{os.path.splitext(file)[1]}")
            shutil.move(file_path, output_file)
            
        except Exception as e:
            logging.error(f"Error processing file {file}: {str(e)}")
            error_details = traceback.format_exc()
            logging.error(f"Processing error details: {error_details}")
            raise

    def _generate_schema(self, df, table_name):
        """Generate SQL schema for the table"""
        columns = []
        
        # Special handling for specific tables with known large numbers
        is_fs220l = 'fs220l' in table_name.lower()
        # Add other tables known to have large numbers
        tables_with_large_numbers = ['fs220l', 'fs220d', 'fs220e', 'fs220f']
        has_large_numbers = any(t in table_name.lower() for t in tables_with_large_numbers)
        
        # Debug column types
        logging.debug(f"Table {table_name} column types: {df.dtypes}")
        
        # First pass: check if any column names are duplicated
        column_counts = {}
        for col in df.columns:
            clean_col = col.strip().lower().replace(' ', '_').replace('-', '_')
            column_counts[clean_col] = column_counts.get(clean_col, 0) + 1
            
        # Get duplicate columns
        duplicates = {col: count for col, count in column_counts.items() if count > 1}
        if duplicates:
            logging.warning(f"Found duplicate column names in {table_name}: {duplicates}")
        
        column_index = {}  # Track columns we've already processed
        
        for col in df.columns:
            # Clean column name
            clean_col = col.strip().lower().replace(' ', '_').replace('-', '_')
            
            # Handle duplicate column names by adding a suffix
            if clean_col in column_index:
                column_index[clean_col] += 1
                clean_col = f"{clean_col}_{column_index[clean_col]}"
            else:
                column_index[clean_col] = 0
            
            # Determine data type
            dtype = str(df[col].dtype)
            
            # Special handling for tables which have values exceeding integer range
            if has_large_numbers:
                # Use NUMERIC for all numeric columns in these tables to avoid integer overflow
                if 'int' in dtype or 'float' in dtype:
                    sql_type = 'NUMERIC'
                elif 'datetime' in dtype:
                    sql_type = 'TIMESTAMP'
                else:
                    sql_type = 'TEXT'
            else:
                # Normal type inference for other tables
                if 'int' in dtype:
                    # Always check for large values in integer columns
                    try:
                        # Get max and min, handling NaN values safely
                        non_null_values = df[col].dropna()
                        if len(non_null_values) > 0:
                            max_val = non_null_values.max()
                            min_val = non_null_values.min()
                            
                            # Even with non-null values, pandas might return NaN
                            if pd.isna(max_val) or pd.isna(min_val):
                                sql_type = 'INTEGER'
                            # If values exceed int4 range, use BIGINT
                            elif max_val > 2147483647 or min_val < -2147483648:
                                # Using NUMERIC instead of BIGINT for maximum compatibility
                                sql_type = 'NUMERIC'
                                logging.info(f"Column {col} in {table_name} has large values: min={min_val}, max={max_val}, using NUMERIC type")
                            else:
                                sql_type = 'INTEGER'
                        else:
                            sql_type = 'INTEGER'  # Default if all values are null
                    except Exception as e:
                        # If any error occurs during value checking, use NUMERIC to be safe
                        logging.warning(f"Error checking values in column {col}: {str(e)}. Using NUMERIC type to be safe.")
                        sql_type = 'NUMERIC'
                elif 'float' in dtype:
                    sql_type = 'NUMERIC'
                elif 'datetime' in dtype:
                    sql_type = 'TIMESTAMP'
                else:
                    # For text columns, check for very large values
                    try:
                        max_len = df[col].astype(str).map(len).max()
                        if max_len > 255:
                            sql_type = 'TEXT'
                        else:
                            sql_type = 'TEXT'  # Always use TEXT for simplicity
                    except:
                        sql_type = 'TEXT'  # Default to TEXT if check fails
                
            columns.append(f"{clean_col} {sql_type}")
            
        # Create a clean, single-line SQL statement
        schema = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {', '.join(columns)});"
        logging.debug(f"Generated schema: {schema}")
        return schema

    def close(self):
        """Close database connection"""
        self.db.close() 