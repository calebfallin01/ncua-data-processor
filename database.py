import os
from supabase import create_client, Client
import pandas as pd
import logging
from dotenv import load_dotenv
import time
import json
import requests

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        """Initialize Supabase connection"""
        print("Initializing Supabase connection...")
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
            
        try:
            self.supabase: Client = create_client(self.url, self.key)
            print("Successfully connected to Supabase")
        except Exception as e:
            print(f"Error connecting to Supabase: {str(e)}")
            raise

    def create_tables(self, table_name: str, schema: str):
        """Create tables in Supabase if they don't exist"""
        try:
            # Clean table name
            table_name = table_name.strip().lower().replace('-', '_')
            logging.debug(f"Attempting to create table: {table_name}")
            
            # First try to check if the table exists using a direct REST API call
            # This avoids JSON parsing issues
            try:
                url = self.url
                key = self.key
                headers = {
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "Prefer": "count=exact"
                }
                
                # Use a simple HEAD request to check if table exists
                check_response = requests.head(
                    f"{url}/rest/v1/{table_name}?limit=0",
                    headers=headers
                )
                
                # If table exists (200 response) we're done
                if check_response.status_code == 200:
                    logging.info(f"Table {table_name} already exists")
                    return
                    
            except Exception as check_error:
                logging.debug(f"Error checking table existence via REST: {str(check_error)}")
                # Fall back to standard approach if direct check fails
                pass
                
            # If we get here, either the table doesn't exist or we couldn't check
            # Try to query the table using the Supabase client to see if it exists
            try:
                self.supabase.table(table_name).select("*").limit(1).execute()
                logging.info(f"Table {table_name} already exists")
                return
            except Exception as e:
                # If table doesn't exist (based on error message), create it
                if 'relation "' in str(e) and '" does not exist' in str(e) or '42P01' in str(e):
                    try:
                        headers = {
                            "apikey": self.key,
                            "Authorization": f"Bearer {self.key}",
                            "Content-Type": "application/json",
                            "Prefer": "return=minimal"
                        }
                        
                        # Use the execute_sql_with_minimal_response function to avoid JSON response issues
                        logging.debug(f"Creating table with schema: {schema}")
                        response = requests.post(
                            f"{self.url}/rest/v1/rpc/execute_sql_with_minimal_response",
                            headers=headers,
                            json={"sql_query": schema},
                            timeout=30  # Add timeout to prevent hanging
                        )
                        
                        # For 204 No Content responses, this is actually success
                        if response.status_code == 204:
                            logging.info(f"Created table {table_name} successfully (status: 204)")
                            return
                            
                        if response.status_code >= 400:
                            logging.error(f"Failed to create table {table_name}: Status {response.status_code}, Response {response.text}")
                            # Try a simpler approach with just creating the table without all columns
                            simple_schema = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY);"
                            
                            logging.debug(f"Trying simpler table creation: {simple_schema}")
                            response = requests.post(
                                f"{self.url}/rest/v1/rpc/execute_sql_with_minimal_response",
                                headers=headers,
                                json={"sql_query": simple_schema},
                                timeout=30
                            )
                            
                            if response.status_code >= 400:
                                logging.error(f"Failed to create simple table: {response.text}")
                                raise Exception(f"Failed to create table {table_name}: {response.text}")
                            
                            # Table was created with just an ID, now add columns one by one
                            if "(" in schema and ")" in schema:
                                cols_str = schema.split("(")[1].split(")")[0]
                                # Remove primary key part which we already added
                                cols_str = cols_str.replace("id SERIAL PRIMARY KEY,", "")
                                cols_str = cols_str.replace("id SERIAL PRIMARY KEY", "")
                                
                                # Add columns one by one
                                success_count = 0
                                error_count = 0
                                for col_def in [c.strip() for c in cols_str.split(",") if c.strip()]:
                                    if col_def and len(col_def.split()) >= 2:
                                        col_name = col_def.split()[0].strip()
                                        col_type = " ".join(col_def.split()[1:]).strip()
                                        
                                        if col_name.lower() == "id":
                                            continue  # Skip id column
                                            
                                        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type};"
                                        try:
                                            alter_response = requests.post(
                                                f"{self.url}/rest/v1/rpc/execute_sql_with_minimal_response",
                                                headers=headers,
                                                json={"sql_query": alter_sql},
                                                timeout=20
                                            )
                                            
                                            if alter_response.status_code < 400:
                                                success_count += 1
                                            else:
                                                logging.warning(f"Failed to add column {col_name}: {alter_response.text}")
                                                error_count += 1
                                        except Exception as col_err:
                                            logging.warning(f"Error adding column {col_name}: {str(col_err)}")
                                            error_count += 1
                                
                                logging.info(f"Added {success_count} columns to table {table_name}, {error_count} columns failed")
                                if success_count > 0:
                                    # Even if some columns failed, consider the table creation a success
                                    return
                        else:
                            logging.info(f"Created table {table_name} in one step")
                            return
                            
                    except requests.exceptions.Timeout as timeout_error:
                        logging.error(f"Timeout creating table {table_name}: {str(timeout_error)}")
                        raise Exception(f"Timeout creating table {table_name}: {str(timeout_error)}")
                    except Exception as create_error:
                        logging.error(f"Error creating table {table_name}: {str(create_error)}")
                        raise create_error
                else:
                    logging.error(f"Error checking table {table_name}: {str(e)}")
                    raise
                
        except Exception as e:
            logging.error(f"Error in create_tables: {str(e)}")
            # Don't raise the exception - just log it and continue
            # This way one table failure won't stop the entire process
            return False

    def insert_data(self, table_name: str, data: pd.DataFrame):
        """Insert data into Supabase table"""
        try:
            # Clean table name
            table_name = table_name.strip().lower().replace('-', '_')
            logging.debug(f"Attempting to insert data into table: {table_name}")
            
            # Convert DataFrame to records
            records = data.to_dict('records')
            total_records = len(records)
            logging.debug(f"Total records to insert: {total_records}")
            
            if not records:
                logging.warning(f"No data to insert into table {table_name}")
                return
                
            # Clean column names in records and handle NaN/None values
            cleaned_records = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
                    # Handle NaN/None values
                    if pd.isna(value):
                        cleaned_record[clean_key] = None
                    else:
                        # Convert to string if it's a complex type
                        if isinstance(value, (dict, list)):
                            cleaned_record[clean_key] = str(value)
                        else:
                            cleaned_record[clean_key] = value
                cleaned_records.append(cleaned_record)
            
            # For branch information tables, ensure all text fields are cleaned properly
            if 'branch_information' in table_name or 'branch' in table_name:
                for record in cleaned_records:
                    for key, value in record.items():
                        if isinstance(value, str):
                            # Clean special characters that might cause JSON issues
                            record[key] = value.replace('\u0000', '').replace('\x00', '')
            
            # Insert data in smaller batches
            batch_size = 50  # Reduced batch size even further
            for i in range(0, len(cleaned_records), batch_size):
                batch = cleaned_records[i:i + batch_size]
                start_row = i + 1
                end_row = min(i + batch_size, total_records)
                
                retry_count = 0
                while retry_count < 3:  # Add retries
                    try:
                        headers = {
                            "apikey": self.key,
                            "Authorization": f"Bearer {self.key}",
                            "Content-Type": "application/json",
                            "Prefer": "return=minimal"  # Use minimal response to avoid JSON parsing issues
                        }
                        
                        # Use REST API directly to avoid JSON parsing issues
                        response = requests.post(
                            f"{self.url}/rest/v1/{table_name}",
                            headers=headers,
                            json=batch,
                            timeout=30  # Add timeout to prevent hanging
                        )
                        
                        # A 204 No Content response is actually success for minimal response
                        if response.status_code == 204:
                            print(f"{table_name}: Inserting rows {start_row:,} to {end_row:,} out of {total_records:,}")
                            print("Success (204 No Content)")
                            break
                            
                        if response.status_code >= 400:
                            raise Exception(f"API Error: {response.status_code} - {response.text}")
                            
                        print(f"{table_name}: Inserting rows {start_row:,} to {end_row:,} out of {total_records:,}")
                        print("Success")
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == 3:
                            print(f"{table_name}: Failed inserting rows {start_row:,} to {end_row:,} out of {total_records:,}")
                            print(f"Error: {str(e)}")
                            logging.error(f"Failed to insert batch after 3 retries: {str(e)}")
                            # Don't raise - continue with the next batch
                            break
                        logging.warning(f"Retry {retry_count} after error: {str(e)}")
                        time.sleep(2)  # Wait longer before retrying
                    
                time.sleep(1)  # Increased delay between batches
                
            return True
                
        except Exception as e:
            logging.error(f"Error in insert_data: {str(e)}")
            # Don't raise the exception - just log it
            return False

    def close(self):
        """Close database connection"""
        try:
            # Supabase client doesn't need explicit closing
            logging.info("Database connection closed")
        except Exception as e:
            logging.error(f"Error closing database connection: {str(e)}")
            raise 