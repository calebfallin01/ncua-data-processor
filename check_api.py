import os
import requests
import logging
from dotenv import load_dotenv

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

def check_api_access():
    """Check if we have basic API access to Supabase"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        logging.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
        return False
        
    # Set up headers for API requests
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    
    # Try to access the health endpoint
    health_url = f"{url}/rest/v1/"
    
    try:
        response = requests.get(health_url, headers=headers)
        logging.info(f"Status code: {response.status_code}")
        logging.info(f"Response: {response.text}")
        
        if response.status_code == 200:
            logging.info("Successfully connected to Supabase API")
            return True
        else:
            logging.error(f"Failed to connect to Supabase API: {response.text}")
            return False
    
    except Exception as e:
        logging.error(f"Error connecting to Supabase API: {str(e)}")
        return False

def list_buckets():
    """List storage buckets"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    # Set up headers for API requests
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    
    # Try to list storage buckets
    buckets_url = f"{url}/storage/v1/bucket"
    
    try:
        response = requests.get(buckets_url, headers=headers)
        logging.info(f"Buckets status code: {response.status_code}")
        logging.info(f"Buckets response: {response.text}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error listing buckets: {str(e)}")
        return False

def check_create_table():
    """Check if we can create a table"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    # Set up headers for API requests
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }
    
    # Try to create a test table via direct SQL - using the correct parameter name "sql_query"
    data = {
        "sql_query": "CREATE TABLE IF NOT EXISTS test_table (id serial primary key, name text);"
    }
    
    try:
        response = requests.post(f"{url}/rest/v1/rpc/execute_sql", headers=headers, json=data)
        logging.info(f"SQL status code: {response.status_code}")
        logging.info(f"SQL response: {response.text}")
        
        # Try to create a test table via create_table RPC
        create_table_data = {
            "table_name": "test_table2",
            "schema_sql": "CREATE TABLE IF NOT EXISTS test_table2 (id serial primary key, name text);"
        }
        
        response = requests.post(f"{url}/rest/v1/rpc/create_table", headers=headers, json=create_table_data)
        logging.info(f"Create table status code: {response.status_code}")
        logging.info(f"Create table response: {response.text}")
        
        # Try using direct insert to create a table
        test_data = {"name": "test_value"}
        response = requests.post(f"{url}/rest/v1/test_table", headers=headers, json=test_data)
        logging.info(f"REST insert status code: {response.status_code}")
        logging.info(f"REST insert response: {response.text}")
        
        # Try to select from the table to check if it exists
        select_headers = headers.copy()
        select_headers["Accept"] = "application/json"
        response = requests.get(f"{url}/rest/v1/test_table?limit=1", headers=select_headers)
        logging.info(f"REST select status code: {response.status_code}")
        logging.info(f"REST select response: {response.text}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        return False

def main():
    """Main function to run API checks"""
    logging.info("Checking Supabase API access...")
    check_api_access()
    
    logging.info("\nChecking storage buckets...")
    list_buckets()
    
    logging.info("\nChecking table creation...")
    check_create_table()

if __name__ == "__main__":
    main() 