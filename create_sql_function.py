import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_execute_sql_function():
    """Create or replace the execute_sql RPC function in Supabase"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
    
    # SQL to create the RPC function that returns minimal responses
    sql_function = """
    CREATE OR REPLACE FUNCTION execute_sql_with_minimal_response(sql_query text)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    AS $$
    BEGIN
      EXECUTE sql_query;
    END;
    $$;
    """
    
    # Headers for the Supabase REST API
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    # Make the REST API request to create the function
    response = requests.post(
        f"{url}/rest/v1/rpc/execute_sql",
        headers=headers,
        json={"sql_query": sql_function}
    )
    
    # Check if the request was successful
    if response.status_code >= 400:
        print(f"Error: {response.status_code}, {response.text}")
        return False
    else:
        print("Successfully created execute_sql_with_minimal_response function")
        return True

if __name__ == "__main__":
    create_execute_sql_function() 