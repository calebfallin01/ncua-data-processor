import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import re
import requests

# Load environment variables
load_dotenv()

def connect_to_supabase():
    """Connect to Supabase and return client"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
        
    try:
        supabase = create_client(url, key)
        print("Successfully connected to Supabase")
        return supabase
    except Exception as e:
        print(f"Error connecting to Supabase: {str(e)}")
        raise

def get_table_list():
    """Get a hardcoded list of tables we expect to find"""
    # Use a hardcoded list since we know the table structure
    table_list = [
        'fs220a_2024_12', 'fs220b_2024_12', 'fs220c_2024_12', 
        'fs220d_2024_12', 'fs220g_2024_12', 'fs220p_2024_12',
        'fs220q_2024_12', 'fs220r_2024_12', 'fs220s_2024_12',
        'credit_union_branch_information_2024_12', 'tradenames_2024_12'
    ]
    return table_list

def get_row_counts(supabase, tables):
    """Get row counts for each table using direct REST API calls"""
    table_counts = {}
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "count=exact"
    }
    
    for table in tables:
        try:
            # Use direct REST API which doesn't require JSON parsing
            response = requests.get(
                f"{url}/rest/v1/{table}?select=id&limit=0",
                headers=headers
            )
            
            if response.status_code == 200:
                # Get the count from the content-range header
                content_range = response.headers.get('content-range')
                if content_range:
                    count = int(content_range.split('/')[-1])
                else:
                    count = 0
                table_counts[table] = count
            else:
                # Table might not exist
                table_counts[table] = 0
                
        except Exception as e:
            print(f"Error getting count for {table}: {str(e)}")
            table_counts[table] = "Error"
    
    return table_counts

def extract_period_from_table(table_name):
    """Extract period information from table name"""
    match = re.search(r'(\d{4})_(\d{2})$', table_name)
    if match:
        year, month = match.groups()
        return f"{year}-{month}"
    return "Unknown"

def get_sample_data(table, limit=5):
    """Get sample columns from a table"""
    try:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            f"{url}/rest/v1/{table}?limit={limit}",
            headers=headers
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                return data
        return []
    except Exception as e:
        print(f"Error getting sample data for {table}: {str(e)}")
        return []

def main():
    """Main function to display database summary"""
    try:
        # Connect to Supabase (just to test connection)
        supabase = connect_to_supabase()
        
        # Get hardcoded table list 
        tables = get_table_list()
        if not tables:
            print("No tables in list")
            return
            
        print(f"\nChecking {len(tables)} tables in the database")
        
        # Get row counts
        table_counts = get_row_counts(supabase, tables)
        
        # Group by period
        period_groups = {}
        for table in tables:
            period = extract_period_from_table(table)
            if period not in period_groups:
                period_groups[period] = []
            period_groups[period].append(table)
        
        # Print summary by period
        for period, period_tables in sorted(period_groups.items()):
            print(f"\n=== Period: {period} ===")
            print(f"Found {len(period_tables)} tables")
            
            # Sort tables by count
            period_tables.sort(key=lambda t: table_counts.get(t, 0) if isinstance(table_counts.get(t, 0), int) else -1, reverse=True)
            
            # Print table details
            for table in period_tables:
                count = table_counts.get(table, "Unknown")
                count_display = f"{count:,}" if isinstance(count, int) else count
                print(f"  {table}: {count_display} rows")
                
                # For tables with data, show a sample of column names
                if isinstance(count, int) and count > 0:
                    sample = get_sample_data(table, limit=1)
                    if sample and len(sample) > 0:
                        # Get up to 5 column names to display
                        columns = list(sample[0].keys())[:5]
                        print(f"    Sample columns: {', '.join(columns)}...")
        
        print("\nDatabase summary complete")
        
    except Exception as e:
        print(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main() 