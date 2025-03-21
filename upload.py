import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
from processor import DataProcessor
from database import DatabaseManager  # Import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/upload.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

def create_storage_bucket(supabase, bucket_name):
    """Create a storage bucket if it doesn't exist"""
    try:
        # Check if bucket exists by listing files
        supabase.storage.from_(bucket_name).list()
        logging.info(f"Bucket '{bucket_name}' already exists")
    except Exception as e:
        if "The resource was not found" in str(e):
            try:
                # Create bucket
                supabase.storage.create_bucket(bucket_name, {'public': True})
                logging.info(f"Created bucket '{bucket_name}'")
            except Exception as create_error:
                logging.error(f"Error creating bucket: {str(create_error)}")
                raise
        else:
            logging.error(f"Error checking bucket: {str(e)}")
            raise

def upload_zip_to_storage(supabase, bucket_name, file_path):
    """Upload a zip file to Supabase Storage"""
    file_name = os.path.basename(file_path)
    
    try:
        # Upload file
        with open(file_path, "rb") as f:
            result = supabase.storage.from_(bucket_name).upload(file_name, f)
        
        # Get public URL
        public_url = supabase.storage.from_(bucket_name).get_public_url(file_name)
        logging.info(f"Uploaded file to storage: {public_url}")
        
        # Save file metadata in the database
        result = supabase.table("files").insert({
            "name": file_name, 
            "url": public_url
        }).execute()
        
        logging.info(f"Saved file metadata to database: {file_name}")
        return public_url
    
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        raise

def create_files_table(db_manager):
    """Create files table for storing file metadata using DatabaseManager"""
    try:
        # Create files table schema
        schema = """
        CREATE TABLE IF NOT EXISTS files (
            id uuid primary key default gen_random_uuid(),
            name text not null,
            url text not null,
            uploaded_at timestamp default now()
        );
        """
        
        # Use DatabaseManager to create table
        db_manager.create_tables("files", schema)
        logging.info("Created files table")
    except Exception as e:
        logging.error(f"Error creating files table: {str(e)}")
        raise

def main():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Initialize Supabase client
    try:
        url = os.getenv("SUPABASE_URL")
        service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not url or not service_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env file")
            
        supabase: Client = create_client(url, service_key)
        logging.info("Successfully connected to Supabase")
        
        # Create storage bucket and files table
        bucket_name = "ncua_data"
        create_storage_bucket(supabase, bucket_name)
        create_files_table(db_manager)
        
        # Upload zip file to storage
        zip_file = os.path.join('input', 'call-report-data-2024-03.zip')
        if os.path.exists(zip_file):
            upload_zip_to_storage(supabase, bucket_name, zip_file)
            
            # Process zip file data
            processor = DataProcessor('input', 'output')
            processor.process_files()
            processor.close()
            
            logging.info("Successfully processed NCUA data")
        else:
            logging.error(f"Zip file not found: {zip_file}")
            
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise
    finally:
        # Close the database connection
        db_manager.close()

if __name__ == "__main__":
    main() 