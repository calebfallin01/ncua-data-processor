import os
import time
import signal
import sys
import logging
from pathlib import Path
from processor import DataProcessor

# Disable verbose logging from HTTP libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Simplified format
    handlers=[
        logging.FileHandler('logs/main.log'),
        logging.StreamHandler()
    ]
)

def handle_interrupt(signum, frame):
    """Handle keyboard interrupt gracefully"""
    print("\n\nInterrupted by user. Cleaning up...")
    if 'processor' in globals():
        processor.close()
    print("Exiting gracefully.")
    sys.exit(0)

def main():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    # Create output directory if it doesn't exist
    if not os.path.exists('output'):
        os.makedirs('output')
        
    # Register signal handler for clean exit
    signal.signal(signal.SIGINT, handle_interrupt)
    
    # Initialize processor
    global processor  # Make it accessible to the signal handler
    processor = DataProcessor('input', 'output')
    print("Starting file processing application")
    
    try:
        # Main processing loop
        while True:
            # Check for ZIP files first
            zip_files = [f for f in os.listdir('input') 
                       if f.endswith('.zip') and not f.startswith('.')]
            
            if zip_files:
                # Sort by most recent first (based on filename, typically will include date)
                zip_files.sort(reverse=True)
                
                # Process the newest ZIP file
                newest_zip = zip_files[0]
                print(f"\n=== Processing new ZIP file: {newest_zip} ===")
                processor.process_specific_zip(newest_zip)
                
                # Wait between files to avoid overloading the database
                time.sleep(5)
            else:
                print("\nNo new ZIP files to process, waiting...")
                time.sleep(30)  # Wait 30 seconds before checking again
                
    except Exception as e:
        print(f"Error in main process: {str(e)}")
    finally:
        # Clean up
        processor.close()
        print("Application terminated.")

if __name__ == "__main__":
    main() 