import os
import zipfile
import pandas as pd
import logging
import tempfile
import shutil
import json
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extract_analyze.log'),
        logging.StreamHandler()
    ]
)

def extract_and_analyze_zip(zip_file_path, output_dir):
    """Extract and analyze the structure of files in a zip"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Get the zip filename without extension
    zip_filename = os.path.basename(zip_file_path)
    zip_name_without_ext = os.path.splitext(zip_filename)[0]
    
    # Create a specific folder for this zip file in the output directory
    zip_output_dir = os.path.join(output_dir, zip_name_without_ext)
    if not os.path.exists(zip_output_dir):
        os.makedirs(zip_output_dir)
        logging.info(f"Created output directory for zip file: {zip_output_dir}")
    
    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        logging.info(f"Extracting to temporary directory: {temp_dir}")
        
        # Extract the ZIP file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        logging.info(f"Contents of extracted directory: {os.listdir(temp_dir)}")
        
        # Process each file
        file_structures = {}
        
        for file_name in os.listdir(temp_dir):
            if file_name.endswith('.txt'):
                file_path = os.path.join(temp_dir, file_name)
                logging.info(f"Analyzing file: {file_name}")
                
                # Try to read the file with different encodings and separators
                df = read_data_file(file_path)
                
                if df is not None and not df.empty:
                    # Save structure information
                    structure = {
                        'file_name': file_name,
                        'rows': len(df),
                        'columns': len(df.columns),
                        'column_names': list(df.columns),
                        'column_types': {col: str(df[col].dtype) for col in df.columns},
                        'sample_data': df.head(5).to_dict('records')
                    }
                    
                    file_structures[file_name] = structure
                    
                    # Extract the file to the zip-specific output directory
                    output_file = os.path.join(zip_output_dir, file_name)
                    shutil.copy(file_path, output_file)
                    logging.info(f"Copied {file_name} to {zip_output_dir}")
                    
                    # Save a sample CSV to the zip-specific output directory
                    csv_output = os.path.join(zip_output_dir, f"{os.path.splitext(file_name)[0]}_sample.csv")
                    df.head(100).to_csv(csv_output, index=False)
                    logging.info(f"Saved sample data to {csv_output}")
                    
                    # Also copy sample files to the main extracted_data directory for backward compatibility
                    main_csv_output = os.path.join(output_dir, f"{os.path.splitext(file_name)[0]}_sample.csv")
                    shutil.copy(csv_output, main_csv_output)
                    logging.info(f"Copied sample data to {main_csv_output} for backward compatibility")
        
        # Save structure information to JSON in the zip-specific output directory
        structure_file = os.path.join(zip_output_dir, 'file_structures.json')
        with open(structure_file, 'w') as f:
            json.dump(file_structures, f, indent=2)
        
        logging.info(f"Saved file structure information to {structure_file}")
        
        # Also save to main output directory for backward compatibility
        main_structure_file = os.path.join(output_dir, 'file_structures.json')
        shutil.copy(structure_file, main_structure_file)
        logging.info(f"Copied structure file to {main_structure_file} for backward compatibility")

def read_data_file(file_path):
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

def main():
    try:
        # Find all zip files in the input directory
        input_dir = 'input'
        output_base_dir = os.path.join('output', 'extracted_data')
        
        zip_files = [f for f in os.listdir(input_dir) if f.endswith('.zip') and not f.startswith('.')]
        
        if not zip_files:
            logging.error("No zip files found in input directory")
            return
        
        # Process the first zip file (assuming we process one at a time)
        zip_file = os.path.join(input_dir, zip_files[0])
        logging.info(f"Processing zip file: {zip_file}")
        
        extract_and_analyze_zip(zip_file, output_base_dir)
        logging.info("Successfully extracted and analyzed NCUA data")
    
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main() 