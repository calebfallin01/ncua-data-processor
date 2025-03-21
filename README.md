# NCUA Data Processor

This application automatically processes ZIP files containing data and imports them into a Supabase database.

## Setup

1. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your Supabase project:
   - Go to https://supabase.com and create a new project
   - Once created, go to the project settings
   - Note down your project URL and anon/public key
   - Create the necessary tables in your Supabase dashboard based on your data structure

## Usage

1. Start the application:
```bash
python main.py
```

2. Place ZIP files containing data files (CSV, JSON, or Excel) in the `input` directory.

3. The application will automatically:
   - Detect new ZIP files
   - Extract their contents
   - Process the data files
   - Import the data into your Supabase database

## Supported File Formats

- CSV files (.csv)
- JSON files (.json)
- Excel files (.xlsx, .xls)

## Directory Structure

- `input/`: Place ZIP files here for processing
- `temp_extract/`: Temporary directory for ZIP extraction (created automatically)
- `main.py`: Main application script
- `database.py`: Database management module
- `processor.py`: Data processing module
- `requirements.txt`: Project dependencies

## Notes

- Table creation is handled through the Supabase dashboard
- Data is inserted in batches of 1000 records to avoid rate limits
- The application supports nested JSON structures
- Make sure your Supabase tables match the structure of your data files 