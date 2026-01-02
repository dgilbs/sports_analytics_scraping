# Database Connection Setup

## Overview
This project now uses environment variables to securely store database credentials, making it safe to push to GitHub.

## Setup Instructions

### 1. Environment Variables
Create a `.env` file in the project root (already created, but here's the template):

```env
DATABASE_URL=your_postgresql_connection_string_here
```

**Note:** The `.env` file is already in `.gitignore` and will NOT be committed to Git.

### 2. Install Dependencies
```bash
source virtual_env/bin/activate
pip install -r requirements.txt
```

### 3. Required Dependencies
- `python-dotenv` - Load environment variables from .env file
- `psycopg2-binary` - PostgreSQL database adapter
- `sqlalchemy` - SQL toolkit and ORM
- `pandas` - Data manipulation
- `requests` - HTTP library for web scraping
- `numpy` - Numerical computing
- `matplotlib` - Plotting library
- `scikit-learn` - Machine learning library

## Updated Files

### 1. `query_db.py`
**Changes:**
- ✅ Removed hardcoded database credentials
- ✅ Added `get_connection()` function that uses `DATABASE_URL` from environment
- ✅ Updated all functions to use secure connection method
- ✅ Changed schema references from `soccer` to `nwsfl`
- ✅ Added backward compatibility aliases (old function names still work)

**New Functions:**
- `get_all_nwsfl_tables()` - Get all tables in nwsfl schema
- `get_all_nwsfl_views()` - Get all views in nwsfl schema
- `backup_all_nwsfl_tables()` - Backup all tables to CSV
- `pull_all_nwsfl_reporting_tables()` - Pull reporting tables
- `pull_all_analytics_nwsfl_tables()` - Pull analytics views

**Deprecated (still work, but use new functions):**
- `get_all_soccer_tables()` → use `get_all_nwsfl_tables()`
- `get_all_soccer_views()` → use `get_all_nwsfl_views()`
- `backup_all_soccer_tables()` → use `backup_all_nwsfl_tables()`

### 2. `code_run.py`
**Changes:**
- ✅ Removed hardcoded connection string
- ✅ Uses `DATABASE_URL` from `.env` file
- ✅ Simplified import (removed invalid parent directory path)
- ✅ Now imports `query_db` directly from project directory

### 3. `scraping_script.py`
**Changes:**
- ✅ Changed default schema from `soccer` to `nwsfl` in `upsert_df()` function
- ✅ Updated SQLite database name from `soccer.db` to `nwsfl.db`

### 4. `data_config.yaml`
**Changes:**
- ✅ Updated all `table_schema` references from `soccer` to `nwsfl`
- ✅ Updated schema in `match_report_upsert_config`

## Database Schema

All data is now stored in the `nwsfl` schema in your Neon PostgreSQL database.

### Schema Copy
If you need to copy tables between schemas, use:
```bash
python copy_schemas_auto.py list   # List tables in soccer schema
python copy_schemas_auto.py copy   # Copy all tables to nwsfl schema
```

**Note:** Copy scripts are in `.gitignore` and won't be committed.

## Security Features

### Files Protected (in `.gitignore`):
- ✅ `.env` - Contains actual database credentials
- ✅ `config.yaml` - May contain sensitive config
- ✅ `streamlit_secrets.txt` - Streamlit secrets
- ✅ `db_config.yaml` - Database configuration
- ✅ `leagues.yaml` - League configuration
- ✅ `code_run_config_default.yaml` - Runtime configuration
- ✅ `copy_schemas*.py` - Database utility scripts

### Files Safe to Commit:
- ✅ `.env.example` - Template with no real credentials
- ✅ `requirements.txt` - Dependency list
- ✅ `query_db.py` - Now uses environment variables
- ✅ `code_run.py` - Now uses environment variables
- ✅ `scraping_script.py` - Now uses nwsfl schema
- ✅ `nwsfl_app.py` - Main Streamlit app

## Usage Examples

### Using query_db functions:
```python
from dotenv import load_dotenv
import query_db as qdb
import os

# Load environment variables
load_dotenv()

# Get connection string
conn_string = os.getenv('DATABASE_URL')

# Use query_db functions
tables = qdb.get_all_nwsfl_tables()
print(f"Found {len(tables)} tables")

# Backup all tables
qdb.backup_all_nwsfl_tables(conn_string)
```

### Direct database connection:
```python
import query_db as qdb

# Get a connection (uses DATABASE_URL from .env)
conn = qdb.get_connection()

# Use the connection
cursor = conn.cursor()
cursor.execute("SELECT * FROM nwsfl.dim_players LIMIT 5")
results = cursor.fetchall()

# Close when done
conn.close()
```

## Troubleshooting

### "ModuleNotFoundError: No module named 'dotenv'"
```bash
pip install -r requirements.txt
```

### "DATABASE_URL not found in environment variables"
Make sure you have a `.env` file in the project root with:
```env
DATABASE_URL=your_connection_string_here
```

### Import errors with query_db
Make sure all dependencies are installed:
```bash
pip install matplotlib scikit-learn
```

## Git Safety Checklist

Before pushing to GitHub, verify:
- ✅ `.env` file exists locally but is in `.gitignore`
- ✅ No hardcoded credentials in any `.py` files
- ✅ All config files with sensitive data are in `.gitignore`
- ✅ `.env.example` has placeholder values only

## Testing Connection

Test if everything is set up correctly:
```bash
python -c "import query_db; print('✓ Success')"
```

If this runs without errors, you're all set!

