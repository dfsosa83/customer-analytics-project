#!/usr/bin/env python3
"""
Download datasets from Snowflake to local files.

This script uses the working Snowpark connection to download data
in various formats (CSV, JSON, Parquet, Excel).
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.utils.snowflake_simple import create_session

def download_dataset(query, filename=None, format='csv', limit=None):
    """
    Download a dataset from Snowflake.
    
    Args:
        query: SQL query to execute
        filename: Output filename (auto-generated if None)
        format: Output format ('csv', 'json', 'parquet', 'excel')
        limit: Maximum number of rows (None for all)
    """
    
    print(f"🏔️  SNOWFLAKE DATA DOWNLOAD")
    print("=" * 40)
    print(f"Query: {query}")
    print(f"Format: {format}")
    print(f"Limit: {limit or 'No limit'}")
    print("-" * 40)
    
    # Create session
    session = create_session()
    
    if session is None:
        print("❌ Failed to connect to Snowflake")
        return False
    
    try:
        # Add limit to query if specified
        if limit:
            if 'LIMIT' not in query.upper():
                query = f"{query} LIMIT {limit}"
        
        print("🔍 Executing query...")
        start_time = datetime.now()
        
        # Execute query and convert to pandas
        snow_df = session.sql(query)
        pandas_df = snow_df.to_pandas()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        print(f"✅ Query executed in {execution_time:.2f} seconds")
        print(f"📊 Retrieved {len(pandas_df)} rows, {len(pandas_df.columns)} columns")
        
        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"snowflake_data_{timestamp}.{format}"
        
        # Ensure data directory exists
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)

        filepath = data_dir / filename
        
        print(f"💾 Saving to: {filepath}")
        
        # Save in requested format
        if format.lower() == 'csv':
            pandas_df.to_csv(filepath, index=False)
        elif format.lower() == 'json':
            pandas_df.to_json(filepath, orient='records', indent=2)
        elif format.lower() == 'parquet':
            pandas_df.to_parquet(filepath, index=False)
        elif format.lower() == 'excel':
            pandas_df.to_excel(filepath, index=False)
        else:
            print(f"❌ Unsupported format: {format}")
            return False
        
        file_size = filepath.stat().st_size / (1024 * 1024)  # MB
        print(f"✅ File saved successfully!")
        print(f"📁 Location: {filepath.absolute()}")
        print(f"📏 Size: {file_size:.2f} MB")
        
        # Show preview
        print(f"\n📋 Data Preview:")
        print(pandas_df.head())
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False
        
    finally:
        session.close()

def list_tables():
    """List available tables in the current database."""
    print("🏔️  AVAILABLE TABLES")
    print("=" * 40)
    
    session = create_session()
    if session is None:
        print("❌ Failed to connect to Snowflake")
        return
    
    try:
        # Get tables in current schema
        result = session.sql("SHOW TABLES").collect()
        
        if result:
            print("📋 Tables in current schema:")
            for row in result:
                table_name = row[1]  # Table name is usually in the second column
                print(f"  - {table_name}")
        else:
            print("No tables found in current schema")
            
    except Exception as e:
        print(f"❌ Error listing tables: {str(e)}")
        
    finally:
        session.close()

def main():
    """Interactive data download."""
    print("🏔️  SNOWFLAKE DATA DOWNLOADER")
    print("=" * 50)
    
    # Show available tables
    list_tables()
    
    print("\n" + "=" * 50)
    print("📥 Download Options:")
    print("1. Download specific table")
    print("2. Custom SQL query")
    print("3. Quick examples")
    
    choice = input("\nSelect option (1-3): ").strip()
    
    if choice == "1":
        table_name = input("Enter table name: ").strip()
        limit = input("Enter row limit (or press Enter for all): ").strip()
        format_choice = input("Format (csv/json/parquet/excel) [csv]: ").strip() or "csv"
        
        limit = int(limit) if limit.isdigit() else None
        query = f"SELECT * FROM {table_name}"
        
        download_dataset(query, format=format_choice, limit=limit)
        
    elif choice == "2":
        print("\nEnter your SQL query (press Enter twice to finish):")
        query_lines = []
        while True:
            line = input()
            if line == "" and query_lines:
                break
            query_lines.append(line)
        
        query = " ".join(query_lines)
        format_choice = input("Format (csv/json/parquet/excel) [csv]: ").strip() or "csv"
        
        download_dataset(query, format=format_choice)
        
    elif choice == "3":
        print("\n📋 Quick Examples:")
        examples = [
            ("Current user info", "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()"),
            ("Sample data", "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 5"),
            ("Database info", "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        ]
        
        for i, (desc, query) in enumerate(examples, 1):
            print(f"{i}. {desc}")
        
        example_choice = input("\nSelect example (1-3): ").strip()
        if example_choice.isdigit() and 1 <= int(example_choice) <= len(examples):
            desc, query = examples[int(example_choice) - 1]
            print(f"\nExecuting: {desc}")
            download_dataset(query, filename=f"{desc.lower().replace(' ', '_')}.csv")

if __name__ == "__main__":
    main()
