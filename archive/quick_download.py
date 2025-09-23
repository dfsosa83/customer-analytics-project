#!/usr/bin/env python3
"""
Quick download examples for common Snowflake data export scenarios.
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from download_data import download_dataset, list_tables

def download_full_table(table_name, format='csv'):
    """Download a complete table."""
    query = f"SELECT * FROM {table_name}"
    filename = f"{table_name.lower()}.{format}"
    return download_dataset(query, filename=filename, format=format)

def download_sample(table_name, rows=1000, format='csv'):
    """Download a sample of a table."""
    query = f"SELECT * FROM {table_name}"
    filename = f"{table_name.lower()}_sample_{rows}.{format}"
    return download_dataset(query, filename=filename, format=format, limit=rows)

def download_recent_data(table_name, date_column, days=30, format='csv'):
    """Download recent data from a table."""
    query = f"""
    SELECT * FROM {table_name} 
    WHERE {date_column} >= CURRENT_DATE - {days}
    ORDER BY {date_column} DESC
    """
    filename = f"{table_name.lower()}_last_{days}_days.{format}"
    return download_dataset(query, filename=filename, format=format)

def download_custom_query(query, filename=None, format='csv'):
    """Download data from a custom query."""
    return download_dataset(query, filename=filename, format=format)

# Example usage functions
def example_downloads():
    """Show example downloads you can run."""
    
    print("🏔️  QUICK DOWNLOAD EXAMPLES")
    print("=" * 50)
    
    examples = [
        {
            "name": "Download Panama Bonds Data (if exists)",
            "function": lambda: download_sample("HISTORY_PANAMA_GLOBAL_BONDS_1", 100, 'csv')
        },
        {
            "name": "Download Current Session Info",
            "function": lambda: download_custom_query(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()",
                "session_info.csv"
            )
        },
        {
            "name": "Download Database Schema Info",
            "function": lambda: download_custom_query(
                "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'",
                "schema_tables.csv"
            )
        },
        {
            "name": "List All Available Tables",
            "function": list_tables
        }
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example['name']}")
    
    choice = input(f"\nSelect example (1-{len(examples)}): ").strip()
    
    if choice.isdigit() and 1 <= int(choice) <= len(examples):
        selected = examples[int(choice) - 1]
        print(f"\n🚀 Running: {selected['name']}")
        print("-" * 40)
        selected['function']()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    example_downloads()
