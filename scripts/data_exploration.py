#!/usr/bin/env python3
"""
Data exploration script for Customer Analytics Project.

This script provides utilities for exploring Snowflake data sources
and understanding the structure of customer and product data.
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from app.utils.database import get_snowflake_connection
    from app.config import settings
except ImportError:
    print("⚠️  Application modules not available. Run this after implementing the core app.")
    sys.exit(1)


class DataExplorer:
    """Utility class for exploring Snowflake data sources."""
    
    def __init__(self):
        """Initialize the data explorer."""
        self.connection = None
        
    def connect(self):
        """Establish connection to Snowflake."""
        try:
            self.connection = get_snowflake_connection()
            print("✅ Connected to Snowflake")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            return False
    
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List all tables in the specified schema."""
        if not self.connection:
            print("❌ No database connection")
            return []
        
        schema = schema or settings.SNOWFLAKE_SCHEMA
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema.upper()}'
        ORDER BY table_name
        """
        
        try:
            df = pd.read_sql(query, self.connection)
            tables = df['TABLE_NAME'].tolist()
            print(f"📋 Found {len(tables)} tables in schema '{schema}':")
            for table in tables:
                print(f"  - {table}")
            return tables
        except Exception as e:
            print(f"❌ Error listing tables: {e}")
            return []
    
    def describe_table(self, table_name: str) -> pd.DataFrame:
        """Get column information for a table."""
        if not self.connection:
            print("❌ No database connection")
            return pd.DataFrame()
        
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = '{table_name.upper()}'
        AND table_schema = '{settings.SNOWFLAKE_SCHEMA.upper()}'
        ORDER BY ordinal_position
        """
        
        try:
            df = pd.read_sql(query, self.connection)
            print(f"\n📊 Table structure for '{table_name}':")
            print(df.to_string(index=False))
            return df
        except Exception as e:
            print(f"❌ Error describing table: {e}")
            return pd.DataFrame()
    
    def sample_data(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """Get sample data from a table."""
        if not self.connection:
            print("❌ No database connection")
            return pd.DataFrame()
        
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        try:
            df = pd.read_sql(query, self.connection)
            print(f"\n📋 Sample data from '{table_name}' (first {limit} rows):")
            print(df.to_string(index=False))
            return df
        except Exception as e:
            print(f"❌ Error sampling data: {e}")
            return pd.DataFrame()
    
    def table_stats(self, table_name: str) -> Dict:
        """Get basic statistics for a table."""
        if not self.connection:
            print("❌ No database connection")
            return {}
        
        query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        
        try:
            df = pd.read_sql(query, self.connection)
            row_count = df['ROW_COUNT'].iloc[0]
            
            stats = {
                'table_name': table_name,
                'row_count': row_count
            }
            
            print(f"\n📈 Statistics for '{table_name}':")
            print(f"  Total rows: {row_count:,}")
            
            return stats
        except Exception as e:
            print(f"❌ Error getting table stats: {e}")
            return {}
    
    def explore_relationships(self, customer_table: str, product_table: str, 
                            relationship_table: str) -> None:
        """Explore relationships between customer, product, and relationship tables."""
        if not self.connection:
            print("❌ No database connection")
            return
        
        print(f"\n🔍 Exploring relationships between tables...")
        
        # Check for common columns
        queries = {
            'customer_product_relationships': f"""
                SELECT COUNT(*) as relationship_count
                FROM {relationship_table}
            """,
            'unique_customers': f"""
                SELECT COUNT(DISTINCT customer_id) as unique_customers
                FROM {relationship_table}
            """,
            'unique_products': f"""
                SELECT COUNT(DISTINCT product_id) as unique_products
                FROM {relationship_table}
            """
        }
        
        for description, query in queries.items():
            try:
                df = pd.read_sql(query, self.connection)
                result = df.iloc[0, 0]
                print(f"  {description}: {result:,}")
            except Exception as e:
                print(f"  ❌ Error with {description}: {e}")


def main():
    """Main exploration function."""
    print("🔍 Customer Analytics Data Explorer")
    print("=" * 50)
    
    explorer = DataExplorer()
    
    if not explorer.connect():
        sys.exit(1)
    
    # List all tables
    tables = explorer.list_tables()
    
    if not tables:
        print("No tables found. Please check your Snowflake configuration.")
        sys.exit(1)
    
    # Interactive exploration
    while True:
        print("\n" + "=" * 50)
        print("Available commands:")
        print("1. List tables")
        print("2. Describe table structure")
        print("3. Sample table data")
        print("4. Table statistics")
        print("5. Explore relationships")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            explorer.list_tables()
        
        elif choice == '2':
            table_name = input("Enter table name: ").strip()
            if table_name:
                explorer.describe_table(table_name)
        
        elif choice == '3':
            table_name = input("Enter table name: ").strip()
            limit = input("Enter number of rows (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            if table_name:
                explorer.sample_data(table_name, limit)
        
        elif choice == '4':
            table_name = input("Enter table name: ").strip()
            if table_name:
                explorer.table_stats(table_name)
        
        elif choice == '5':
            customer_table = input("Enter customer table name: ").strip()
            product_table = input("Enter product table name: ").strip()
            relationship_table = input("Enter relationship table name: ").strip()
            if all([customer_table, product_table, relationship_table]):
                explorer.explore_relationships(customer_table, product_table, relationship_table)
        
        elif choice == '6':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
