#!/usr/bin/env python3
"""
Simple download using the exact working connection from experiments.
"""

import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd
from pathlib import Path
from datetime import datetime

# Load environment variables
load_dotenv()

# Connection parameters (exact copy from experiments)
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("PROD"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

def simple_download():
    """Simple download with minimal data to avoid SSL issues."""
    
    print("🏔️  SIMPLE PORTFOLIO DOWNLOAD")
    print("=" * 40)
    
    try:
        # Create session using exact working method
        print("🔗 Connecting to Snowflake...")
        session = Session.builder.configs(connection_parameters).create()
        print("✅ Successfully connected to Snowflake!")
        
        # First, get basic info
        print("\n📊 Getting table info...")
        count_query = "SELECT COUNT(*) as total FROM RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY"
        count_result = session.sql(count_query).collect()
        total_rows = count_result[0][0]
        print(f"Total rows in table: {total_rows:,}")
        
        # Try a very simple query with just a few key columns and small limit
        print("\n🔍 Downloading sample data...")
        
        simple_query = """
        SELECT 
            LOAD_DATE,
            PORTFOLIO_ID,
            NAME_CUSTOMER,
            SECURITY_NO,
            NAME_SECURITY,
            VALOR_MERCADO_LCY
        FROM RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY 
        WHERE LOAD_DATE = 20250902
        LIMIT 50
        """
        
        print("Query:", simple_query)
        
        # Execute query
        start_time = datetime.now()
        result = session.sql(simple_query).collect()
        execution_time = (datetime.now() - start_time).total_seconds()
        
        print(f"✅ Query executed in {execution_time:.2f} seconds")
        print(f"📊 Retrieved {len(result)} rows")
        
        if len(result) == 0:
            print("⚠️  No data returned. Trying different date...")
            
            # Try without date filter
            simple_query2 = """
            SELECT 
                LOAD_DATE,
                PORTFOLIO_ID,
                NAME_CUSTOMER,
                SECURITY_NO,
                NAME_SECURITY,
                VALOR_MERCADO_LCY
            FROM RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY 
            LIMIT 20
            """
            
            result = session.sql(simple_query2).collect()
            print(f"📊 Retrieved {len(result)} rows (no date filter)")
        
        if result:
            # Convert to simple format
            print("🔄 Converting to DataFrame...")
            
            data = []
            for row in result:
                data.append({
                    'LOAD_DATE': row[0],
                    'PORTFOLIO_ID': row[1],
                    'NAME_CUSTOMER': row[2],
                    'SECURITY_NO': row[3],
                    'NAME_SECURITY': row[4],
                    'VALOR_MERCADO_LCY': row[5]
                })
            
            df = pd.DataFrame(data)
            
            # Ensure data directory exists
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            # Save to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"portfolio_simple_{len(df)}rows_{timestamp}.csv"
            filepath = data_dir / filename
            
            df.to_csv(filepath, index=False)
            
            file_size = filepath.stat().st_size / 1024  # KB
            print(f"\n✅ SUCCESS!")
            print(f"📁 File saved: {filepath}")
            print(f"📏 Size: {file_size:.1f} KB")
            print(f"📊 Rows: {len(df)}")
            
            print(f"\n📋 Sample data:")
            print(df.head().to_string())
            
            print(f"\n📈 Summary:")
            print(f"  - Unique portfolios: {df['PORTFOLIO_ID'].nunique()}")
            print(f"  - Unique customers: {df['NAME_CUSTOMER'].nunique()}")
            print(f"  - Date range: {df['LOAD_DATE'].min()} to {df['LOAD_DATE'].max()}")
            
            return True
        else:
            print("❌ No data returned from query")
            return False
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False
    finally:
        try:
            session.close()
            print("\n🔒 Session closed")
        except:
            pass

if __name__ == "__main__":
    simple_download()
