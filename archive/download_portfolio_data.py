#!/usr/bin/env python3
"""
Download portfolio data with SSL certificate workaround.
Uses smaller batches to avoid S3 certificate issues.
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import time

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.utils.snowflake_simple import create_session

def download_portfolio_batch(limit=1000, offset=0, filename=None):
    """
    Download portfolio data in smaller batches to avoid SSL issues.
    """
    
    print(f"🏔️  DOWNLOADING PORTFOLIO DATA (BATCH)")
    print("=" * 50)
    print(f"Limit: {limit} rows")
    print(f"Offset: {offset}")
    print("-" * 50)
    
    session = create_session()
    if session is None:
        print("❌ Failed to connect to Snowflake")
        return False
    
    try:
        # Use a simpler query approach with LIMIT and OFFSET
        query = f"""
        SELECT 
            LOAD_DATE,
            ANIO,
            MES, 
            DIA,
            MIS_DATE,
            PORTFOLIO_ID,
            NAME_CUSTOMER,
            SECURITY_NO,
            NAME_SECURITY,
            SECURITY_CCY,
            NOMINAL,
            MARKET_PRICE_LCY,
            MARKET_PRICE_CCY,
            VALOR_MERCADO_LCY,
            VALOR_MERCADO_CCY,
            COSTO,
            P_G_NO_REALIZADAS,
            ACCOUNT_OFFICER,
            OFFICER_NAME,
            OFFICER_AREA,
            RATING,
            RATING_DESC
        FROM RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY 
        ORDER BY LOAD_DATE DESC, PORTFOLIO_ID
        LIMIT {limit}
        """
        
        if offset > 0:
            query += f" OFFSET {offset}"
        
        print("🔍 Executing query...")
        start_time = datetime.now()
        
        # Execute query and get results
        result = session.sql(query).collect()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ Query executed in {execution_time:.2f} seconds")
        print(f"📊 Retrieved {len(result)} rows")
        
        if not result:
            print("❌ No data returned")
            return False
        
        # Convert to pandas DataFrame manually
        print("🔄 Converting to DataFrame...")
        data = []
        for row in result:
            row_dict = row.as_dict()
            data.append(row_dict)
        
        df = pd.DataFrame(data)
        print(f"📊 DataFrame created: {len(df)} rows, {len(df.columns)} columns")
        
        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"portfolio_security_batch_{limit}_{timestamp}.csv"
        
        # Ensure data directory exists
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        filepath = data_dir / filename
        
        print(f"💾 Saving to: {filepath}")
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        
        file_size = filepath.stat().st_size / (1024 * 1024)  # MB
        print(f"✅ File saved successfully!")
        print(f"📁 Location: {filepath.absolute()}")
        print(f"📏 Size: {file_size:.2f} MB")
        
        # Show preview
        print(f"\n📋 Data Preview (first 3 rows):")
        print(df.head(3).to_string())
        
        print(f"\n📊 Summary:")
        print(f"  - Total rows: {len(df)}")
        print(f"  - Date range: {df['LOAD_DATE'].min()} to {df['LOAD_DATE'].max()}")
        print(f"  - Unique portfolios: {df['PORTFOLIO_ID'].nunique()}")
        print(f"  - Unique customers: {df['NAME_CUSTOMER'].nunique()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False
        
    finally:
        session.close()

def download_multiple_batches(total_rows=10000, batch_size=1000):
    """Download data in multiple batches."""
    
    print(f"🏔️  DOWNLOADING PORTFOLIO DATA (MULTIPLE BATCHES)")
    print("=" * 60)
    print(f"Total target rows: {total_rows}")
    print(f"Batch size: {batch_size}")
    print("-" * 60)
    
    all_data = []
    batches = (total_rows + batch_size - 1) // batch_size  # Ceiling division
    
    for batch_num in range(batches):
        offset = batch_num * batch_size
        current_limit = min(batch_size, total_rows - offset)
        
        print(f"\n🔄 Processing batch {batch_num + 1}/{batches}")
        print(f"   Rows: {offset + 1} to {offset + current_limit}")
        
        session = create_session()
        if session is None:
            print("❌ Failed to connect to Snowflake")
            break
        
        try:
            query = f"""
            SELECT 
                LOAD_DATE, PORTFOLIO_ID, NAME_CUSTOMER, SECURITY_NO, NAME_SECURITY,
                SECURITY_CCY, NOMINAL, VALOR_MERCADO_LCY, VALOR_MERCADO_CCY,
                COSTO, P_G_NO_REALIZADAS, OFFICER_NAME, RATING_DESC
            FROM RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY 
            ORDER BY LOAD_DATE DESC, PORTFOLIO_ID
            LIMIT {current_limit} OFFSET {offset}
            """
            
            result = session.sql(query).collect()
            
            if result:
                batch_data = [row.as_dict() for row in result]
                all_data.extend(batch_data)
                print(f"   ✅ Retrieved {len(result)} rows")
            else:
                print(f"   ⚠️  No more data available")
                break
                
        except Exception as e:
            print(f"   ❌ Error in batch {batch_num + 1}: {str(e)}")
            break
        finally:
            session.close()
        
        # Small delay between batches
        time.sleep(1)
    
    if all_data:
        # Combine all data
        df = pd.DataFrame(all_data)
        
        # Save combined data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"portfolio_security_combined_{len(df)}rows_{timestamp}.csv"
        
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        filepath = data_dir / filename
        
        df.to_csv(filepath, index=False)
        
        file_size = filepath.stat().st_size / (1024 * 1024)
        print(f"\n🎉 COMBINED DOWNLOAD COMPLETE!")
        print(f"📁 Location: {filepath.absolute()}")
        print(f"📊 Total rows: {len(df)}")
        print(f"📏 Size: {file_size:.2f} MB")
        
        return True
    
    return False

def main():
    """Main function with options."""
    print("🏔️  PORTFOLIO DATA DOWNLOADER")
    print("=" * 50)
    print("1. Download 1,000 rows (single batch)")
    print("2. Download 5,000 rows (5 batches)")
    print("3. Download 10,000 rows (10 batches)")
    print("4. Custom download")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice == "1":
        download_portfolio_batch(1000)
    elif choice == "2":
        download_multiple_batches(5000, 1000)
    elif choice == "3":
        download_multiple_batches(10000, 1000)
    elif choice == "4":
        try:
            rows = int(input("Enter number of rows: "))
            batch_size = int(input("Enter batch size (recommended: 1000): ") or "1000")
            download_multiple_batches(rows, batch_size)
        except ValueError:
            print("Invalid input")
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()
