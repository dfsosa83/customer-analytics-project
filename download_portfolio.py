#!/usr/bin/env python3
"""
🏔️ SNOWFLAKE PORTFOLIO DATA DOWNLOADER

Easy-to-use script for downloading portfolio data from Snowflake.
This is the final, production-ready version.

Usage:
    python download_portfolio.py

Features:
- Downloads portfolio data from RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY
- Multiple size options (100, 500, 1000+ rows)
- Automatic batch processing for large datasets
- Saves to data/ folder with timestamps
- Handles SSL certificate issues automatically

Author: Customer Analytics Team
Version: 1.0.0
"""

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

def check_api_health():
    """Check if the Snowflake API is running and healthy."""
    try:
        response = requests.get("http://localhost:8000/api/snowflake/health", timeout=10)
        if response.status_code == 200:
            health_data = response.json()
            print("✅ Snowflake API is healthy!")
            print(f"   User: {health_data.get('current_user', 'Unknown')}")
            print(f"   Database: {health_data.get('current_database', 'Unknown')}")
            print(f"   Connection time: {health_data.get('connection_time', 0):.2f}s")
            return True
        else:
            print(f"❌ API health check failed: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API. Please start the server first:")
        print("   uvicorn app.main:app --reload --port 8000")
        return False
    except Exception as e:
        print(f"❌ API connection error: {e}")
        return False

def download_single_batch(limit=500, offset=0):
    """Download a single batch of portfolio data."""
    
    query = f"""
    SELECT 
        LOAD_DATE,
        PORTFOLIO_ID,
        NAME_CUSTOMER,
        SECURITY_NO,
        NAME_SECURITY,
        SECURITY_CCY,
        NOMINAL,
        VALOR_MERCADO_LCY,
        VALOR_MERCADO_CCY,
        COSTO,
        P_G_NO_REALIZADAS,
        OFFICER_NAME,
        RATING_DESC
    FROM RESULTADO.PRIVALBANK.TG_FACT_PORTFOLIO_SECURITY 
    ORDER BY LOAD_DATE DESC, PORTFOLIO_ID
    LIMIT {limit}
    """
    
    if offset > 0:
        query += f" OFFSET {offset}"
    
    payload = {"query": query}
    
    try:
        response = requests.post(
            "http://localhost:8000/api/snowflake/query",
            json=payload,
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success') and result.get('data'):
                return result['data'], result.get('execution_time', 0)
            else:
                print(f"❌ Query failed: {result.get('error_message', 'Unknown error')}")
                return None, 0
        else:
            print(f"❌ API request failed: {response.status_code}")
            return None, 0
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None, 0

def download_portfolio_data(target_rows=500):
    """Download portfolio data with automatic batch processing."""
    
    print(f"🏔️  DOWNLOADING PORTFOLIO DATA")
    print("=" * 50)
    print(f"Target rows: {target_rows}")
    print("-" * 50)
    
    # Check API health first
    if not check_api_health():
        return False
    
    all_data = []
    batch_size = 500  # Safe batch size that avoids SSL issues
    
    if target_rows <= batch_size:
        # Single batch download
        print(f"🔍 Downloading {target_rows} rows...")
        start_time = datetime.now()
        
        data, exec_time = download_single_batch(target_rows)
        
        if data:
            all_data = data
            total_time = (datetime.now() - start_time).total_seconds()
            print(f"✅ Downloaded {len(data)} rows in {total_time:.2f}s")
        else:
            print("❌ Download failed")
            return False
    else:
        # Multiple batch download
        batches_needed = (target_rows + batch_size - 1) // batch_size
        print(f"📦 Using {batches_needed} batches of {batch_size} rows each")
        
        for batch_num in range(1, batches_needed + 1):
            offset = (batch_num - 1) * batch_size
            current_limit = min(batch_size, target_rows - len(all_data))
            
            if current_limit <= 0:
                break
            
            print(f"🔄 Batch {batch_num}/{batches_needed}: rows {offset+1} to {offset+current_limit}")
            
            data, exec_time = download_single_batch(current_limit, offset)
            
            if data:
                all_data.extend(data)
                print(f"   ✅ Retrieved {len(data)} rows (total: {len(all_data)})")
                
                # Small delay between batches
                if batch_num < batches_needed:
                    time.sleep(1)
            else:
                print(f"   ❌ Batch {batch_num} failed")
                break
    
    if all_data:
        # Save data
        df = pd.DataFrame(all_data)
        
        # Create filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"portfolio_data_{len(df)}rows_{timestamp}.csv"
        
        # Ensure data directory exists
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        filepath = data_dir / filename
        df.to_csv(filepath, index=False)
        
        file_size = filepath.stat().st_size / (1024 * 1024)  # MB
        
        print(f"\n🎉 DOWNLOAD SUCCESSFUL!")
        print(f"📁 File: {filepath}")
        print(f"📊 Rows: {len(df):,}")
        print(f"📏 Size: {file_size:.2f} MB")
        
        # Show summary
        print(f"\n📈 Data Summary:")
        if 'PORTFOLIO_ID' in df.columns:
            print(f"  - Unique portfolios: {df['PORTFOLIO_ID'].nunique():,}")
        if 'NAME_CUSTOMER' in df.columns:
            print(f"  - Unique customers: {df['NAME_CUSTOMER'].nunique():,}")
        if 'LOAD_DATE' in df.columns:
            print(f"  - Date range: {df['LOAD_DATE'].min()} to {df['LOAD_DATE'].max()}")
        
        # Show sample
        print(f"\n📋 Sample Data (first 3 rows):")
        print(df.head(3)[['PORTFOLIO_ID', 'NAME_CUSTOMER', 'NAME_SECURITY', 'VALOR_MERCADO_LCY']].to_string())
        
        return True
    else:
        print("\n❌ No data downloaded")
        return False

def main():
    """Main function with user-friendly options."""
    
    print("🏔️  SNOWFLAKE PORTFOLIO DATA DOWNLOADER")
    print("=" * 60)
    print("📊 Download portfolio holdings from Snowflake")
    print("💾 Data saved to: data/portfolio_data_[rows]_[timestamp].csv")
    print("-" * 60)
    
    print("\n📋 Download Options:")
    print("1. Quick sample (100 rows) - ~30 seconds")
    print("2. Medium dataset (500 rows) - ~1 minute") 
    print("3. Large dataset (2,500 rows) - ~3 minutes")
    print("4. Extra large (5,000 rows) - ~6 minutes")
    print("5. Custom size")
    
    while True:
        choice = input("\nSelect option (1-5): ").strip()
        
        if choice == "1":
            success = download_portfolio_data(100)
            break
        elif choice == "2":
            success = download_portfolio_data(500)
            break
        elif choice == "3":
            success = download_portfolio_data(2500)
            break
        elif choice == "4":
            success = download_portfolio_data(5000)
            break
        elif choice == "5":
            try:
                custom_rows = int(input("Enter number of rows: "))
                if custom_rows > 0:
                    success = download_portfolio_data(custom_rows)
                    break
                else:
                    print("Please enter a positive number")
            except ValueError:
                print("Please enter a valid number")
        else:
            print("Please select 1-5")
    
    if success:
        print(f"\n✨ Download complete! Check the data/ folder for your file.")
        print(f"🚀 You can run this script anytime to get fresh data.")
    else:
        print(f"\n💡 Troubleshooting:")
        print(f"   1. Make sure the API server is running:")
        print(f"      uvicorn app.main:app --reload --port 8000")
        print(f"   2. Check your Snowflake connection")
        print(f"   3. Try a smaller dataset first")

if __name__ == "__main__":
    main()
