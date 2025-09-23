#!/usr/bin/env python3
"""
Download portfolio data via the API to avoid direct connection issues.
"""

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

def test_api_connection():
    """Test if the API is working."""
    try:
        response = requests.get("http://localhost:8000/api/snowflake/health", timeout=30)
        if response.status_code == 200:
            health_data = response.json()
            print("✅ API is healthy!")
            print(f"   User: {health_data.get('current_user')}")
            print(f"   Database: {health_data.get('current_database')}")
            print(f"   Connection time: {health_data.get('connection_time'):.2f}s")
            return True
        else:
            print(f"❌ API health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ API connection error: {e}")
        return False

def download_portfolio_via_api(limit=100):
    """Download portfolio data via API."""
    
    print(f"🏔️  DOWNLOADING VIA API")
    print("=" * 40)
    print(f"Target rows: {limit}")
    print("-" * 40)
    
    # Test API first
    if not test_api_connection():
        return False
    
    # Prepare query
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
    ORDER BY LOAD_DATE DESC
    LIMIT {limit}
    """
    
    payload = {
        "query": query
    }
    
    try:
        print("🔍 Executing query via API...")
        start_time = datetime.now()
        
        response = requests.post(
            "http://localhost:8000/api/snowflake/query",
            json=payload,
            timeout=120  # 2 minutes timeout
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"✅ Query executed in {execution_time:.2f} seconds")
            print(f"📊 Retrieved {result.get('row_count', 0)} rows")
            
            if result.get('success') and result.get('data'):
                # Convert to DataFrame
                df = pd.DataFrame(result['data'])
                
                # Ensure data directory exists
                data_dir = Path("data")
                data_dir.mkdir(exist_ok=True)
                
                # Save to CSV
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"portfolio_api_{len(df)}rows_{timestamp}.csv"
                filepath = data_dir / filename
                
                df.to_csv(filepath, index=False)
                
                file_size = filepath.stat().st_size / 1024  # KB
                print(f"\n✅ SUCCESS!")
                print(f"📁 File saved: {filepath}")
                print(f"📏 Size: {file_size:.1f} KB")
                print(f"📊 Rows: {len(df)}")
                
                print(f"\n📋 Sample data:")
                print(df.head(3).to_string())
                
                print(f"\n📈 Summary:")
                if 'PORTFOLIO_ID' in df.columns:
                    print(f"  - Unique portfolios: {df['PORTFOLIO_ID'].nunique()}")
                if 'NAME_CUSTOMER' in df.columns:
                    print(f"  - Unique customers: {df['NAME_CUSTOMER'].nunique()}")
                if 'LOAD_DATE' in df.columns:
                    print(f"  - Date range: {df['LOAD_DATE'].min()} to {df['LOAD_DATE'].max()}")
                
                return True
            else:
                print(f"❌ Query failed: {result.get('error_message', 'Unknown error')}")
                return False
        else:
            print(f"❌ API request failed: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"   Error: {error_detail}")
            except:
                print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def download_multiple_sizes():
    """Try downloading different sizes to find what works."""
    
    sizes = [50, 100, 500, 1000]
    
    for size in sizes:
        print(f"\n{'='*50}")
        print(f"TRYING {size} ROWS")
        print(f"{'='*50}")
        
        success = download_portfolio_via_api(size)
        
        if success:
            print(f"✅ Successfully downloaded {size} rows!")
            
            # Ask if user wants to try larger
            if size < 1000:
                try_larger = input(f"\nTry downloading {sizes[sizes.index(size)+1] if sizes.index(size)+1 < len(sizes) else 2000} rows? (y/n): ")
                if try_larger.lower() != 'y':
                    break
            else:
                print("🎉 Large download successful!")
                break
        else:
            print(f"❌ Failed to download {size} rows")
            if size > 50:
                print("Trying smaller size...")
                continue
            else:
                print("Even small download failed. Check connection.")
                break

if __name__ == "__main__":
    print("🏔️  PORTFOLIO DATA DOWNLOADER (API)")
    print("=" * 50)
    print("1. Download 100 rows")
    print("2. Download 500 rows") 
    print("3. Download 1000 rows")
    print("4. Try multiple sizes")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice == "1":
        download_portfolio_via_api(100)
    elif choice == "2":
        download_portfolio_via_api(500)
    elif choice == "3":
        download_portfolio_via_api(1000)
    elif choice == "4":
        download_multiple_sizes()
    else:
        print("Invalid choice, downloading 100 rows...")
        download_portfolio_via_api(100)
