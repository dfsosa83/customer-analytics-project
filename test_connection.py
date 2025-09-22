#!/usr/bin/env python3
"""
Simple Snowflake connection test with detailed error reporting.
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_snowflake_connection():
    """Test Snowflake connection with detailed error reporting."""
    
    print("🧪 SNOWFLAKE CONNECTION TEST")
    print("=" * 40)
    
    try:
        # Load configuration
        from app.utils.snowflake_config import load_snowflake_config
        
        print("📋 Loading configuration...")
        config = load_snowflake_config()
        
        # Show masked config
        masked = config.mask_sensitive_data()
        print(f"   Account: {masked['snowflake_account']}")
        print(f"   User: {masked['snowflake_user']}")
        print(f"   Database: {masked['snowflake_database']}")
        print(f"   Warehouse: {masked['snowflake_warehouse']}")
        
        print("\n🔌 Testing connection...")
        
        # Try to import snowflake connector
        try:
            import snowflake.connector
            print("✅ Snowflake connector imported successfully")
        except ImportError as e:
            print(f"❌ Failed to import snowflake connector: {e}")
            print("💡 Install with: pip install snowflake-connector-python")
            return False
        
        # Try to connect
        try:
            conn = snowflake.connector.connect(
                account=config.snowflake_account,
                user=config.snowflake_user,
                password=config.snowflake_password,
                warehouse=config.snowflake_warehouse,
                database=config.snowflake_database,
                schema=config.snowflake_schema,
                role=config.snowflake_role,
                timeout=30
            )
            
            print("✅ Connection successful!")
            
            # Test a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            print(f"✅ Query test successful - Snowflake version: {result[0]}")
            
            cursor.close()
            conn.close()
            
            print("\n🎉 All tests passed! Your Snowflake connection is working.")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            
            # Provide specific troubleshooting based on error
            error_str = str(e).lower()
            
            if "250001" in error_str:
                print("\n💡 Error 250001 troubleshooting:")
                print("   1. Check account format (try without .snowflakecomputing.com)")
                print("   2. Verify region is correct")
                print("   3. Try lowercase account name")
                print("   4. Check network connectivity")
                
            elif "authentication" in error_str or "login" in error_str:
                print("\n💡 Authentication error troubleshooting:")
                print("   1. Verify username and password")
                print("   2. Check if account is locked")
                print("   3. Verify user has access to the warehouse/database")
                
            elif "network" in error_str or "timeout" in error_str:
                print("\n💡 Network error troubleshooting:")
                print("   1. Check internet connection")
                print("   2. Verify firewall settings")
                print("   3. Try from different network")
                
            return False
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False


if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
