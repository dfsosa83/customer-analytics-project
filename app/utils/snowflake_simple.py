"""
Simple Snowflake Connection using Snowpark (based on working experiments)

This module replicates the working connection approach from your experiments folder.
"""

import os
import logging
from typing import Optional
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_connection_parameters():
    """Get connection parameters in the same format as your working example."""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),  # Try this first
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }

def get_connection_parameters_alt():
    """Alternative connection parameters using PROD variable like in experiments."""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("PROD", os.getenv("SNOWFLAKE_DATABASE")),  # Try PROD first, fallback to SNOWFLAKE_DATABASE
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }

def create_session(use_alt_params=False):
    """
    Create Snowpark session exactly like your working example.
    
    Args:
        use_alt_params: If True, try alternative parameter format
    """
    try:
        if use_alt_params:
            connection_parameters = get_connection_parameters_alt()
        else:
            connection_parameters = get_connection_parameters()
        
        # Log connection attempt (without sensitive data)
        masked_params = {k: v if k not in ['password'] else '***' for k, v in connection_parameters.items()}
        logger.info(f"Attempting connection with parameters: {masked_params}")
        
        session = Session.builder.configs(connection_parameters).create()
        logger.info("Successfully connected to Snowflake!")
        return session
        
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        return None

def test_connection():
    """Test the connection and return detailed results."""
    print("🧪 TESTING SNOWFLAKE CONNECTION (Simple Snowpark)")
    print("=" * 55)
    
    # Test standard parameters first
    print("\n📋 Testing with standard parameters...")
    session = create_session(use_alt_params=False)
    
    if session is None:
        print("❌ Standard parameters failed")
        print("\n📋 Testing with alternative parameters (PROD database)...")
        session = create_session(use_alt_params=True)
    
    if session is not None:
        try:
            print("✅ Connection successful!")
            
            # Test a simple query
            print("\n🔍 Testing query execution...")
            result = session.sql("SELECT CURRENT_VERSION()").collect()
            version = result[0][0]
            print(f"✅ Query successful - Snowflake version: {version}")
            
            # Test database access
            print("\n🗄️ Testing database access...")
            result = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
            database = result[0][0]
            schema = result[0][1]
            print(f"✅ Database access successful - DB: {database}, Schema: {schema}")
            
            session.close()
            print("\n🎉 All tests passed! Connection is working perfectly.")
            return True
            
        except Exception as e:
            print(f"❌ Query test failed: {e}")
            session.close()
            return False
    else:
        print("❌ All connection attempts failed")
        
        # Show troubleshooting info
        print("\n💡 Troubleshooting:")
        params = get_connection_parameters()
        print(f"   Account: {params['account']}")
        print(f"   User: {params['user']}")
        print(f"   Database: {params['database']}")
        print(f"   Warehouse: {params['warehouse']}")
        
        print("\n🔧 Try these fixes:")
        print("   1. Check if PROD environment variable exists")
        print("   2. Verify account format (remove .snowflakecomputing.com)")
        print("   3. Try different regions")
        
        return False

def query_data_simple(query: str) -> Optional[pd.DataFrame]:
    """
    Execute a query and return results as pandas DataFrame.
    Replicates the pattern from your experiments/query_data.py
    """
    session = create_session()
    
    if session is not None:
        try:
            # Execute query
            snow_df = session.sql(query)
            
            # Convert to pandas
            pandas_df = snow_df.to_pandas()
            
            print("Data retrieved successfully!")
            return pandas_df
            
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return None
        
        finally:
            session.close()
    
    return None

if __name__ == "__main__":
    test_connection()
