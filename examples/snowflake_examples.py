#!/usr/bin/env python3
"""
Snowflake Usage Examples

This script demonstrates how to use the Snowflake utilities in the
Customer Analytics Project.

Author: Customer Analytics Team
Version: 1.0.0
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.utils import (
    load_snowflake_config,
    get_connection_manager,
    execute_query,
    execute_query_to_dataframe,
    get_data_exporter,
    validate_snowflake_setup,
    ValidationLevel
)


def example_1_basic_connection():
    """Example 1: Basic connection and query execution."""
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Connection and Query")
    print("="*60)
    
    try:
        # Load configuration
        config = load_snowflake_config()
        print(f"✅ Configuration loaded for account: {config.snowflake_account}")
        
        # Get connection manager
        manager = get_connection_manager()
        
        # Test connection
        status = manager.test_connection()
        if status['success']:
            print(f"✅ Connection successful!")
            print(f"   User: {status['current_user']}")
            print(f"   Database: {status['current_database']}")
            print(f"   Schema: {status['current_schema']}")
        else:
            print(f"❌ Connection failed: {status['error']}")
            return
        
        # Execute a simple query
        result = execute_query("SELECT CURRENT_TIMESTAMP() as current_time")
        
        if result.success:
            print(f"✅ Query executed successfully!")
            print(f"   Execution time: {result.execution_time:.2f}s")
            print(f"   Result: {result.data[0]['CURRENT_TIME']}")
        else:
            print(f"❌ Query failed: {result.error_message}")
            
    except Exception as e:
        print(f"❌ Example failed: {e}")


def example_2_dataframe_query():
    """Example 2: Query execution with pandas DataFrame."""
    print("\n" + "="*60)
    print("EXAMPLE 2: Query with pandas DataFrame")
    print("="*60)
    
    try:
        # Execute query and get DataFrame
        query = """
            SELECT 
                'Customer Analytics' as project,
                CURRENT_USER() as user_name,
                CURRENT_DATABASE() as database_name,
                CURRENT_SCHEMA() as schema_name,
                CURRENT_TIMESTAMP() as timestamp
        """
        
        df = execute_query_to_dataframe(query)
        
        print(f"✅ DataFrame created successfully!")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        print("\nData:")
        print(df.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Example failed: {e}")


def example_3_table_operations():
    """Example 3: Table operations and metadata."""
    print("\n" + "="*60)
    print("EXAMPLE 3: Table Operations")
    print("="*60)
    
    try:
        manager = get_connection_manager()
        
        # List tables in current schema
        query = f"""
            SELECT 
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                CREATED
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{manager.config.snowflake_schema.upper()}'
            ORDER BY TABLE_NAME
            LIMIT 10
        """
        
        result = execute_query(query)
        
        if result.success:
            print(f"✅ Found {result.row_count} tables in schema '{manager.config.snowflake_schema}':")
            
            for table in result.data:
                print(f"   - {table['TABLE_NAME']} ({table['TABLE_TYPE']}) - {table['ROW_COUNT']} rows")
                
            # If we found tables, get info about the first one
            if result.data:
                first_table = result.data[0]['TABLE_NAME']
                try:
                    table_info = manager.get_table_info(first_table)
                    print(f"\n📊 Details for table '{first_table}':")
                    print(f"   Columns: {table_info['column_count']}")
                    print(f"   Rows: {table_info['row_count']}")
                    print("   Column details:")
                    for col in table_info['columns'][:5]:  # Show first 5 columns
                        print(f"     - {col['COLUMN_NAME']} ({col['DATA_TYPE']})")
                    
                    if len(table_info['columns']) > 5:
                        print(f"     ... and {len(table_info['columns']) - 5} more columns")
                        
                except Exception as e:
                    print(f"   ⚠️  Could not get table details: {e}")
        else:
            print(f"❌ Failed to list tables: {result.error_message}")
            
    except Exception as e:
        print(f"❌ Example failed: {e}")


def example_4_data_export():
    """Example 4: Data export functionality."""
    print("\n" + "="*60)
    print("EXAMPLE 4: Data Export")
    print("="*60)
    
    try:
        exporter = get_data_exporter()
        
        # Create sample data query
        query = """
            SELECT 
                seq4() as id,
                'Customer_' || seq4() as customer_name,
                uniform(18, 80, random()) as age,
                uniform(1000, 50000, random()) as annual_revenue,
                CURRENT_TIMESTAMP() as created_at
            FROM table(generator(rowcount => 50))
        """
        
        # Get export preview
        preview = exporter.get_export_preview(query, limit=5)
        
        if preview['success']:
            print(f"✅ Export preview generated:")
            print(f"   Estimated total rows: {preview['total_estimated_rows']}")
            print(f"   Columns: {preview['column_count']}")
            print(f"   Preview rows: {preview['preview_row_count']}")
            print(f"   Supported formats: {', '.join(preview['supported_formats'])}")
            
            print("\n📋 Preview data (first 3 rows):")
            for i, row in enumerate(preview['preview_data'][:3]):
                print(f"   Row {i+1}: ID={row['ID']}, Name={row['CUSTOMER_NAME']}, Age={row['AGE']}")
            
            # Save to local file
            export_info = exporter.save_query_results_to_file(
                query=query,
                file_path="temp_export_example.csv",
                format="csv"
            )
            
            if export_info['success']:
                print(f"\n✅ Data exported to file:")
                print(f"   File: {export_info['file_path']}")
                print(f"   Format: {export_info['format']}")
                print(f"   Rows: {export_info['row_count']}")
                print(f"   Size: {export_info['file_size_mb']} MB")
                
                # Clean up
                import os
                if os.path.exists(export_info['file_path']):
                    os.remove(export_info['file_path'])
                    print(f"   (Temporary file cleaned up)")
            else:
                print(f"❌ Export failed")
                
        else:
            print(f"❌ Export preview failed: {preview['error']}")
            
    except Exception as e:
        print(f"❌ Example failed: {e}")


def example_5_validation():
    """Example 5: Configuration validation."""
    print("\n" + "="*60)
    print("EXAMPLE 5: Configuration Validation")
    print("="*60)
    
    try:
        # Run basic validation
        report = validate_snowflake_setup(ValidationLevel.BASIC)
        
        print(f"Validation Results:")
        print(f"   Overall Status: {'✅ PASSED' if report.overall_success else '❌ FAILED'}")
        print(f"   Total Checks: {report.total_checks}")
        print(f"   Passed: {report.passed_checks}")
        print(f"   Failed: {report.failed_checks}")
        print(f"   Total Time: {report.total_time:.2f}s")
        
        print(f"\nIndividual Check Results:")
        for result in report.results:
            status = "✅" if result.success else "❌"
            print(f"   {status} {result.check_name}: {result.message}")
            
    except Exception as e:
        print(f"❌ Example failed: {e}")


def main():
    """Run all examples."""
    print("🏔️  SNOWFLAKE USAGE EXAMPLES")
    print("Customer Analytics Project")
    print("="*70)
    
    print("\nThis script demonstrates various Snowflake operations.")
    print("Make sure you have configured your .env file with valid Snowflake credentials.")
    
    # Check if configuration exists
    try:
        config = load_snowflake_config()
        print(f"\n✅ Configuration found for account: {config.snowflake_account}")
    except Exception as e:
        print(f"\n❌ Configuration error: {e}")
        print("\n💡 Please run the setup script first:")
        print("   python scripts/setup_snowflake.py")
        return
    
    # Run examples
    examples = [
        example_1_basic_connection,
        example_2_dataframe_query,
        example_3_table_operations,
        example_4_data_export,
        example_5_validation
    ]
    
    for i, example_func in enumerate(examples, 1):
        try:
            example_func()
        except KeyboardInterrupt:
            print(f"\n\n⏹️  Examples interrupted by user.")
            break
        except Exception as e:
            print(f"\n❌ Example {i} failed with unexpected error: {e}")
        
        if i < len(examples):
            input(f"\nPress Enter to continue to Example {i+1}...")
    
    print("\n" + "="*70)
    print("🎉 Examples completed!")
    print("\nFor more information, see:")
    print("- docs/snowflake_setup.md")
    print("- API documentation: http://localhost:8000/docs (when server is running)")


if __name__ == "__main__":
    main()
