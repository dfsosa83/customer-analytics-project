from snowflake_connect import create_session

def get_bonds_data():
    # Create session
    session = create_session()
    
    if session is not None:
        try:
            # Your query
            query = """
            SELECT *
            FROM DATA_SCIENCE.PUBLIC.HISTORY_PANAMA_GLOBAL_BONDS_1
            LIMIT 5
            """
            
            # Execute query
            snow_df = session.sql(query)
            
            # Convert to pandas
            pandas_df = snow_df.to_pandas()
            
            print("Data retrieved successfully!")
            print("\nFirst few rows:")
            print(pandas_df.head())
            
            return pandas_df
            
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return None
        
        finally:
            session.close()
            
if __name__ == "__main__":
    df = get_bonds_data()
