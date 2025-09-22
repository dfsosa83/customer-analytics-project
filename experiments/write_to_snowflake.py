from snowflake_connect import create_session
from transform_data import transform_bonds_data
import pandas as pd

def write_to_snowflake():
    # Define the file path
    file_path = 'C:/Users/dsosa/Documents/snowflake_visual/data/processed/panama_short_term_999.csv'
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Convert columns to numeric, keeping NaN values
        #numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        #for col in numeric_columns:
         #   df[col] = pd.to_numeric(df[col], errors='coerce')[3]
        
        # Create session
        session = create_session()
        
        # Create the target table and stage
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS TRANSFORMED_BONDS_PANAMA_SHORT_TERM_999 (
            date TIMESTAMP,
            brasil_short_term DOUBLE,
            chile_short_term DOUBLE,
            colombia_short_term DOUBLE,
            crc_short_term DOUBLE,
            indonesia_short_term DOUBLE,
            mexico_short_term DOUBLE,
            peru_short_term DOUBLE,
            rd_short_term DOUBLE,
            salvador_short_term DOUBLE,
            panama_short_term DOUBLE
)

        """
        
        create_stage_sql = """
        CREATE OR REPLACE STAGE my_csv_stage
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        """
        
        session.sql(create_table_sql).collect()
        session.sql(create_stage_sql).collect()
        
        # Write to Snowflake
        session.write_pandas(
            df,
            "TRANSFORMED_BONDS_PANAMA_SHORT_TERM_999",
            auto_create_table=True,
            overwrite=True
        )
        print("Data successfully written to Snowflake!")
            
    except Exception as e:
        print(f"Error writing to Snowflake: {str(e)}")
            
    finally:
        session.close()

if __name__ == "__main__":
    write_to_snowflake()
