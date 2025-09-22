from query_data import get_bonds_data
import pandas as pd

def transform_bonds_data():
    # Get data
    df = get_bonds_data()
    
    if df is not None:
        try:

            #copy of the original dataframe
            df = df.copy()
            # Convert date columns
            #df['DATES'] = pd.to_datetime(df['DATES'])
            
            # Replace '#N/A N/A' with NaN
            #df = df.replace('#N/A N/A', pd.NA)
            
            # Convert numeric columns
            #numeric_columns = df.select_dtypes(include=['object']).columns
            #for col in numeric_columns:
            #    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            print("\nTransformed data preview:")
            print(df.head())
            
            return df
            
        except Exception as e:
            print(f"Error transforming data: {str(e)}")
            return None

if __name__ == "__main__":
    transformed_df = transform_bonds_data()
