import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd

# Load environment variables
load_dotenv()

# Connection parameters
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("PROD"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# Create session
def create_session():
    try:
        session = Session.builder.configs(connection_parameters).create()
        print("Successfully connected to Snowflake!")
        return session
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        return None

