"""
Snowflake Configuration Module

This module provides a comprehensive configuration system for Snowflake connections.
It handles environment variables, validation, and provides easy setup for new users.

Author: Customer Analytics Team
Version: 1.0.0
"""

import os
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from pydantic import validator, Field
from pydantic_settings import BaseSettings


logger = logging.getLogger(__name__)


@dataclass
class SnowflakeCredentials:
    """
    Data class to hold Snowflake credentials with validation.
    """
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    
    def __post_init__(self):
        """Validate credentials after initialization."""
        if not all([self.account, self.user, self.password, self.warehouse, self.database, self.schema]):
            raise ValueError("All required Snowflake credentials must be provided")
        
        # Clean account identifier
        if not self.account.endswith('.snowflakecomputing.com'):
            if '.' not in self.account:
                raise ValueError("Account must include region (e.g., 'abc12345.us-east-1')")


class SnowflakeConfig(BaseSettings):
    """
    Snowflake configuration class using Pydantic for validation and environment variable loading.
    
    This class automatically loads configuration from environment variables and validates them.
    It provides a clean interface for Snowflake connection parameters.
    """
    
    # Core Snowflake Connection Parameters
    snowflake_account: str = Field(..., env='SNOWFLAKE_ACCOUNT', description="Snowflake account identifier")
    snowflake_user: str = Field(..., env='SNOWFLAKE_USER', description="Snowflake username")
    snowflake_password: str = Field(..., env='SNOWFLAKE_PASSWORD', description="Snowflake password")
    snowflake_warehouse: str = Field(..., env='SNOWFLAKE_WAREHOUSE', description="Snowflake warehouse name")
    snowflake_database: str = Field(..., env='SNOWFLAKE_DATABASE', description="Snowflake database name")
    snowflake_schema: str = Field(..., env='SNOWFLAKE_SCHEMA', description="Snowflake schema name")
    snowflake_role: Optional[str] = Field(None, env='SNOWFLAKE_ROLE', description="Snowflake role (optional)")
    
    # Connection Settings
    query_timeout: int = Field(300, env='QUERY_TIMEOUT', description="Query timeout in seconds")
    connection_pool_size: int = Field(10, env='CONNECTION_POOL_SIZE', description="Connection pool size")
    
    # Performance Settings
    max_query_rows: int = Field(10000, env='MAX_QUERY_ROWS', description="Maximum rows per query")
    cache_ttl: int = Field(3600, env='CACHE_TTL', description="Cache TTL in seconds")
    
    # Export Settings
    default_export_format: str = Field('csv', env='DEFAULT_EXPORT_FORMAT', description="Default export format")
    max_export_size_mb: int = Field(100, env='MAX_EXPORT_SIZE_MB', description="Maximum export size in MB")
    export_retention_days: int = Field(30, env='EXPORT_RETENTION_DAYS', description="Export file retention days")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from .env file
        
    @validator('snowflake_account')
    def validate_account(cls, v):
        """Validate Snowflake account format."""
        if not v:
            raise ValueError("Snowflake account is required")

        # Handle different account formats
        if v.endswith('.snowflakecomputing.com'):
            return v

        # If it contains a dot, assume it has region
        if '.' in v:
            return f"{v}.snowflakecomputing.com"

        # If it's just the account identifier without region, provide helpful error
        if len(v) > 5 and '-' in v:  # Looks like account identifier
            raise ValueError(
                f"Account '{v}' needs region. Common formats:\n"
                f"  - {v}.us-east-1.snowflakecomputing.com\n"
                f"  - {v}.us-west-2.snowflakecomputing.com\n"
                f"  - {v}.eu-west-1.snowflakecomputing.com\n"
                f"Check your Snowflake URL to find the correct region."
            )

        raise ValueError("Account must include region (e.g., 'abc12345.us-east-1')")

        return v
    
    @validator('default_export_format')
    def validate_export_format(cls, v):
        """Validate export format."""
        allowed_formats = ['csv', 'json', 'parquet', 'xlsx']
        if v.lower() not in allowed_formats:
            raise ValueError(f"Export format must be one of: {allowed_formats}")
        return v.lower()
    
    @validator('query_timeout', 'connection_pool_size', 'max_query_rows', 'cache_ttl', 'max_export_size_mb', 'export_retention_days')
    def validate_positive_integers(cls, v):
        """Validate that numeric settings are positive."""
        if v <= 0:
            raise ValueError("Value must be positive")
        return v
    
    def get_connection_params(self) -> Dict[str, Any]:
        """
        Get connection parameters formatted for Snowflake connector.
        
        Returns:
            Dict containing connection parameters for snowflake.connector.connect()
        """
        params = {
            'account': self.snowflake_account,
            'user': self.snowflake_user,
            'password': self.snowflake_password,
            'warehouse': self.snowflake_warehouse,
            'database': self.snowflake_database,
            'schema': self.snowflake_schema,
            'client_session_keep_alive': True,
            'network_timeout': self.query_timeout,
        }
        
        if self.snowflake_role:
            params['role'] = self.snowflake_role
            
        return params
    
    def get_sqlalchemy_url(self) -> str:
        """
        Get SQLAlchemy connection URL for Snowflake.
        
        Returns:
            SQLAlchemy connection string
        """
        base_url = f"snowflake://{self.snowflake_user}:{self.snowflake_password}@{self.snowflake_account}"
        params = f"/{self.snowflake_database}/{self.snowflake_schema}?warehouse={self.snowflake_warehouse}"
        
        if self.snowflake_role:
            params += f"&role={self.snowflake_role}"
            
        return base_url + params
    
    def validate_connection_params(self) -> bool:
        """
        Validate that all required connection parameters are present and valid.
        
        Returns:
            True if all parameters are valid
            
        Raises:
            ValueError: If any required parameter is missing or invalid
        """
        required_fields = [
            'snowflake_account', 'snowflake_user', 'snowflake_password',
            'snowflake_warehouse', 'snowflake_database', 'snowflake_schema'
        ]
        
        for field in required_fields:
            value = getattr(self, field)
            if not value or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Required field '{field}' is missing or empty")
        
        return True
    
    def to_credentials(self) -> SnowflakeCredentials:
        """
        Convert configuration to SnowflakeCredentials dataclass.
        
        Returns:
            SnowflakeCredentials instance
        """
        return SnowflakeCredentials(
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            role=self.snowflake_role
        )
    
    def mask_sensitive_data(self) -> Dict[str, Any]:
        """
        Get configuration dictionary with sensitive data masked for logging.
        
        Returns:
            Dictionary with masked sensitive fields
        """
        config_dict = self.dict()
        
        # Mask sensitive fields
        sensitive_fields = ['snowflake_password']
        for field in sensitive_fields:
            if field in config_dict and config_dict[field]:
                config_dict[field] = '*' * 8
                
        return config_dict


def load_snowflake_config() -> SnowflakeConfig:
    """
    Load and validate Snowflake configuration from environment variables.
    
    Returns:
        Validated SnowflakeConfig instance
        
    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If .env file is not found (in development)
    """
    try:
        config = SnowflakeConfig()
        config.validate_connection_params()
        
        logger.info("Snowflake configuration loaded successfully")
        logger.debug(f"Configuration: {config.mask_sensitive_data()}")
        
        return config
        
    except Exception as e:
        logger.error(f"Failed to load Snowflake configuration: {str(e)}")
        raise


def create_sample_env_file(file_path: str = ".env.snowflake.sample") -> None:
    """
    Create a sample environment file with Snowflake configuration template.
    
    Args:
        file_path: Path where to create the sample file
    """
    sample_content = """# Snowflake Configuration Sample
# Copy this to .env and fill in your actual values

# =============================================================================
# SNOWFLAKE CONNECTION SETTINGS
# =============================================================================
SNOWFLAKE_ACCOUNT=your_account.us-east-1.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_secure_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ANALYST_ROLE

# =============================================================================
# PERFORMANCE SETTINGS
# =============================================================================
QUERY_TIMEOUT=300
CONNECTION_POOL_SIZE=10
MAX_QUERY_ROWS=10000
CACHE_TTL=3600

# =============================================================================
# EXPORT SETTINGS
# =============================================================================
DEFAULT_EXPORT_FORMAT=csv
MAX_EXPORT_SIZE_MB=100
EXPORT_RETENTION_DAYS=30

# =============================================================================
# EXAMPLE VALUES
# =============================================================================
# SNOWFLAKE_ACCOUNT=abc12345.us-east-1.snowflakecomputing.com
# SNOWFLAKE_USER=analytics_user
# SNOWFLAKE_WAREHOUSE=ANALYTICS_WH
# SNOWFLAKE_DATABASE=CUSTOMER_DATA
# SNOWFLAKE_SCHEMA=ANALYTICS
"""
    
    with open(file_path, 'w') as f:
        f.write(sample_content)
    
    logger.info(f"Sample environment file created at: {file_path}")


if __name__ == "__main__":
    # Example usage and testing
    try:
        config = load_snowflake_config()
        print("✅ Snowflake configuration loaded successfully!")
        print(f"Account: {config.snowflake_account}")
        print(f"Database: {config.snowflake_database}")
        print(f"Schema: {config.snowflake_schema}")
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("\n💡 Creating sample configuration file...")
        create_sample_env_file()
        print("Please copy .env.snowflake.sample to .env and fill in your credentials.")
