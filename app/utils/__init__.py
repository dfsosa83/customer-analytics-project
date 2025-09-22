"""
Utility functions and helpers for the customer analytics application.

This package provides comprehensive Snowflake connectivity, data export,
and validation utilities for the Customer Analytics Project.
"""

# Snowflake Configuration and Connection
from .snowflake_config import (
    SnowflakeConfig,
    SnowflakeCredentials,
    load_snowflake_config,
    create_sample_env_file
)

from .snowflake_connection import (
    SnowflakeConnectionManager,
    QueryResult,
    get_connection_manager,
    execute_query,
    execute_query_to_dataframe
)

# Data Export Utilities
from .data_export import (
    DataExporter,
    get_data_exporter,
    export_query,
    export_table
)

# Validation Utilities
from .snowflake_validator import (
    SnowflakeValidator,
    ValidationLevel,
    ValidationResult,
    ValidationReport,
    validate_snowflake_setup,
    print_validation_report
)

__version__ = "1.0.0"

__all__ = [
    # Configuration
    "SnowflakeConfig",
    "SnowflakeCredentials",
    "load_snowflake_config",
    "create_sample_env_file",

    # Connection Management
    "SnowflakeConnectionManager",
    "QueryResult",
    "get_connection_manager",
    "execute_query",
    "execute_query_to_dataframe",

    # Data Export
    "DataExporter",
    "get_data_exporter",
    "export_query",
    "export_table",

    # Validation
    "SnowflakeValidator",
    "ValidationLevel",
    "ValidationResult",
    "ValidationReport",
    "validate_snowflake_setup",
    "print_validation_report"
]
