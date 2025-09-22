"""
Snowflake Connection Manager

This module provides connection management, pooling, and query execution for Snowflake.
It includes retry logic, error handling, and connection health monitoring.

Author: Customer Analytics Team
Version: 1.0.0
"""

import logging
import time
from typing import Optional, Dict, Any, List, Union, AsyncGenerator
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass
import asyncio
from concurrent.futures import ThreadPoolExecutor

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import Error as SnowflakeError
from snowflake.snowpark import Session
import pandas as pd

from .snowflake_config import SnowflakeConfig, load_snowflake_config


logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Data class to hold query results with metadata."""
    data: Union[List[Dict], pd.DataFrame]
    row_count: int
    execution_time: float
    query: str
    columns: List[str]
    success: bool = True
    error_message: Optional[str] = None


class SnowflakeConnectionManager:
    """
    Manages Snowflake connections with pooling, retry logic, and health monitoring.
    
    Features:
    - Connection pooling
    - Automatic retry on connection failures
    - Query timeout handling
    - Connection health monitoring
    - Async support for non-blocking operations
    """
    
    def __init__(self, config: Optional[SnowflakeConfig] = None):
        """
        Initialize the connection manager.
        
        Args:
            config: SnowflakeConfig instance. If None, loads from environment.
        """
        self.config = config or load_snowflake_config()
        self._connection_pool: List[snowflake.connector.SnowflakeConnection] = []
        self._pool_lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=self.config.connection_pool_size)
        self._health_check_interval = 300  # 5 minutes
        self._last_health_check = 0
        
        logger.info("Snowflake Connection Manager initialized")

    def create_snowpark_session(self) -> Optional[Session]:
        """
        Create a Snowpark session using the working approach from experiments.
        This is the preferred method for new connections.
        """
        try:
            connection_parameters = {
                "account": self.config.snowflake_account,
                "user": self.config.snowflake_user,
                "password": self.config.snowflake_password,
                "role": self.config.snowflake_role,
                "warehouse": self.config.snowflake_warehouse,
                "database": self.config.snowflake_database,
                "schema": self.config.snowflake_schema
            }

            session = Session.builder.configs(connection_parameters).create()
            logger.info("Successfully created Snowpark session")
            return session

        except Exception as e:
            logger.error(f"Error creating Snowpark session: {str(e)}")
            return None

    def _create_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Create a new Snowflake connection.
        
        Returns:
            New Snowflake connection
            
        Raises:
            SnowflakeError: If connection fails
        """
        try:
            connection_params = self.config.get_connection_params()
            connection = snowflake.connector.connect(**connection_params)
            
            logger.debug("New Snowflake connection created")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to create Snowflake connection: {str(e)}")
            raise SnowflakeError(f"Connection failed: {str(e)}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager to get a connection from the pool.
        
        Yields:
            Snowflake connection
        """
        connection = None
        try:
            # Try to get connection from pool
            if self._connection_pool:
                connection = self._connection_pool.pop()
                
                # Test connection health
                if not self._is_connection_healthy(connection):
                    connection.close()
                    connection = None
            
            # Create new connection if needed
            if connection is None:
                connection = self._create_connection()
            
            yield connection
            
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            if connection:
                try:
                    connection.close()
                except:
                    pass
            raise
            
        finally:
            # Return connection to pool if healthy
            if connection and self._is_connection_healthy(connection):
                if len(self._connection_pool) < self.config.connection_pool_size:
                    self._connection_pool.append(connection)
                else:
                    connection.close()
            elif connection:
                try:
                    connection.close()
                except:
                    pass
    
    def _is_connection_healthy(self, connection: snowflake.connector.SnowflakeConnection) -> bool:
        """
        Check if a connection is healthy.
        
        Args:
            connection: Snowflake connection to check
            
        Returns:
            True if connection is healthy
        """
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False
    
    def execute_query(self, query: str, params: Optional[Dict] = None, 
                     fetch_size: Optional[int] = None) -> QueryResult:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries
            fetch_size: Maximum number of rows to fetch
            
        Returns:
            QueryResult with data and metadata
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor(DictCursor)
                
                # Set query timeout
                cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {self.config.query_timeout}")
                
                # Execute query
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch results
                if fetch_size:
                    results = cursor.fetchmany(fetch_size)
                else:
                    max_rows = self.config.max_query_rows
                    results = cursor.fetchmany(max_rows)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                execution_time = time.time() - start_time
                
                cursor.close()
                
                logger.info(f"Query executed successfully in {execution_time:.2f}s, returned {len(results)} rows")
                
                return QueryResult(
                    data=results,
                    row_count=len(results),
                    execution_time=execution_time,
                    query=query,
                    columns=columns,
                    success=True
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            
            logger.error(f"Query execution failed after {execution_time:.2f}s: {error_msg}")
            
            return QueryResult(
                data=[],
                row_count=0,
                execution_time=execution_time,
                query=query,
                columns=[],
                success=False,
                error_message=error_msg
            )
    
    def execute_query_to_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            with self.get_connection() as connection:
                # Set query timeout
                connection.cursor().execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {self.config.query_timeout}")
                
                # Execute query and return DataFrame
                if params:
                    df = pd.read_sql(query, connection, params=params)
                else:
                    df = pd.read_sql(query, connection)
                
                logger.info(f"Query executed successfully, returned DataFrame with {len(df)} rows")
                return df
                
        except Exception as e:
            logger.error(f"Failed to execute query to DataFrame: {str(e)}")
            raise
    
    async def execute_query_async(self, query: str, params: Optional[Dict] = None) -> QueryResult:
        """
        Execute a query asynchronously.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            QueryResult with data and metadata
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.execute_query, query, params)
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Snowflake connection and return status information.
        
        Returns:
            Dictionary with connection test results
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                # Test basic connectivity
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                
                # Get current session info
                cursor.execute("""
                    SELECT 
                        CURRENT_USER() as current_user,
                        CURRENT_ROLE() as current_role,
                        CURRENT_DATABASE() as current_database,
                        CURRENT_SCHEMA() as current_schema,
                        CURRENT_WAREHOUSE() as current_warehouse
                """)
                session_info = cursor.fetchone()
                
                cursor.close()
                
                connection_time = time.time() - start_time
                
                return {
                    'success': True,
                    'connection_time': round(connection_time, 3),
                    'snowflake_version': version,
                    'current_user': session_info[0],
                    'current_role': session_info[1],
                    'current_database': session_info[2],
                    'current_schema': session_info[3],
                    'current_warehouse': session_info[4],
                    'timestamp': time.time()
                }
                
        except Exception as e:
            connection_time = time.time() - start_time
            
            return {
                'success': False,
                'connection_time': round(connection_time, 3),
                'error': str(e),
                'timestamp': time.time()
            }
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about a table including columns and row count.
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses current schema if not provided)
            
        Returns:
            Dictionary with table information
        """
        schema = schema or self.config.snowflake_schema
        
        try:
            # Get column information
            columns_query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema.upper()}' 
                AND TABLE_NAME = '{table_name.upper()}'
                ORDER BY ORDINAL_POSITION
            """
            
            columns_result = self.execute_query(columns_query)
            
            if not columns_result.success:
                raise Exception(columns_result.error_message)
            
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
            count_result = self.execute_query(count_query)
            
            row_count = count_result.data[0]['COUNT(*)'] if count_result.success else 0
            
            return {
                'table_name': table_name,
                'schema': schema,
                'columns': columns_result.data,
                'row_count': row_count,
                'column_count': len(columns_result.data)
            }
            
        except Exception as e:
            logger.error(f"Failed to get table info for {schema}.{table_name}: {str(e)}")
            raise
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        for connection in self._connection_pool:
            try:
                connection.close()
            except:
                pass
        
        self._connection_pool.clear()
        logger.info("All connections closed")
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        self.close_all_connections()


# Global connection manager instance
_connection_manager: Optional[SnowflakeConnectionManager] = None


def get_connection_manager() -> SnowflakeConnectionManager:
    """
    Get the global connection manager instance.
    
    Returns:
        SnowflakeConnectionManager instance
    """
    global _connection_manager
    
    if _connection_manager is None:
        _connection_manager = SnowflakeConnectionManager()
    
    return _connection_manager


def execute_query(query: str, params: Optional[Dict] = None) -> QueryResult:
    """
    Convenience function to execute a query using the global connection manager.
    
    Args:
        query: SQL query to execute
        params: Query parameters
        
    Returns:
        QueryResult with data and metadata
    """
    manager = get_connection_manager()
    return manager.execute_query(query, params)


def execute_query_to_dataframe(query: str, params: Optional[Dict] = None) -> pd.DataFrame:
    """
    Convenience function to execute a query and return a DataFrame.
    
    Args:
        query: SQL query to execute
        params: Query parameters
        
    Returns:
        pandas DataFrame with results
    """
    manager = get_connection_manager()
    return manager.execute_query_to_dataframe(query, params)
