"""
Snowflake API Router

This module provides FastAPI endpoints for Snowflake operations including
querying, data export, and connection management.

Author: Customer Analytics Team
Version: 1.0.0
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, validator

from ..utils.snowflake_connection import get_connection_manager, SnowflakeConnectionManager
from ..utils.data_export import get_data_exporter, DataExporter


logger = logging.getLogger(__name__)

# Create router (no prefix here since it's added in main.py)
router = APIRouter(tags=["snowflake"])


# Pydantic models for request/response
class QueryRequest(BaseModel):
    """Request model for SQL query execution."""
    query: str = Field(..., description="SQL query to execute", min_length=1)
    params: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    limit: Optional[int] = Field(None, description="Maximum number of rows to return", ge=1, le=10000)
    
    @validator('query')
    def validate_query(cls, v):
        """Basic query validation."""
        v = v.strip()
        if not v:
            raise ValueError("Query cannot be empty")
        
        # Block potentially dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        query_upper = v.upper()
        
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                raise ValueError(f"Query contains potentially dangerous keyword: {keyword}")
        
        return v


class QueryResponse(BaseModel):
    """Response model for query execution."""
    success: bool
    data: List[Dict[str, Any]]
    row_count: int
    execution_time: float
    columns: List[str]
    query: str
    timestamp: datetime
    error_message: Optional[str] = None


class ExportRequest(BaseModel):
    """Request model for data export."""
    query: str = Field(..., description="SQL query to execute for export")
    format: str = Field("csv", description="Export format (csv, json, parquet, xlsx)")
    filename: Optional[str] = Field(None, description="Custom filename for export")
    params: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    
    @validator('format')
    def validate_format(cls, v):
        """Validate export format."""
        allowed_formats = ['csv', 'json', 'parquet', 'xlsx']
        if v.lower() not in allowed_formats:
            raise ValueError(f"Format must be one of: {allowed_formats}")
        return v.lower()


class TableExportRequest(BaseModel):
    """Request model for table export."""
    table_name: str = Field(..., description="Name of the table to export")
    schema: Optional[str] = Field(None, description="Schema name (uses current if not provided)")
    format: str = Field("csv", description="Export format")
    filename: Optional[str] = Field(None, description="Custom filename")
    limit: Optional[int] = Field(None, description="Maximum number of rows to export", ge=1)
    
    @validator('format')
    def validate_format(cls, v):
        allowed_formats = ['csv', 'json', 'parquet', 'xlsx']
        if v.lower() not in allowed_formats:
            raise ValueError(f"Format must be one of: {allowed_formats}")
        return v.lower()


class ConnectionStatus(BaseModel):
    """Response model for connection status."""
    success: bool
    connection_time: float
    snowflake_version: Optional[str] = None
    current_user: Optional[str] = None
    current_role: Optional[str] = None
    current_database: Optional[str] = None
    current_schema: Optional[str] = None
    current_warehouse: Optional[str] = None
    error: Optional[str] = None
    timestamp: float


class TableInfo(BaseModel):
    """Response model for table information."""
    table_name: str
    schema: str
    columns: List[Dict[str, Any]]
    row_count: int
    column_count: int


# Dependency to get connection manager
def get_connection_manager_dep() -> SnowflakeConnectionManager:
    """Dependency to get connection manager."""
    return get_connection_manager()


# Dependency to get data exporter
def get_data_exporter_dep() -> DataExporter:
    """Dependency to get data exporter."""
    return get_data_exporter()


@router.get("/health", response_model=ConnectionStatus)
async def health_check(
    connection_manager: SnowflakeConnectionManager = Depends(get_connection_manager_dep)
):
    """
    Check Snowflake connection health and return status information.
    
    Returns:
        Connection status with session information
    """
    try:
        status = connection_manager.test_connection()
        return ConnectionStatus(**status)
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Snowflake connection failed: {str(e)}")


@router.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    connection_manager: SnowflakeConnectionManager = Depends(get_connection_manager_dep)
):
    """
    Execute a SQL query and return results.
    
    Args:
        request: Query request with SQL and parameters
        
    Returns:
        Query results with metadata
    """
    try:
        # Add limit to query if specified
        query = request.query
        if request.limit:
            query = f"SELECT * FROM ({query}) LIMIT {request.limit}"
        
        result = connection_manager.execute_query_snowpark(query, request.params)
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_message)
        
        return QueryResponse(
            success=result.success,
            data=result.data,
            row_count=result.row_count,
            execution_time=result.execution_time,
            columns=result.columns,
            query=result.query,
            timestamp=datetime.now(),
            error_message=result.error_message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


@router.post("/export")
async def export_data(
    request: ExportRequest,
    data_exporter: DataExporter = Depends(get_data_exporter_dep)
):
    """
    Export query results as a downloadable file.
    
    Args:
        request: Export request with query and format
        
    Returns:
        StreamingResponse with the exported file
    """
    try:
        return data_exporter.export_query_results(
            query=request.query,
            format=request.format,
            filename=request.filename,
            params=request.params
        )
    except Exception as e:
        logger.error(f"Data export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.post("/export/table")
async def export_table(
    request: TableExportRequest,
    data_exporter: DataExporter = Depends(get_data_exporter_dep)
):
    """
    Export an entire table as a downloadable file.
    
    Args:
        request: Table export request
        
    Returns:
        StreamingResponse with the exported file
    """
    try:
        return data_exporter.export_table(
            table_name=request.table_name,
            schema=request.schema,
            format=request.format,
            filename=request.filename,
            limit=request.limit
        )
    except Exception as e:
        logger.error(f"Table export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Table export failed: {str(e)}")


@router.post("/export/preview")
async def export_preview(
    request: QueryRequest,
    data_exporter: DataExporter = Depends(get_data_exporter_dep)
):
    """
    Get a preview of query results for export validation.
    
    Args:
        request: Query request
        
    Returns:
        Preview data with export information
    """
    try:
        preview = data_exporter.get_export_preview(
            query=request.query,
            params=request.params,
            limit=request.limit or 100
        )
        
        if not preview['success']:
            raise HTTPException(status_code=400, detail=preview['error'])
        
        return preview
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Export preview failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")


@router.get("/tables")
async def list_tables(
    schema: Optional[str] = Query(None, description="Schema name (uses current if not provided)"),
    connection_manager: SnowflakeConnectionManager = Depends(get_connection_manager_dep)
):
    """
    List all tables in the specified schema.
    
    Args:
        schema: Schema name
        
    Returns:
        List of tables with basic information
    """
    try:
        schema = schema or connection_manager.config.snowflake_schema
        
        query = f"""
            SELECT 
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                BYTES,
                CREATED,
                LAST_ALTERED
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema.upper()}'
            ORDER BY TABLE_NAME
        """
        
        result = connection_manager.execute_query_snowpark(query)
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_message)
        
        return {
            'success': True,
            'schema': schema,
            'tables': result.data,
            'table_count': result.row_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


@router.get("/tables/{table_name}", response_model=TableInfo)
async def get_table_info(
    table_name: str,
    schema: Optional[str] = Query(None, description="Schema name"),
    connection_manager: SnowflakeConnectionManager = Depends(get_connection_manager_dep)
):
    """
    Get detailed information about a specific table.
    
    Args:
        table_name: Name of the table
        schema: Schema name
        
    Returns:
        Detailed table information including columns and row count
    """
    try:
        table_info = connection_manager.get_table_info(table_name, schema)
        return TableInfo(**table_info)
        
    except Exception as e:
        logger.error(f"Failed to get table info: {str(e)}")
        raise HTTPException(status_code=404, detail=f"Table not found or access denied: {str(e)}")


@router.get("/schemas")
async def list_schemas(
    connection_manager: SnowflakeConnectionManager = Depends(get_connection_manager_dep)
):
    """
    List all schemas in the current database.
    
    Returns:
        List of schemas
    """
    try:
        query = """
            SELECT 
                SCHEMA_NAME,
                SCHEMA_OWNER,
                CREATED,
                LAST_ALTERED
            FROM INFORMATION_SCHEMA.SCHEMATA 
            ORDER BY SCHEMA_NAME
        """
        
        result = connection_manager.execute_query_snowpark(query)
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_message)
        
        return {
            'success': True,
            'database': connection_manager.config.snowflake_database,
            'schemas': result.data,
            'schema_count': result.row_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list schemas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(e)}")


@router.get("/config")
async def get_configuration(
    connection_manager: SnowflakeConnectionManager = Depends(get_connection_manager_dep)
):
    """
    Get current Snowflake configuration (with sensitive data masked).
    
    Returns:
        Configuration information
    """
    try:
        config_info = connection_manager.config.mask_sensitive_data()
        
        return {
            'success': True,
            'configuration': config_info,
            'supported_export_formats': ['csv', 'json', 'parquet', 'xlsx']
        }
        
    except Exception as e:
        logger.error(f"Failed to get configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get configuration: {str(e)}")
