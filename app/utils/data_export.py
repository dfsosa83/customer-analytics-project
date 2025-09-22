"""
Data Export Utilities

This module provides utilities for exporting data from Snowflake in various formats
with streaming support for large datasets.

Supported formats:
- CSV
- JSON
- Parquet
- Excel (XLSX)

Author: Customer Analytics Team
Version: 1.0.0
"""

import os
import io
import json
import logging
import tempfile
from typing import Optional, Dict, Any, Union, Generator, BinaryIO
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from .snowflake_connection import SnowflakeConnectionManager, get_connection_manager
from .snowflake_config import SnowflakeConfig


logger = logging.getLogger(__name__)


class DataExporter:
    """
    Handles data export operations with support for multiple formats and streaming.
    """
    
    def __init__(self, connection_manager: Optional[SnowflakeConnectionManager] = None):
        """
        Initialize the data exporter.
        
        Args:
            connection_manager: SnowflakeConnectionManager instance
        """
        self.connection_manager = connection_manager or get_connection_manager()
        self.config = self.connection_manager.config
        
        # Supported export formats
        self.supported_formats = ['csv', 'json', 'parquet', 'xlsx']
        
        # MIME types for different formats
        self.mime_types = {
            'csv': 'text/csv',
            'json': 'application/json',
            'parquet': 'application/octet-stream',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
    
    def export_query_results(self, query: str, format: str = 'csv', 
                           filename: Optional[str] = None,
                           params: Optional[Dict] = None) -> StreamingResponse:
        """
        Export query results in the specified format as a streaming response.
        
        Args:
            query: SQL query to execute
            format: Export format (csv, json, parquet, xlsx)
            filename: Custom filename for the export
            params: Query parameters
            
        Returns:
            FastAPI StreamingResponse for file download
        """
        format = format.lower()
        
        if format not in self.supported_formats:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Supported formats: {self.supported_formats}"
            )
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_{timestamp}.{format}"
        elif not filename.endswith(f".{format}"):
            filename = f"{filename}.{format}"
        
        try:
            # Execute query and get DataFrame
            df = self.connection_manager.execute_query_to_dataframe(query, params)
            
            # Check export size limit
            self._check_export_size_limit(df)
            
            # Generate streaming response based on format
            if format == 'csv':
                return self._stream_csv(df, filename)
            elif format == 'json':
                return self._stream_json(df, filename)
            elif format == 'parquet':
                return self._stream_parquet(df, filename)
            elif format == 'xlsx':
                return self._stream_xlsx(df, filename)
                
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
    
    def _check_export_size_limit(self, df: pd.DataFrame) -> None:
        """
        Check if the DataFrame size exceeds the export limit.
        
        Args:
            df: DataFrame to check
            
        Raises:
            HTTPException: If size limit is exceeded
        """
        # Estimate size in MB (rough calculation)
        estimated_size_mb = (df.memory_usage(deep=True).sum()) / (1024 * 1024)
        
        if estimated_size_mb > self.config.max_export_size_mb:
            raise HTTPException(
                status_code=413,
                detail=f"Export size ({estimated_size_mb:.1f} MB) exceeds limit ({self.config.max_export_size_mb} MB)"
            )
    
    def _stream_csv(self, df: pd.DataFrame, filename: str) -> StreamingResponse:
        """Stream DataFrame as CSV."""
        def generate():
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            yield buffer.getvalue().encode('utf-8')
        
        return StreamingResponse(
            generate(),
            media_type=self.mime_types['csv'],
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    def _stream_json(self, df: pd.DataFrame, filename: str) -> StreamingResponse:
        """Stream DataFrame as JSON."""
        def generate():
            # Convert DataFrame to JSON with proper handling of datetime and NaN
            json_str = df.to_json(orient='records', date_format='iso', default_handler=str)
            yield json_str.encode('utf-8')
        
        return StreamingResponse(
            generate(),
            media_type=self.mime_types['json'],
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    def _stream_parquet(self, df: pd.DataFrame, filename: str) -> StreamingResponse:
        """Stream DataFrame as Parquet."""
        def generate():
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            yield buffer.getvalue()
        
        return StreamingResponse(
            generate(),
            media_type=self.mime_types['parquet'],
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    def _stream_xlsx(self, df: pd.DataFrame, filename: str) -> StreamingResponse:
        """Stream DataFrame as Excel file."""
        def generate():
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Data')
            buffer.seek(0)
            yield buffer.getvalue()
        
        return StreamingResponse(
            generate(),
            media_type=self.mime_types['xlsx'],
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    def save_query_results_to_file(self, query: str, file_path: str, 
                                  format: Optional[str] = None,
                                  params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Save query results to a local file.
        
        Args:
            query: SQL query to execute
            file_path: Path where to save the file
            format: Export format (auto-detected from file extension if not provided)
            params: Query parameters
            
        Returns:
            Dictionary with export information
        """
        # Auto-detect format from file extension
        if not format:
            format = Path(file_path).suffix.lower().lstrip('.')
            
        if format not in self.supported_formats:
            raise ValueError(f"Unsupported format '{format}'. Supported formats: {self.supported_formats}")
        
        try:
            # Execute query and get DataFrame
            df = self.connection_manager.execute_query_to_dataframe(query, params)
            
            # Check export size limit
            self._check_export_size_limit(df)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Save based on format
            if format == 'csv':
                df.to_csv(file_path, index=False)
            elif format == 'json':
                df.to_json(file_path, orient='records', date_format='iso', indent=2)
            elif format == 'parquet':
                df.to_parquet(file_path, index=False, engine='pyarrow')
            elif format == 'xlsx':
                df.to_excel(file_path, index=False, sheet_name='Data')
            
            # Get file info
            file_size = os.path.getsize(file_path)
            
            logger.info(f"Data exported successfully to {file_path} ({file_size} bytes)")
            
            return {
                'success': True,
                'file_path': file_path,
                'format': format,
                'row_count': len(df),
                'column_count': len(df.columns),
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to save query results to file: {str(e)}")
            raise
    
    def export_table(self, table_name: str, schema: Optional[str] = None,
                    format: str = 'csv', filename: Optional[str] = None,
                    limit: Optional[int] = None) -> StreamingResponse:
        """
        Export an entire table in the specified format.
        
        Args:
            table_name: Name of the table to export
            schema: Schema name (uses current schema if not provided)
            format: Export format
            filename: Custom filename
            limit: Maximum number of rows to export
            
        Returns:
            StreamingResponse for file download
        """
        schema = schema or self.config.snowflake_schema
        
        # Build query
        query = f"SELECT * FROM {schema}.{table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{table_name}_{timestamp}"
        
        return self.export_query_results(query, format, filename)
    
    def get_export_preview(self, query: str, params: Optional[Dict] = None,
                          limit: int = 100) -> Dict[str, Any]:
        """
        Get a preview of query results for export validation.
        
        Args:
            query: SQL query to preview
            params: Query parameters
            limit: Number of rows to preview
            
        Returns:
            Dictionary with preview information
        """
        try:
            # Add limit to query for preview
            preview_query = f"SELECT * FROM ({query}) LIMIT {limit}"
            
            result = self.connection_manager.execute_query(preview_query, params)
            
            if not result.success:
                raise Exception(result.error_message)
            
            # Estimate full result size
            count_query = f"SELECT COUNT(*) as total_rows FROM ({query})"
            count_result = self.connection_manager.execute_query(count_query, params)
            
            total_rows = 0
            if count_result.success and count_result.data:
                total_rows = count_result.data[0].get('TOTAL_ROWS', 0)
            
            return {
                'success': True,
                'preview_data': result.data,
                'preview_row_count': result.row_count,
                'total_estimated_rows': total_rows,
                'columns': result.columns,
                'column_count': len(result.columns),
                'execution_time': result.execution_time,
                'supported_formats': self.supported_formats
            }
            
        except Exception as e:
            logger.error(f"Failed to get export preview: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'supported_formats': self.supported_formats
            }


# Global exporter instance
_data_exporter: Optional[DataExporter] = None


def get_data_exporter() -> DataExporter:
    """
    Get the global data exporter instance.
    
    Returns:
        DataExporter instance
    """
    global _data_exporter
    
    if _data_exporter is None:
        _data_exporter = DataExporter()
    
    return _data_exporter


def export_query(query: str, format: str = 'csv', filename: Optional[str] = None,
                params: Optional[Dict] = None) -> StreamingResponse:
    """
    Convenience function to export query results.
    
    Args:
        query: SQL query to execute
        format: Export format
        filename: Custom filename
        params: Query parameters
        
    Returns:
        StreamingResponse for file download
    """
    exporter = get_data_exporter()
    return exporter.export_query_results(query, format, filename, params)


def export_table(table_name: str, schema: Optional[str] = None,
                format: str = 'csv', filename: Optional[str] = None,
                limit: Optional[int] = None) -> StreamingResponse:
    """
    Convenience function to export a table.
    
    Args:
        table_name: Name of the table to export
        schema: Schema name
        format: Export format
        filename: Custom filename
        limit: Maximum number of rows
        
    Returns:
        StreamingResponse for file download
    """
    exporter = get_data_exporter()
    return exporter.export_table(table_name, schema, format, filename, limit)
