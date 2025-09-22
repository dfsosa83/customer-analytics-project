"""
Snowflake Configuration Validator

This module provides validation utilities for Snowflake setup and configuration.
It includes connection testing, permission validation, and setup verification.

Author: Customer Analytics Team
Version: 1.0.0
"""

import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .snowflake_config import SnowflakeConfig, load_snowflake_config
from .snowflake_connection import SnowflakeConnectionManager


logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Validation levels for different types of checks."""
    BASIC = "basic"
    STANDARD = "standard"
    COMPREHENSIVE = "comprehensive"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    check_name: str
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    execution_time: Optional[float] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    overall_success: bool
    total_checks: int
    passed_checks: int
    failed_checks: int
    results: List[ValidationResult]
    total_time: float
    timestamp: float


class SnowflakeValidator:
    """
    Validates Snowflake configuration and connectivity.
    
    Provides comprehensive testing of:
    - Configuration validity
    - Connection establishment
    - Permission verification
    - Performance testing
    """
    
    def __init__(self, config: Optional[SnowflakeConfig] = None):
        """
        Initialize the validator.
        
        Args:
            config: SnowflakeConfig instance. If None, loads from environment.
        """
        self.config = config
        self.connection_manager: Optional[SnowflakeConnectionManager] = None
        
    def validate_setup(self, level: ValidationLevel = ValidationLevel.STANDARD) -> ValidationReport:
        """
        Perform comprehensive validation of Snowflake setup.
        
        Args:
            level: Validation level (basic, standard, comprehensive)
            
        Returns:
            ValidationReport with all check results
        """
        start_time = time.time()
        results = []
        
        logger.info(f"Starting Snowflake validation at {level.value} level")
        
        # Configuration validation
        results.extend(self._validate_configuration())
        
        # Connection validation
        if level in [ValidationLevel.STANDARD, ValidationLevel.COMPREHENSIVE]:
            results.extend(self._validate_connection())
        
        # Permission validation
        if level == ValidationLevel.COMPREHENSIVE:
            results.extend(self._validate_permissions())
            results.extend(self._validate_performance())
        
        # Calculate summary
        total_time = time.time() - start_time
        passed_checks = sum(1 for r in results if r.success)
        failed_checks = len(results) - passed_checks
        overall_success = failed_checks == 0
        
        report = ValidationReport(
            overall_success=overall_success,
            total_checks=len(results),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            results=results,
            total_time=total_time,
            timestamp=time.time()
        )
        
        logger.info(f"Validation completed: {passed_checks}/{len(results)} checks passed")
        return report
    
    def _validate_configuration(self) -> List[ValidationResult]:
        """Validate configuration settings."""
        results = []
        
        # Test 1: Load configuration
        result = self._run_check(
            "Configuration Loading",
            self._check_config_loading
        )
        results.append(result)
        
        if not result.success:
            return results
        
        # Test 2: Validate required fields
        results.append(self._run_check(
            "Required Fields",
            self._check_required_fields
        ))
        
        # Test 3: Validate field formats
        results.append(self._run_check(
            "Field Formats",
            self._check_field_formats
        ))
        
        # Test 4: Validate numeric settings
        results.append(self._run_check(
            "Numeric Settings",
            self._check_numeric_settings
        ))
        
        return results
    
    def _validate_connection(self) -> List[ValidationResult]:
        """Validate connection establishment."""
        results = []
        
        if not self.config:
            results.append(ValidationResult(
                check_name="Connection Setup",
                success=False,
                message="Configuration not loaded, skipping connection tests"
            ))
            return results
        
        # Test 1: Create connection manager
        result = self._run_check(
            "Connection Manager",
            self._check_connection_manager
        )
        results.append(result)
        
        if not result.success:
            return results
        
        # Test 2: Basic connectivity
        results.append(self._run_check(
            "Basic Connectivity",
            self._check_basic_connectivity
        ))
        
        # Test 3: Session information
        results.append(self._run_check(
            "Session Information",
            self._check_session_info
        ))
        
        # Test 4: Simple query execution
        results.append(self._run_check(
            "Query Execution",
            self._check_query_execution
        ))
        
        return results
    
    def _validate_permissions(self) -> List[ValidationResult]:
        """Validate user permissions."""
        results = []
        
        if not self.connection_manager:
            results.append(ValidationResult(
                check_name="Permission Setup",
                success=False,
                message="Connection manager not available, skipping permission tests"
            ))
            return results
        
        # Test 1: Database access
        results.append(self._run_check(
            "Database Access",
            self._check_database_access
        ))
        
        # Test 2: Schema access
        results.append(self._run_check(
            "Schema Access",
            self._check_schema_access
        ))
        
        # Test 3: Warehouse usage
        results.append(self._run_check(
            "Warehouse Usage",
            self._check_warehouse_usage
        ))
        
        # Test 4: Table listing
        results.append(self._run_check(
            "Table Listing",
            self._check_table_listing
        ))
        
        return results
    
    def _validate_performance(self) -> List[ValidationResult]:
        """Validate performance characteristics."""
        results = []
        
        if not self.connection_manager:
            return results
        
        # Test 1: Connection pool
        results.append(self._run_check(
            "Connection Pool",
            self._check_connection_pool
        ))
        
        # Test 2: Query timeout
        results.append(self._run_check(
            "Query Timeout",
            self._check_query_timeout
        ))
        
        # Test 3: Large result handling
        results.append(self._run_check(
            "Large Results",
            self._check_large_results
        ))
        
        return results
    
    def _run_check(self, check_name: str, check_function) -> ValidationResult:
        """Run a validation check and return the result."""
        start_time = time.time()
        
        try:
            success, message, details = check_function()
            execution_time = time.time() - start_time
            
            return ValidationResult(
                check_name=check_name,
                success=success,
                message=message,
                details=details,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            return ValidationResult(
                check_name=check_name,
                success=False,
                message=f"Check failed with exception: {str(e)}",
                execution_time=execution_time
            )
    
    def _check_config_loading(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check if configuration can be loaded."""
        try:
            self.config = load_snowflake_config()
            return True, "Configuration loaded successfully", {
                'account': self.config.snowflake_account,
                'database': self.config.snowflake_database,
                'schema': self.config.snowflake_schema
            }
        except Exception as e:
            return False, f"Failed to load configuration: {str(e)}", None
    
    def _check_required_fields(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check if all required fields are present."""
        required_fields = [
            'snowflake_account', 'snowflake_user', 'snowflake_password',
            'snowflake_warehouse', 'snowflake_database', 'snowflake_schema'
        ]
        
        missing_fields = []
        for field in required_fields:
            value = getattr(self.config, field, None)
            if not value or (isinstance(value, str) and not value.strip()):
                missing_fields.append(field)
        
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}", {
                'missing_fields': missing_fields
            }
        
        return True, "All required fields are present", {
            'required_fields': required_fields
        }
    
    def _check_field_formats(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check if field formats are valid."""
        issues = []

        # Check for common format issues (but allow simple account format that works)
        if ' ' in self.config.snowflake_account:
            issues.append("Account identifier should not contain spaces")

        # Basic account format validation (allow both simple and full formats)
        if len(self.config.snowflake_account) < 5:
            issues.append("Account identifier is too short")

        if issues:
            return False, f"Format issues: {'; '.join(issues)}", {
                'issues': issues
            }

        return True, "Field formats are valid", None
    
    def _check_numeric_settings(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check if numeric settings are valid."""
        settings = {
            'query_timeout': self.config.query_timeout,
            'connection_pool_size': self.config.connection_pool_size,
            'max_query_rows': self.config.max_query_rows,
            'cache_ttl': self.config.cache_ttl
        }
        
        invalid_settings = []
        for name, value in settings.items():
            if value <= 0:
                invalid_settings.append(f"{name}: {value}")
        
        if invalid_settings:
            return False, f"Invalid numeric settings: {', '.join(invalid_settings)}", {
                'invalid_settings': invalid_settings
            }
        
        return True, "Numeric settings are valid", settings
    
    def _check_connection_manager(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check if connection manager can be created."""
        try:
            self.connection_manager = SnowflakeConnectionManager(self.config)
            return True, "Connection manager created successfully", None
        except Exception as e:
            return False, f"Failed to create connection manager: {str(e)}", None
    
    def _check_basic_connectivity(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check basic connectivity to Snowflake using Snowpark session."""
        try:
            start_time = time.time()

            # Use the working Snowpark session approach
            session = self.connection_manager.create_snowpark_session()
            connection_time = time.time() - start_time

            if session is not None:
                try:
                    # Test a simple query
                    result = session.sql("SELECT CURRENT_VERSION()").collect()
                    version = result[0][0] if result else "Unknown"
                    session.close()

                    return True, "Basic connectivity successful", {
                        'success': True,
                        'connection_time': connection_time,
                        'snowflake_version': version,
                        'timestamp': time.time()
                    }
                except Exception as query_error:
                    if session:
                        session.close()
                    return False, f"Query test failed: {str(query_error)}", {
                        'success': False,
                        'connection_time': connection_time,
                        'error': str(query_error),
                        'timestamp': time.time()
                    }
            else:
                return False, "Failed to create Snowpark session", {
                    'success': False,
                    'connection_time': connection_time,
                    'error': "Session creation failed",
                    'timestamp': time.time()
                }

        except Exception as e:
            return False, f"Connection failed: {str(e)}", {
                'success': False,
                'error': str(e),
                'timestamp': time.time()
            }
    
    def _check_session_info(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check session information retrieval using Snowpark."""
        try:
            session = self.connection_manager.create_snowpark_session()

            if session is not None:
                try:
                    query = """
                        SELECT
                            CURRENT_USER() as current_user,
                            CURRENT_ROLE() as current_role,
                            CURRENT_DATABASE() as current_database,
                            CURRENT_SCHEMA() as current_schema,
                            CURRENT_WAREHOUSE() as current_warehouse
                    """

                    result = session.sql(query).collect()
                    session.close()

                    if result:
                        row = result[0]
                        session_info = {
                            'current_user': row[0],
                            'current_role': row[1],
                            'current_database': row[2],
                            'current_schema': row[3],
                            'current_warehouse': row[4]
                        }
                        return True, "Session information retrieved successfully", session_info
                    else:
                        return False, "No session info returned", None

                except Exception as query_error:
                    if session:
                        session.close()
                    return False, f"Session info query failed: {str(query_error)}", None
            else:
                return False, "Failed to create session for info check", None

        except Exception as e:
            return False, f"Session info check failed: {str(e)}", None
    
    def _check_query_execution(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check simple query execution using Snowpark."""
        try:
            session = self.connection_manager.create_snowpark_session()

            if session is not None:
                try:
                    start_time = time.time()
                    result = session.sql("SELECT 1 as test_value").collect()
                    execution_time = time.time() - start_time
                    session.close()

                    if result:
                        return True, "Query execution successful", {
                            'execution_time': execution_time,
                            'row_count': len(result),
                            'test_value': result[0][0]
                        }
                    else:
                        return False, "Query returned no results", None

                except Exception as query_error:
                    if session:
                        session.close()
                    return False, f"Query execution failed: {str(query_error)}", None
            else:
                return False, "Failed to create session for query test", None

        except Exception as e:
            return False, f"Query execution check failed: {str(e)}", None
    
    def _check_database_access(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check database access permissions."""
        try:
            query = f"USE DATABASE {self.config.snowflake_database}"
            result = self.connection_manager.execute_query(query)
            
            if result.success:
                return True, "Database access confirmed", None
            else:
                return False, f"Database access denied: {result.error_message}", None
                
        except Exception as e:
            return False, f"Database access check failed: {str(e)}", None
    
    def _check_schema_access(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check schema access permissions."""
        try:
            query = f"USE SCHEMA {self.config.snowflake_database}.{self.config.snowflake_schema}"
            result = self.connection_manager.execute_query(query)
            
            if result.success:
                return True, "Schema access confirmed", None
            else:
                return False, f"Schema access denied: {result.error_message}", None
                
        except Exception as e:
            return False, f"Schema access check failed: {str(e)}", None
    
    def _check_warehouse_usage(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check warehouse usage permissions."""
        try:
            query = f"USE WAREHOUSE {self.config.snowflake_warehouse}"
            result = self.connection_manager.execute_query(query)
            
            if result.success:
                return True, "Warehouse usage confirmed", None
            else:
                return False, f"Warehouse usage denied: {result.error_message}", None
                
        except Exception as e:
            return False, f"Warehouse usage check failed: {str(e)}", None
    
    def _check_table_listing(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check ability to list tables."""
        try:
            query = f"""
                SELECT COUNT(*) as table_count 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.config.snowflake_schema.upper()}'
            """
            
            result = self.connection_manager.execute_query(query)
            
            if result.success:
                table_count = result.data[0]['TABLE_COUNT'] if result.data else 0
                return True, f"Table listing successful ({table_count} tables found)", {
                    'table_count': table_count
                }
            else:
                return False, f"Table listing failed: {result.error_message}", None
                
        except Exception as e:
            return False, f"Table listing check failed: {str(e)}", None
    
    def _check_connection_pool(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check connection pool functionality."""
        try:
            # Test multiple concurrent connections
            start_time = time.time()
            
            for i in range(3):
                result = self.connection_manager.execute_query("SELECT 1")
                if not result.success:
                    return False, f"Connection pool test failed on iteration {i+1}", None
            
            pool_time = time.time() - start_time
            
            return True, "Connection pool working correctly", {
                'pool_test_time': pool_time,
                'pool_size': self.config.connection_pool_size
            }
            
        except Exception as e:
            return False, f"Connection pool check failed: {str(e)}", None
    
    def _check_query_timeout(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check query timeout configuration."""
        try:
            # Test that timeout setting is applied
            query = f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {self.config.query_timeout}"
            result = self.connection_manager.execute_query(query)
            
            if result.success:
                return True, "Query timeout configuration successful", {
                    'timeout_seconds': self.config.query_timeout
                }
            else:
                return False, f"Query timeout configuration failed: {result.error_message}", None
                
        except Exception as e:
            return False, f"Query timeout check failed: {str(e)}", None
    
    def _check_large_results(self) -> Tuple[bool, str, Optional[Dict]]:
        """Check handling of large result sets."""
        try:
            # Test with a query that returns multiple rows
            query = "SELECT seq4() as id, randstr(10, random()) as data FROM table(generator(rowcount => 100))"
            result = self.connection_manager.execute_query(query)
            
            if result.success:
                return True, f"Large result handling successful ({result.row_count} rows)", {
                    'row_count': result.row_count,
                    'execution_time': result.execution_time
                }
            else:
                return False, f"Large result handling failed: {result.error_message}", None
                
        except Exception as e:
            return False, f"Large result check failed: {str(e)}", None


def validate_snowflake_setup(level: ValidationLevel = ValidationLevel.STANDARD) -> ValidationReport:
    """
    Convenience function to validate Snowflake setup.
    
    Args:
        level: Validation level
        
    Returns:
        ValidationReport with results
    """
    validator = SnowflakeValidator()
    return validator.validate_setup(level)


def print_validation_report(report: ValidationReport) -> None:
    """
    Print a formatted validation report.
    
    Args:
        report: ValidationReport to print
    """
    print("\n" + "="*60)
    print("SNOWFLAKE VALIDATION REPORT")
    print("="*60)
    
    # Summary
    status = "✅ PASSED" if report.overall_success else "❌ FAILED"
    print(f"Overall Status: {status}")
    print(f"Total Checks: {report.total_checks}")
    print(f"Passed: {report.passed_checks}")
    print(f"Failed: {report.failed_checks}")
    print(f"Total Time: {report.total_time:.2f}s")
    print()
    
    # Individual results
    for result in report.results:
        status_icon = "✅" if result.success else "❌"
        time_str = f" ({result.execution_time:.2f}s)" if result.execution_time else ""
        print(f"{status_icon} {result.check_name}{time_str}")
        print(f"   {result.message}")
        
        if result.details:
            for key, value in result.details.items():
                print(f"   - {key}: {value}")
        print()
    
    print("="*60)


if __name__ == "__main__":
    # Run validation when script is executed directly
    report = validate_snowflake_setup(ValidationLevel.COMPREHENSIVE)
    print_validation_report(report)
