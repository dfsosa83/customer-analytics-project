# Snowflake Setup Guide

This guide will help you configure Snowflake connectivity for the Customer Analytics Project. The setup is designed to be simple and secure, allowing anyone with Snowflake access to get started quickly.

## 📋 Prerequisites

Before setting up Snowflake connectivity, ensure you have:

1. **Snowflake Account Access**
   - Valid Snowflake account with appropriate permissions
   - Username and password
   - Access to a warehouse, database, and schema

2. **Required Information**
   - Snowflake account identifier (e.g., `abc12345.us-east-1`)
   - Warehouse name
   - Database name
   - Schema name
   - Role (optional, uses default if not specified)

## 🚀 Quick Setup

### Step 1: Copy Environment Template

Copy the environment template to create your configuration file:

```bash
cp .env.template .env
```

### Step 2: Configure Snowflake Credentials

Edit the `.env` file and update the Snowflake section with your credentials:

```bash
# =============================================================================
# SNOWFLAKE CONFIGURATION
# =============================================================================
SNOWFLAKE_ACCOUNT=your_account.us-east-1.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_secure_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ANALYST_ROLE  # Optional
```

### Step 3: Test Connection

Test your Snowflake connection:

```bash
python -c "from app.utils.snowflake_config import load_snowflake_config; config = load_snowflake_config(); print('✅ Configuration loaded successfully!')"
```

Or use the connection test:

```bash
python -c "from app.utils.snowflake_connection import get_connection_manager; manager = get_connection_manager(); result = manager.test_connection(); print('✅ Connection successful!' if result['success'] else f'❌ Connection failed: {result.get(\"error\")}')"
```

## 🔧 Detailed Configuration

### Environment Variables Reference

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | ✅ | Snowflake account identifier | `abc12345.us-east-1.snowflakecomputing.com` |
| `SNOWFLAKE_USER` | ✅ | Snowflake username | `analytics_user` |
| `SNOWFLAKE_PASSWORD` | ✅ | Snowflake password | `secure_password_123` |
| `SNOWFLAKE_WAREHOUSE` | ✅ | Warehouse name | `ANALYTICS_WH` |
| `SNOWFLAKE_DATABASE` | ✅ | Database name | `CUSTOMER_DATA` |
| `SNOWFLAKE_SCHEMA` | ✅ | Schema name | `ANALYTICS` |
| `SNOWFLAKE_ROLE` | ❌ | Role name (optional) | `ANALYST_ROLE` |

### Performance Settings

Configure performance settings in your `.env` file:

```bash
# Query timeout in seconds (default: 300)
QUERY_TIMEOUT=300

# Connection pool size (default: 10)
CONNECTION_POOL_SIZE=10

# Maximum rows per query (default: 10000)
MAX_QUERY_ROWS=10000

# Cache TTL in seconds (default: 3600)
CACHE_TTL=3600
```

### Export Settings

Configure data export settings:

```bash
# Default export format (csv, json, parquet, xlsx)
DEFAULT_EXPORT_FORMAT=csv

# Maximum export file size in MB
MAX_EXPORT_SIZE_MB=100

# Export file retention days
EXPORT_RETENTION_DAYS=30
```

## 🔍 Finding Your Snowflake Information

### Account Identifier

Your Snowflake account identifier can be found in several ways:

1. **From Snowflake Web UI**: Look at the URL when logged in
   - Format: `https://app.snowflake.com/[region]/[account]/`
   - Example: `https://app.snowflake.com/us-east-1/abc12345/`

2. **From SQL**: Run this query in Snowflake:
   ```sql
   SELECT CURRENT_ACCOUNT();
   ```

3. **Full Format**: `[account].[region].snowflakecomputing.com`
   - Example: `abc12345.us-east-1.snowflakecomputing.com`

### Current Session Information

To find your current session information, run these queries in Snowflake:

```sql
-- Get current session info
SELECT 
    CURRENT_USER() as current_user,
    CURRENT_ROLE() as current_role,
    CURRENT_DATABASE() as current_database,
    CURRENT_SCHEMA() as current_schema,
    CURRENT_WAREHOUSE() as current_warehouse;

-- List available warehouses
SHOW WAREHOUSES;

-- List available databases
SHOW DATABASES;

-- List available schemas
SHOW SCHEMAS;

-- List available roles
SHOW ROLES;
```

## 🛡️ Security Best Practices

### 1. Environment File Security

- **Never commit `.env` files** to version control
- Use strong, unique passwords
- Regularly rotate credentials
- Consider using environment-specific files (`.env.dev`, `.env.prod`)

### 2. Role-Based Access

- Use specific roles with minimal required permissions
- Avoid using `ACCOUNTADMIN` or `SYSADMIN` roles for applications
- Create dedicated service accounts for applications

### 3. Network Security

- Use Snowflake's network policies if available
- Consider IP whitelisting for production environments
- Enable MFA for user accounts

## 🧪 Testing Your Setup

### 1. Configuration Test

```python
from app.utils.snowflake_config import load_snowflake_config

try:
    config = load_snowflake_config()
    print("✅ Configuration loaded successfully!")
    print(f"Account: {config.snowflake_account}")
    print(f"Database: {config.snowflake_database}")
    print(f"Schema: {config.snowflake_schema}")
except Exception as e:
    print(f"❌ Configuration error: {e}")
```

### 2. Connection Test

```python
from app.utils.snowflake_connection import get_connection_manager

manager = get_connection_manager()
result = manager.test_connection()

if result['success']:
    print("✅ Connection successful!")
    print(f"Snowflake Version: {result['snowflake_version']}")
    print(f"Current User: {result['current_user']}")
    print(f"Current Role: {result['current_role']}")
    print(f"Current Database: {result['current_database']}")
    print(f"Current Schema: {result['current_schema']}")
    print(f"Current Warehouse: {result['current_warehouse']}")
else:
    print(f"❌ Connection failed: {result['error']}")
```

### 3. Query Test

```python
from app.utils.snowflake_connection import execute_query

# Test a simple query
result = execute_query("SELECT CURRENT_TIMESTAMP() as current_time")

if result.success:
    print("✅ Query executed successfully!")
    print(f"Result: {result.data}")
else:
    print(f"❌ Query failed: {result.error_message}")
```

## 🚀 Using the API

Once configured, you can use the Snowflake API endpoints:

### Health Check

```bash
curl -X GET "http://localhost:8000/api/snowflake/health"
```

### Execute Query

```bash
curl -X POST "http://localhost:8000/api/snowflake/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT CURRENT_TIMESTAMP() as current_time",
    "limit": 100
  }'
```

### Export Data

```bash
curl -X POST "http://localhost:8000/api/snowflake/export" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM your_table LIMIT 1000",
    "format": "csv",
    "filename": "export_data"
  }' \
  --output export_data.csv
```

## 🔧 Troubleshooting

### Common Issues

1. **"Account identifier is invalid"**
   - Ensure account includes region: `account.region.snowflakecomputing.com`
   - Check for typos in account identifier

2. **"Authentication failed"**
   - Verify username and password
   - Check if account is locked or password expired
   - Ensure user has necessary permissions

3. **"Warehouse not found"**
   - Verify warehouse name and permissions
   - Ensure warehouse is running
   - Check if user has USAGE privilege on warehouse

4. **"Database/Schema not found"**
   - Verify database and schema names
   - Check user permissions
   - Ensure objects exist and are accessible

### Debug Mode

Enable debug logging by setting in your `.env`:

```bash
LOG_LEVEL=DEBUG
```

### Getting Help

1. Check the application logs for detailed error messages
2. Use the `/snowflake/health` endpoint to test connectivity
3. Verify permissions in Snowflake using the queries in this guide
4. Contact your Snowflake administrator for access issues

## 📚 Additional Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Project API Documentation](http://localhost:8000/docs) (when running)
