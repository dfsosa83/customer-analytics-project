# Data Sources Documentation

## Primary Data Source: Snowflake

### Connection Configuration
```python
# Environment variables required:
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

### Expected Data Structure

#### Customer Data
```sql
-- Expected customer table structure
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_type VARCHAR(50),
    registration_date DATE,
    status VARCHAR(20),
    region VARCHAR(100),
    -- Additional customer attributes
);
```

#### Product Data  
```sql
-- Expected product table structure
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_type VARCHAR(50),
    launch_date DATE,
    status VARCHAR(20),
    -- Additional product attributes
);
```

#### Customer-Product Relationships
```sql
-- Expected relationship/transaction table
CREATE TABLE customer_products (
    relationship_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    relationship_type VARCHAR(50), -- 'owns', 'interested', 'purchased', etc.
    start_date DATE,
    end_date DATE,
    value DECIMAL(15,2),
    -- Additional relationship attributes
);
```

## Data Quality Expectations

### Customer Data Quality
- **Completeness**: customer_id and customer_name required
- **Uniqueness**: customer_id must be unique
- **Validity**: dates in proper format, status from predefined list
- **Consistency**: region names standardized

### Product Data Quality
- **Completeness**: product_id and product_name required
- **Uniqueness**: product_id must be unique
- **Validity**: dates logical (launch_date <= current_date)
- **Consistency**: categories from predefined taxonomy

### Relationship Data Quality
- **Referential Integrity**: customer_id and product_id must exist
- **Temporal Logic**: start_date <= end_date (if end_date exists)
- **Business Rules**: relationship_type from valid list

## Query Patterns

### Common Analytics Queries
1. **Customer Product Portfolio**: What products does a customer have?
2. **Product Customer Base**: Which customers have a specific product?
3. **Cross-selling Opportunities**: What products do similar customers have?
4. **Customer Segmentation**: Group customers by product usage patterns
5. **Product Performance**: Analyze product adoption and retention

### Performance Considerations
- Use appropriate **indexes** on join columns
- Consider **partitioning** by date for large tables
- Implement **query result caching** for frequently accessed data
- Use **LIMIT** clauses for exploratory queries
