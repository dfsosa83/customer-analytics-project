# Customer Analytics Project

A modern data analytics microservice for querying customer data stored in Snowflake to identify product relationships and conduct exploratory data analysis. Built with FastAPI and designed for deployment as a containerized microservice.

## 🎯 Project Objectives

### Primary Goals
- **Customer-Product Mapping**: Query Snowflake to identify which products belong to specific customers
- **Exploratory Data Analysis**: Conduct comprehensive EDA on customer datasets to understand data relationships and patterns
- **Microservice Architecture**: Deploy as a scalable microservice that other applications can consume via REST API
- **Data Discovery**: Flexible framework for exploring and understanding customer data structures

### Business Value
- **Automated Insights**: Replace manual data queries with automated analytics
- **API-First Design**: Enable integration with other business applications
- **Scalable Architecture**: Support growing data volumes and user base
- **Data Quality**: Implement validation and quality checks for reliable analytics

## 🏗️ Architecture Overview

### High-Level Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client Apps   │───▶│   FastAPI       │───▶│   Snowflake     │
│                 │    │   Microservice  │    │   Data Warehouse│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Data Cache    │
                       │   & Exports     │
                       └─────────────────┘
```

### Core Components
- **API Layer**: FastAPI-based REST endpoints with automatic documentation
- **Business Logic**: Service layer for data processing and analytics
- **Data Access**: Snowflake connectivity with connection pooling
- **Caching**: Local caching for performance optimization
- **Export Engine**: CSV/JSON export capabilities for analysis results

## 📊 Data Sources and Requirements

### Primary Data Source: Snowflake
- **Customer Data**: Customer profiles, demographics, and metadata
- **Product Data**: Product catalog, categories, and attributes  
- **Relationship Data**: Customer-product associations and transactions
- **Historical Data**: Time-series data for trend analysis

### Expected Data Structure
```sql
-- Core tables expected in Snowflake
customers (customer_id, customer_name, customer_type, region, ...)
products (product_id, product_name, category, type, ...)
customer_products (customer_id, product_id, relationship_type, start_date, ...)
```

### Data Quality Requirements
- **Completeness**: Required fields must be populated
- **Consistency**: Standardized formats and naming conventions
- **Referential Integrity**: Valid foreign key relationships
- **Temporal Logic**: Logical date sequences and ranges

## 🛠️ Technology Stack

### Core Technologies
- **Framework**: FastAPI (Python 3.11+)
- **Database**: Snowflake Data Warehouse
- **Data Processing**: pandas, numpy for analytics
- **API Documentation**: OpenAPI/Swagger (automatic)
- **Testing**: pytest with comprehensive test coverage

### Infrastructure
- **Containerization**: Docker + Docker Compose
- **Deployment**: Container-ready for cloud platforms
- **Monitoring**: Health checks and performance metrics
- **Security**: Environment-based configuration management

### Development Tools
- **Code Quality**: Black, flake8, mypy for code standards
- **Documentation**: Markdown-based documentation
- **Version Control**: Git with structured branching strategy

## 🚀 Quick Start

### Prerequisites
- Python 3.11+ installed
- Docker and Docker Compose
- Access to Snowflake data warehouse
- Git for version control

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd customer-analytics-project
   ```

2. **Snowflake setup** (Interactive - Recommended):
   ```bash
   # Run the interactive setup script
   python scripts/setup_snowflake.py

   # This will guide you through:
   # - Creating .env file with your credentials
   # - Validating your Snowflake connection
   # - Testing all functionality
   ```

   **Manual setup** (Alternative):
   ```bash
   # Copy environment template
   cp .env.template .env

   # Edit .env with your Snowflake credentials
   # SNOWFLAKE_ACCOUNT=your_account.region.snowflakecomputing.com
   # SNOWFLAKE_USER=your_username
   # SNOWFLAKE_PASSWORD=your_password
   # SNOWFLAKE_WAREHOUSE=your_warehouse
   # SNOWFLAKE_DATABASE=your_database
   # SNOWFLAKE_SCHEMA=your_schema

   # Validate your setup
   python scripts/setup_snowflake.py --validate-only
   ```

3. **Docker deployment** (recommended):
   ```bash
   # Build and start the application
   docker-compose up --build -d

   # Check application status
   docker-compose logs -f customer-analytics
   ```

4. **Local development** (alternative):
   ```bash
   # Create virtual environment
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt

   # Run the application
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

### Verify Installation
- **Application**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **General Health Check**: http://localhost:8000/health
- **Snowflake Health Check**: http://localhost:8000/api/snowflake/health
- **Snowflake Configuration**: http://localhost:8000/api/snowflake/config

### 🏔️ Snowflake Features

The project includes comprehensive Snowflake integration with:

#### 🔌 Connection Management
- Automatic connection pooling and retry logic
- Health monitoring and connection validation
- Support for multiple authentication methods

#### 📊 Data Operations
- Execute SQL queries with parameter binding
- Export data in multiple formats (CSV, JSON, Parquet, Excel)
- Streaming downloads for large datasets
- Query result caching for performance

#### 🛡️ Security & Validation
- Comprehensive configuration validation
- Secure credential management
- Query safety checks and timeouts
- Role-based access control support

#### 📋 API Endpoints
- `/api/snowflake/health` - Connection health check
- `/api/snowflake/query` - Execute SQL queries
- `/api/snowflake/export` - Export query results
- `/api/snowflake/tables` - List and inspect tables
- `/api/snowflake/schemas` - List available schemas

For detailed Snowflake setup instructions, see [docs/snowflake_setup.md](docs/snowflake_setup.md)

## 📋 Development Roadmap

### Phase 1: Foundation (Current)
- [x] **Project Structure**: Establish directory structure and documentation
- [ ] **Core Models**: Define customer, product, and analytics data models
- [ ] **Snowflake Integration**: Implement database connectivity and basic queries
- [ ] **Basic API**: Create fundamental REST endpoints
- [ ] **Docker Setup**: Containerization and deployment configuration

### Phase 2: Core Analytics
- [ ] **Customer Queries**: Implement customer data retrieval and filtering
- [ ] **Product Mapping**: Build customer-product relationship queries
- [ ] **Data Validation**: Add comprehensive input validation and error handling
- [ ] **Export Functionality**: CSV/JSON export capabilities
- [ ] **Basic EDA**: Implement exploratory data analysis endpoints

### Phase 3: Advanced Features
- [ ] **Advanced Analytics**: Statistical analysis and pattern recognition
- [ ] **Caching Layer**: Implement query result caching for performance
- [ ] **Batch Processing**: Support for large-scale data processing
- [ ] **Data Visualization**: Generate charts and graphs for insights
- [ ] **Performance Optimization**: Query optimization and connection pooling

### Phase 4: Production Readiness
- [ ] **Comprehensive Testing**: Unit, integration, and performance tests
- [ ] **Monitoring & Logging**: Application monitoring and structured logging
- [ ] **Security Hardening**: Authentication, authorization, and data protection
- [ ] **Documentation**: Complete API documentation and user guides
- [ ] **CI/CD Pipeline**: Automated testing and deployment

## 🔧 Configuration

### Environment Variables
```env
# Application Settings
APP_NAME=Customer Analytics API
APP_VERSION=0.1.0
DEBUG=false
HOST=0.0.0.0
PORT=8000

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Performance Settings
QUERY_TIMEOUT=300
CONNECTION_POOL_SIZE=10
CACHE_TTL=3600
```

## 📚 API Documentation

### Core Endpoints
```
GET  /health                          # Application health check
GET  /api/customers/                   # List customers with filtering
GET  /api/customers/{customer_id}      # Get specific customer details
GET  /api/products/                    # List products with filtering
GET  /api/products/{product_id}        # Get specific product details
POST /api/analytics/customer-products  # Analyze customer-product relationships
GET  /api/analytics/export/{format}    # Export analysis results
```

### Example Usage
```python
import requests

# Get customer data
response = requests.get('http://localhost:8000/api/customers/', params={
    'region': 'North America',
    'customer_type': 'Enterprise',
    'limit': 100
})

customers = response.json()

# Analyze customer-product relationships
analysis_request = {
    'customer_ids': ['CUST001', 'CUST002'],
    'analysis_type': 'product_portfolio',
    'include_historical': True
}

response = requests.post(
    'http://localhost:8000/api/analytics/customer-products',
    json=analysis_request
)

results = response.json()
```

## 🧪 Testing

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test categories
pytest tests/test_services/  # Service layer tests
pytest tests/test_routers/   # API endpoint tests
```

### Test Categories
- **Unit Tests**: Individual function and method testing
- **Integration Tests**: Database connectivity and query testing
- **API Tests**: Endpoint functionality and response validation
- **Performance Tests**: Query performance and load testing

## 📈 Performance Considerations

### Optimization Strategies
- **Connection Pooling**: Efficient Snowflake connection management
- **Query Caching**: Cache frequently accessed query results
- **Pagination**: Implement pagination for large result sets
- **Async Processing**: Non-blocking operations for better throughput
- **Index Optimization**: Ensure proper database indexing

### Monitoring Metrics
- **Response Times**: API endpoint performance tracking
- **Database Performance**: Query execution time monitoring
- **Error Rates**: Application error tracking and alerting
- **Resource Usage**: Memory and CPU utilization monitoring

## 🔒 Security

### Data Protection
- **Environment Variables**: Secure credential management
- **Connection Encryption**: Encrypted Snowflake connections
- **Input Validation**: Comprehensive input sanitization
- **Error Handling**: Secure error messages without data exposure

### Access Control
- **API Authentication**: Token-based authentication (future)
- **Role-Based Access**: User role and permission management (future)
- **Audit Logging**: Track data access and modifications (future)

## 🤝 Contributing

### Development Workflow
1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Code Standards
- Follow **PEP 8** Python style guidelines
- Use **type hints** for all function parameters and returns
- Write **comprehensive tests** for new functionality
- Update **documentation** for API changes

## 📞 Support

### Getting Help
- **Documentation**: Check `/docs/` directory for detailed guides
- **API Reference**: Visit `/docs` endpoint for interactive API documentation
- **Issues**: Report bugs and feature requests via issue tracker
- **Discussions**: Use discussion forum for questions and ideas

### Troubleshooting
- **Connection Issues**: Verify Snowflake credentials and network access
- **Performance Problems**: Check query complexity and data volume
- **Docker Issues**: Ensure Docker daemon is running and ports are available

---

**Version**: 0.1.0
**Status**: In Development
**Last Updated**: 2024
**License**: Proprietary
