# System Architecture

## Overview

The Customer Analytics Project is designed as a microservice that connects to Snowflake to analyze customer-product relationships. The architecture follows modern Python web service patterns with clear separation of concerns.

## Architecture Layers

### 1. API Layer (`/routers/`)
- **FastAPI** framework for REST API endpoints
- **OpenAPI/Swagger** automatic documentation
- **Request validation** and response serialization
- **Error handling** and HTTP status management

### 2. Business Logic Layer (`/services/`)
- **SnowflakeService**: Database connectivity and query execution
- **AnalyticsService**: Data analysis and relationship discovery
- **ExportService**: Data export and formatting

### 3. Data Layer (`/models/`)
- **Pydantic models** for data validation and serialization
- **Customer models**: Customer entity definitions
- **Product models**: Product entity definitions  
- **Analytics models**: Analysis result structures

### 4. Utility Layer (`/utils/`)
- **Database utilities**: Connection management and query helpers
- **Validators**: Input validation and data quality checks
- **Common functions**: Shared utility functions

## Data Flow

```
Client Request → API Router → Service Layer → Snowflake → Data Processing → Response
```

1. **Client** sends HTTP request to API endpoint
2. **Router** validates request and routes to appropriate service
3. **Service** executes business logic and queries Snowflake
4. **Data Processing** transforms and analyzes results
5. **Response** returns structured data to client

## Technology Stack

- **Framework**: FastAPI (Python 3.11+)
- **Database**: Snowflake Data Warehouse
- **Containerization**: Docker + Docker Compose
- **Documentation**: OpenAPI/Swagger
- **Testing**: pytest
- **Data Processing**: pandas, numpy
- **Deployment**: Docker containers

## Scalability Considerations

- **Stateless design** for horizontal scaling
- **Connection pooling** for database efficiency
- **Caching layer** for frequently accessed data
- **Async processing** for non-blocking operations
