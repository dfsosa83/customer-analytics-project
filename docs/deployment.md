# Deployment Guide

## Prerequisites

- Docker and Docker Compose installed
- Access to Snowflake data warehouse
- Python 3.11+ (for local development)

## Environment Setup

### 1. Environment Variables

Create a `.env` file from the template:
```bash
cp .env.template .env
```

Configure the following variables:
```env
# Application Settings
APP_NAME=Customer Analytics API
APP_VERSION=0.1.0
DEBUG=true
HOST=0.0.0.0
PORT=8000

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Security
SECRET_KEY=your_secret_key_here
CORS_ORIGINS=["http://localhost:3000", "http://localhost:8080"]

# Logging
LOG_LEVEL=INFO
```

## Deployment Options

### Option 1: Docker Deployment (Recommended)

```bash
# Build and start the application
docker-compose up --build -d

# Check application status
docker-compose logs -f customer-analytics

# Stop the application
docker-compose down
```

### Option 2: Local Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the application
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Health Checks

### Application Health
- **Health Endpoint**: `GET /health`
- **API Documentation**: `GET /docs`
- **Metrics**: `GET /metrics` (if enabled)

### Database Connectivity
```bash
# Test Snowflake connection
curl http://localhost:8000/api/health/database
```

## Monitoring

### Logs
```bash
# Docker logs
docker-compose logs -f customer-analytics

# Application logs (if running locally)
tail -f logs/app.log
```

### Performance Monitoring
- Response time monitoring via health checks
- Database query performance logging
- Error rate tracking

## Scaling Considerations

### Horizontal Scaling
```yaml
# docker-compose.yml
services:
  customer-analytics:
    deploy:
      replicas: 3
    # ... other configuration
```

### Load Balancing
- Use nginx or cloud load balancer
- Configure health check endpoints
- Implement session affinity if needed
