# Quick Start Guide

Get the Customer Analytics Project up and running in minutes.

## 🚀 Prerequisites

Before you begin, ensure you have:
- **Python 3.11+** installed
- **Docker and Docker Compose** installed
- **Snowflake account** with appropriate permissions
- **Git** for version control

## ⚡ 5-Minute Setup

### Step 1: Clone and Navigate
```bash
git clone <repository-url>
cd customer-analytics-project
```

### Step 2: Configure Environment
```bash
# Copy the environment template
cp .env.template .env

# Edit the .env file with your Snowflake credentials
nano .env  # or use your preferred editor
```

Required environment variables:
```env
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

### Step 3: Start the Application
```bash
# Using Docker (recommended)
docker-compose up --build -d

# Check if it's running
docker-compose logs -f customer-analytics
```

### Step 4: Verify Installation
Open your browser and visit:
- **Application**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## 🔧 Alternative: Local Development

If you prefer to run without Docker:

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the application
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## 📋 First Steps

### 1. Test Snowflake Connection
```bash
curl http://localhost:8000/api/snowflake/health
```

Expected response:
```json
{
  "success": true,
  "connection_time": 6.85,
  "current_user": "DSOSA",
  "current_database": "DATA_SCIENCE"
}
```

### 2. Download Portfolio Data (Easy Way!)
```bash
# Interactive portfolio data downloader
python download_portfolio.py
```

**Options available:**
- Quick sample (100 rows) - ~30 seconds
- Medium dataset (500 rows) - ~1 minute
- Large dataset (2,500 rows) - ~3 minutes
- Custom size

**Data saved to:** `data/portfolio_data_[rows]_[timestamp].csv`

### 3. Explore API Documentation
Visit http://localhost:8000/docs to see:
- Available endpoints
- Request/response schemas
- Interactive API testing

### 4. Test Snowflake Endpoints
```bash
# Test basic query
curl -X POST "http://localhost:8000/api/snowflake/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT CURRENT_USER(), CURRENT_DATABASE()"}'
```

## 🛠️ Development Workflow

### Making Changes
1. **Edit code** in your preferred IDE
2. **Run tests** to ensure quality
3. **Check logs** for any issues
4. **Test endpoints** via API docs

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_services/test_snowflake_service.py
```

### Viewing Logs
```bash
# Docker logs
docker-compose logs -f customer-analytics

# Follow logs in real-time
docker-compose logs -f --tail=100 customer-analytics
```

## 🔍 Troubleshooting

### Common Issues

#### 1. Snowflake Connection Failed
**Problem**: Cannot connect to Snowflake database
**Solution**: 
- Verify credentials in `.env` file
- Check network connectivity
- Ensure Snowflake account is active

#### 2. Port Already in Use
**Problem**: Port 8000 is already occupied
**Solution**:
```bash
# Find process using port 8000
lsof -i :8000  # On macOS/Linux
netstat -ano | findstr :8000  # On Windows

# Kill the process or change port in docker-compose.yml
```

#### 3. Docker Build Fails
**Problem**: Docker build encounters errors
**Solution**:
```bash
# Clean Docker cache
docker system prune -a

# Rebuild without cache
docker-compose build --no-cache
```

#### 4. Permission Denied
**Problem**: Permission errors with Docker
**Solution**:
```bash
# On Linux/macOS, ensure Docker permissions
sudo usermod -aG docker $USER
# Log out and back in
```

### Getting Help

1. **Check logs** first: `docker-compose logs customer-analytics`
2. **Review documentation** in `/docs/` directory
3. **Test connectivity** with health check endpoint
4. **Verify environment** variables are correctly set

## 📚 Next Steps

Once you have the application running:

1. **Explore the codebase** to understand the structure
2. **Review documentation** in the `/docs/` directory
3. **Run the test suite** to understand expected behavior
4. **Check the roadmap** in `PROJECT_SUMMARY.md` for upcoming features
5. **Start implementing** your specific analytics requirements

## 🎯 Quick Commands Reference

```bash
# Start application
docker-compose up -d

# Stop application
docker-compose down

# View logs
docker-compose logs -f customer-analytics

# Restart application
docker-compose restart customer-analytics

# Run tests
pytest

# Access container shell
docker-compose exec customer-analytics bash

# Check application status
curl http://localhost:8000/health
```

## 📞 Support

If you encounter issues:
- Check the troubleshooting section above
- Review the full documentation in `README.md`
- Examine application logs for error details
- Verify all prerequisites are properly installed

---

**🎉 You're ready to start building customer analytics!**
