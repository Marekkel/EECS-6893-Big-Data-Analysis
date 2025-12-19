# Backend API - Complete Spark ML Implementation

FastAPI-based backend service for real-time concert ticket price predictions using trained Spark ML RandomForest models.

---

## 📋 System Requirements

- **Python**: 3.10+
- **Java**: OpenJDK 8 or 11 (required for PySpark)
- **RAM**: 8GB+ recommended
- **Trained Models**: Located in `../output/` directory

---

## 🚀 Quick Start

### 1. Install Java (if not already installed)

**Windows**:
```powershell
# Download and install OpenJDK 11 from https://adoptium.net/
# After installation, verify:
java -version
```

**Linux/macOS**:
```bash
# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk

# macOS
brew install openjdk@11

# Verify
java -version
```

---

### 2. Set Java Environment Variables

**Windows PowerShell**:
```powershell
# Set JAVA_HOME (adjust path to your installation)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.x.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# Verify
java -version
```

**Linux/macOS**:
```bash
# Add to ~/.bashrc or ~/.zshrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Apply changes
source ~/.bashrc

# Verify
java -version
```

---

### 3. Create Virtual Environment and Install Dependencies

```powershell
cd backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows
.\venv\Scripts\Activate.ps1

# Linux/macOS
source venv/bin/activate

# If PowerShell execution policy blocks activation
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Install dependencies
pip install -r requirements.txt
```

---

### 4. Verify Model Files

Ensure the following paths exist:
```
../output/ml_multi_models_max/models/random_forest/
../output/ml_multi_models_min/models/random_forest/
../output/ml_results_max/avg_encoding_max/
../output/ml_results_min/avg_encoding_min/
../data/master_df.csv
```

**Check with**:
```powershell
# Windows
ls ..\output\ml_multi_models_max\models\random_forest

# Linux/macOS
ls -R ../output/ml_multi_models_max/models/random_forest/
```

---

### 5. Start the Service

```powershell
# In backend directory
python app_gcp.py
```

**Service starts at**: `http://localhost:8000`

**⏱️ First startup takes 30-60 seconds** due to:
- Initializing Spark session
- Loading RandomForest models (max and min)
- Loading average encoding mappings
- Loading artist reference data

**Success Log Output**:
```
INFO:     Started server process [12345]
INFO:     Waiting for application startup.
[SparkPredictor] Initializing Spark session...
[SparkPredictor] Loading models...
[SparkPredictor] Successfully loaded models and data
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

---

## 🌐 API Endpoints

### Root Endpoint
```
GET /
```

**Description**: Returns API basic information and available endpoints.

**Response**:
```json
{
  "message": "Concert Ticket Price Prediction API",
  "version": "1.0.0",
  "endpoints": {
    "health": "/api/health",
    "model_info": "/api/model-info",
    "predict": "/api/predict"
  },
  "documentation": "/docs"
}
```

---

### Health Check
```
GET /api/health
```

**Description**: Check service and Spark session status.

**Response**:
```json
{
  "status": "healthy",
  "predictor_loaded": true,
  "spark_session_active": true,
  "timestamp": "2024-01-15T10:30:00.123456"
}
```

**Status Codes**:
- `200 OK`: Service healthy
- `503 Service Unavailable`: Service degraded (Spark not initialized)

---

### Model Information
```
GET /api/model-info
```

**Description**: Retrieve model metadata, feature list, and performance metrics.

**Response**:
```json
{
  "model_type": "RandomForest",
  "spark_version": "3.1.2",
  "features": [
    "spotify_popularity",
    "city_avg_max",
    "state_avg_max",
    "genre_avg_max",
    "subgenre_avg_max",
    "global_avg_max",
    "year",
    "month",
    "weekday",
    "is_weekend"
  ],
  "model_performance": {
    "max_price_model": {
      "rmse": 93.7,
      "mae": 42.1,
      "r2": 0.617
    },
    "min_price_model": {
      "rmse": 45.2,
      "mae": 28.5,
      "r2": 0.582
    }
  },
  "training_date": "2024-01-10",
  "dataset_size": 5102
}
```

---

### Price Prediction
```
POST /api/predict
```

**Description**: Predict concert ticket price (max and min) based on input features.

**Request Body**:
```json
{
  "artist": "Taylor Swift",
  "city": "New York",
  "state": "NY",
  "date": "2024-06-15",
  "genre": "Pop",
  "subgenre": "Dance Pop",
  "spotify_followers": 85000000,
  "spotify_popularity": 95
}
```

**Required Fields**:
- `artist` (string): Artist name
- `city` (string): City name
- `state` (string): State code (e.g., "NY", "CA")
- `date` (string): Event date in YYYY-MM-DD format
- `genre` (string): Music genre

**Optional Fields**:
- `subgenre` (string): Music subgenre (defaults to genre if not provided)
- `spotify_followers` (int): Number of Spotify followers (auto-fetched if not provided)
- `spotify_popularity` (int): Spotify popularity score 0-100 (auto-fetched if not provided)

**Response**:
```json
{
  "prediction": {
    "max_price": 118.5,
    "min_price": 45.2,
    "price_range": 73.3,
    "confidence": "high"
  },
  "input_features": {
    "artist": "Taylor Swift",
    "city": "New York",
    "state": "NY",
    "date": "2024-06-15",
    "genre": "Pop",
    "subgenre": "Dance Pop",
    "spotify_followers": 85000000,
    "spotify_popularity": 95,
    "year": 2024,
    "month": 6,
    "weekday": 6,
    "is_weekend": false,
    "city_avg_max": 72.5,
    "state_avg_max": 68.3,
    "genre_avg_max": 58.7,
    "subgenre_avg_max": 62.1,
    "global_avg_max": 55.4
  },
  "model_info": {
    "model_type": "RandomForest",
    "algorithm": "Spark MLlib RandomForestRegressor"
  },
  "timestamp": "2024-01-15T10:35:22.123456"
}
```

**Error Responses**:
```json
// 400 Bad Request - Missing required fields
{
  "detail": "Missing required field: artist"
}

// 400 Bad Request - Invalid date format
{
  "detail": "Invalid date format. Use YYYY-MM-DD"
}

// 500 Internal Server Error - Prediction failed
{
  "detail": "Prediction failed: [error message]"
}
```

---

## 🧪 Testing

### Automated Test Script
```powershell
cd backend
python test_backend.py
```

**Test Coverage**:
- ✅ Health check endpoint
- ✅ Model info endpoint
- ✅ Prediction endpoint with valid data
- ✅ Error handling with invalid data
- ✅ Response time benchmarks

**Sample Output**:
```
Testing health endpoint...
✓ Health check passed

Testing model info endpoint...
✓ Model info retrieved successfully

Testing prediction endpoint...
✓ Prediction successful: max=$118.50, min=$45.20

Testing error handling...
✓ Error handling works correctly

All tests passed! ✨
Average response time: 850ms
```

---

### Manual Testing with curl

**Health Check**:
```bash
curl http://localhost:8000/api/health
```

**Model Info**:
```bash
curl http://localhost:8000/api/model-info
```

**Prediction**:
```bash
curl -X POST http://localhost:8000/api/predict \
  -H "Content-Type: application/json" \
  -d '{
    "artist": "Ed Sheeran",
    "city": "Los Angeles",
    "state": "CA",
    "date": "2024-08-20",
    "genre": "Pop"
  }'
```

---

### Interactive API Documentation

FastAPI automatically generates interactive API docs:

**Swagger UI** (recommended):
```
http://localhost:8000/docs
```

**ReDoc** (alternative):
```
http://localhost:8000/redoc
```

Features:
- Try endpoints directly in browser
- Auto-generated request examples
- Response schema validation
- Authentication testing

---

## 📁 Project Structure

```
backend/
├── app_gcp.py                 # Main FastAPI application
├── predictor_gcp.py           # Spark ML prediction logic
├── requirements.txt           # Python dependencies
├── test_backend.py            # Automated tests
├── startup.py                 # Startup utilities
├── backend_url.txt            # Deployment URL reference
├── QUICKSTART.md              # Quick reference guide
└── README.md                  # This file
```

---

## 🛠️ Configuration

### Environment Variables

Create `.env` file in backend directory:
```env
# Server Configuration
HOST=0.0.0.0
PORT=8000

# Model Paths (relative to backend/)
MODEL_MAX_PATH=../output/ml_multi_models_max/models/random_forest
MODEL_MIN_PATH=../output/ml_multi_models_min/models/random_forest
AVG_ENCODING_MAX_PATH=../output/ml_results_max/avg_encoding_max
AVG_ENCODING_MIN_PATH=../output/ml_results_min/avg_encoding_min
ARTIST_DATA_PATH=../data/master_df.csv

# Spark Configuration
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

---

## 🔧 Troubleshooting

### Issue 1: "Java not found" Error
**Symptom**:
```
Exception: Java gateway process exited before sending its port number
```

**Solution**:
1. Install Java: https://adoptium.net/
2. Set JAVA_HOME:
   ```powershell
   $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.x.x-hotspot"
   ```
3. Verify: `java -version`

---

### Issue 2: "Model files not found" Error
**Symptom**:
```
FileNotFoundError: ../output/ml_multi_models_max/models/random_forest
```

**Solution**:
1. Check if models exist:
   ```powershell
   ls ..\output\ml_multi_models_max\models\
   ```
2. If missing, run training pipeline:
   ```powershell
   cd training
   python run_master_pipeline.py --mode local
   ```

---

### Issue 3: "Port 8000 already in use" Error
**Symptom**:
```
OSError: [Errno 48] Address already in use
```

**Solution (Windows)**:
```powershell
# Find process using port 8000
netstat -ano | findstr :8000

# Kill process
taskkill /PID <PID> /F

# Or use different port
python app_gcp.py --port 8001
```

**Solution (Linux/macOS)**:
```bash
# Find and kill process
lsof -ti:8000 | xargs kill -9

# Or use different port
python app_gcp.py --port 8001
```

---

### Issue 4: Slow First Prediction
**Symptom**: First prediction takes 5-10 seconds

**Explanation**: Normal behavior - Spark lazy initialization

**Solution**: No action needed. Subsequent predictions are fast (<2 seconds)

---

### Issue 5: Memory Issues
**Symptom**:
```
OutOfMemoryError: Java heap space
```

**Solution**: Increase Spark memory allocation
```powershell
# Before starting app
$env:SPARK_DRIVER_MEMORY = "4g"
python app_gcp.py
```

---

## 📊 Performance Benchmarks

### Startup Time
- **Cold start**: 30-60 seconds (includes Spark initialization)
- **Warm start**: 10-15 seconds (Spark session cached)

### Prediction Latency
- **First prediction**: 2-5 seconds (lazy computation)
- **Subsequent predictions**: 500ms - 2 seconds
- **Throughput**: ~30-60 requests/minute

### Memory Usage
- **Idle**: 500MB - 1GB
- **Active (Spark session)**: 1-2GB
- **Under load**: 2-4GB

### Model Size
- **RandomForest MAX**: ~50MB
- **RandomForest MIN**: ~50MB
- **Average encoding mappings**: ~5MB each
- **Total**: ~110MB

---

## 🚀 Deployment

### Local Development
```powershell
python app_gcp.py
```
Access at: http://localhost:8000

### Docker Deployment
```bash
# Build image
docker build -t ticket-price-api .

# Run container
docker run -p 8000:8000 \
  -v $(pwd)/../output:/app/output \
  -v $(pwd)/../data:/app/data \
  ticket-price-api
```

### Google Cloud Run Deployment
```bash
# Using deployment script
cd deployment
./deploy_to_gcp.ps1

# Manual deployment
gcloud builds submit --tag gcr.io/PROJECT_ID/ticket-price-api
gcloud run deploy ticket-price-api \
  --image gcr.io/PROJECT_ID/ticket-price-api \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --timeout 60s
```

See [GCP_DEPLOYMENT_GUIDE.md](../GCP_DEPLOYMENT_GUIDE.md) for detailed instructions.

---

## 📚 Dependencies

### Core Dependencies
```
fastapi==0.104.1         # Web framework
uvicorn==0.24.0          # ASGI server
pyspark==3.1.2           # Spark ML engine
pydantic==2.5.0          # Data validation
```

### Optional Dependencies
```
python-dotenv==1.0.0     # Environment variable management
requests==2.31.0         # HTTP client for testing
pytest==7.4.3            # Testing framework
```

Install all:
```bash
pip install -r requirements.txt
```

---

## 🔐 Security Considerations

### Input Validation
- All inputs validated with Pydantic models
- SQL injection protection (no direct SQL queries)
- XSS protection (JSON responses only)

### CORS Configuration
Enabled for frontend integration:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Rate Limiting
Consider adding rate limiting for production:
```bash
pip install slowapi
```

---

## 📖 Related Documentation

- **Quick Start Guide**: [QUICKSTART.md](QUICKSTART.md)
- **Training Pipeline**: [../training/README.md](../training/)
- **Frontend Integration**: [../frontend/](../frontend/)
- **Deployment Guide**: [../GCP_DEPLOYMENT_GUIDE.md](../GCP_DEPLOYMENT_GUIDE.md)
- **Output Files**: [../OUTPUT_GUIDE.md](../OUTPUT_GUIDE.md)
- **Main Project README**: [../README.md](../README.md)

---

## 🤝 Contributing

Contributions welcome! Please ensure:
1. All tests pass: `python test_backend.py`
2. Code follows PEP 8 style guide
3. API documentation updated
4. Performance benchmarks maintained

---

## 📄 License

MIT License - See [LICENSE](../LICENSE) for details

---

**Last Updated**: December 18, 2025
**Version**: 1.0.0
**Maintainers**: EECS 6893 Big Data Analysis Team
