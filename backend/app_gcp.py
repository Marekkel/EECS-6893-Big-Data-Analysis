#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
FastAPI Backend for Concert Ticket Pricing Prediction - GCP Version
Uses trained Spark ML models from Google Cloud Storage
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
import logging
from datetime import datetime

from predictor_gcp import SparkMLPredictorGCP

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Concert Ticket Pricing API - GCP",
    description="Predict concert ticket prices using trained RandomForest models from GCS",
    version="2.0.0"
)

# Configure CORS for public access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for public API
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global predictor instance
predictor = None
predictor_loading = False

# GCP configuration from environment variables
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-bucket-name")
GCS_OUTPUT_PATH = os.getenv("GCS_OUTPUT_PATH", "output")
GCS_DATA_PATH = os.getenv("GCS_DATA_PATH", "data")

def get_predictor():
    """Lazy load predictor on first request"""
    global predictor, predictor_loading
    
    if predictor is not None:
        return predictor
    
    if predictor_loading:
        raise HTTPException(
            status_code=503,
            detail="Model is loading, please try again in 30 seconds"
        )
    
    try:
        predictor_loading = True
        logger.info("Initializing Spark ML predictor from GCS (lazy loading)...")
        logger.info(f"Bucket: {GCS_BUCKET}")
        logger.info(f"Output path: {GCS_OUTPUT_PATH}")
        
        predictor = SparkMLPredictorGCP(
            bucket_name=GCS_BUCKET,
            output_path=GCS_OUTPUT_PATH,
            data_path=GCS_DATA_PATH
        )
        logger.info("Spark ML predictor loaded successfully from GCS!")
        predictor_loading = False
        return predictor
    except Exception as e:
        predictor_loading = False
        logger.error(f"Failed to load predictor: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to initialize model: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global predictor
    if predictor:
        predictor.cleanup()
        logger.info("Predictor cleaned up")


# Request/Response Models
class PredictionRequest(BaseModel):
    """Request model for prediction"""
    artist: str = Field(..., description="Artist name")
    city: str = Field(..., description="City name")
    state: str = Field(..., description="State code (e.g., NY, CA)")
    date: Optional[str] = Field(None, description="Event date (YYYY-MM-DD)")
    genre: Optional[str] = Field(None, description="Genre name")

    class Config:
        schema_extra = {
            "example": {
                "artist": "Taylor Swift",
                "city": "New York",
                "state": "NY",
                "date": "2024-06-15",
                "genre": "Pop"
            }
        }


class FeatureImportance(BaseModel):
    """Feature importance model"""
    name: str
    score: float


class PredictionResponse(BaseModel):
    """Response model for prediction"""
    probability: float = Field(..., description="Event occurrence probability")
    pred_min: float = Field(..., description="Predicted minimum ticket price")
    pred_max: float = Field(..., description="Predicted maximum ticket price")
    top_features: List[FeatureImportance] = Field(..., description="Top contributing features")
    note: Optional[str] = Field(None, description="Additional notes")
    model_info: Optional[Dict] = Field(None, description="Model information")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Concert Ticket Pricing API - GCP",
        "version": "2.0.0",
        "model": "RandomForest (Spark ML)",
        "deployed_on": "Google Cloud Run",
        "endpoints": [
            "/api/predict",
            "/api/health",
            "/api/model-info"
        ]
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint - always returns healthy to pass Cloud Run checks"""
    return {
        "status": "healthy",
        "predictor_loaded": predictor is not None,
        "predictor_loading": predictor_loading,
        "environment": "GCP",
        "bucket": GCS_BUCKET,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/model-info")
async def model_info():
    """Get model information"""
    pred = get_predictor()
    return pred.get_model_info()


@app.post("/api/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """
    Predict ticket prices using trained Spark ML models from GCS
    
    Args:
        request: PredictionRequest containing artist, location, and time info
        
    Returns:
        PredictionResponse with probability and price predictions
    """
    # Lazy load predictor on first request
    pred = get_predictor()
    
    try:
        logger.info(f"Received prediction request: {request.dict()}")
        
        # Perform prediction using Spark ML models from GCS
        result = pred.predict(
            artist=request.artist,
            city=request.city,
            state=request.state,
            date=request.date,
            genre=request.genre
        )
        
        logger.info(f"Prediction successful: min=${result['pred_min']:.2f}, max=${result['pred_max']:.2f}")
        
        return PredictionResponse(**result)
        
    except ValueError as e:
        logger.warning(f"Invalid input: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(
        "app_gcp:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )
