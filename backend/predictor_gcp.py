#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark ML Model Predictor - GCP Version
Loads trained RandomForest models from Google Cloud Storage
"""

import pandas as pd
import numpy as np
import tempfile
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
import logging

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

logger = logging.getLogger(__name__)


class SparkMLPredictorGCP:
    """
    Predictor using trained Spark ML RandomForest models from GCS
    """
    
    def __init__(self, bucket_name: str, output_path: str = "output", data_path: str = "data"):
        """
        Initialize predictor by loading Spark ML models from GCS
        
        Args:
            bucket_name: GCS bucket name
            output_path: Path to output directory in bucket
            data_path: Path to data directory in bucket
        """
        self.bucket_name = bucket_name
        self.output_path = output_path
        self.data_path = data_path
        
        logger.info("Initializing GCS client...")
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        
        logger.info("Creating temporary directory...")
        self.temp_dir = tempfile.mkdtemp()
        
        logger.info("Initializing Spark session...")
        self.spark = self._create_spark_session()
        
        logger.info("Downloading models from GCS...")
        self.local_max_model = self._download_model_from_gcs(
            f"{output_path}/ml_multi_models_max/models/random_forest",
            "max_model"
        )
        self.local_min_model = self._download_model_from_gcs(
            f"{output_path}/ml_multi_models_min/models/random_forest",
            "min_model"
        )
        
        logger.info("Loading Spark ML models...")
        from pyspark.ml.regression import RandomForestRegressionModel
        self.max_model = RandomForestRegressionModel.load(self.local_max_model)
        self.min_model = RandomForestRegressionModel.load(self.local_min_model)
        
        logger.info("Loading average encoding mappings...")
        self._load_avg_encodings_from_gcs()
        
        logger.info("Loading artist data...")
        self._load_artist_data_from_gcs()
        
        logger.info("GCP Predictor initialized successfully!")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for inference"""
        import os
        driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "6g")
        executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "6g")
        
        spark = (
            SparkSession.builder
            .appName("TicketPricingPredictor-GCP")
            .config("spark.driver.memory", driver_memory)
            .config("spark.executor.memory", executor_memory)
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.maxResultSize", "4g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    
    def _download_model_from_gcs(self, gcs_path: str, local_name: str) -> str:
        """Download Spark ML model from GCS"""
        local_path = os.path.join(self.temp_dir, local_name)
        os.makedirs(local_path, exist_ok=True)
        
        # Download all model files
        blobs = self.bucket.list_blobs(prefix=gcs_path)
        for blob in blobs:
            if blob.name.endswith('/'):
                continue
            
            # Reconstruct local path
            relative_path = blob.name[len(gcs_path):].lstrip('/')
            local_file = os.path.join(local_path, relative_path)
            
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            blob.download_to_filename(local_file)
        
        logger.info(f"Downloaded model from gs://{self.bucket_name}/{gcs_path} to {local_path}")
        return local_path
    
    def _download_csv_from_gcs(self, gcs_path: str) -> pd.DataFrame:
        """Download and read CSV from GCS"""
        # Find the CSV file (part-*.csv)
        blobs = list(self.bucket.list_blobs(prefix=gcs_path))
        csv_blob = None
        for blob in blobs:
            if 'part-' in blob.name and blob.name.endswith('.csv'):
                csv_blob = blob
                break
        
        if not csv_blob:
            raise FileNotFoundError(f"No CSV file found in gs://{self.bucket_name}/{gcs_path}")
        
        # Download to string and read
        csv_content = csv_blob.download_as_string()
        from io import StringIO
        return pd.read_csv(StringIO(csv_content.decode('utf-8')))
    
    def _load_avg_encodings_from_gcs(self):
        """Load average encoding mappings from GCS"""
        # Max price encodings
        self.city_avg_max = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_max/avg_encoding_max/city_avg"
        )
        self.state_avg_max = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_max/avg_encoding_max/state_avg"
        )
        self.genre_avg_max = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_max/avg_encoding_max/genre_avg"
        )
        self.subgenre_avg_max = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_max/avg_encoding_max/subgenre_avg"
        )
        self.global_avg_max = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_max/avg_encoding_max/global_avg"
        )
        
        # Min price encodings
        self.city_avg_min = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_min/avg_encoding_min/city_avg"
        )
        self.state_avg_min = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_min/avg_encoding_min/state_avg"
        )
        self.genre_avg_min = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_min/avg_encoding_min/genre_avg"
        )
        self.subgenre_avg_min = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_min/avg_encoding_min/subgenre_avg"
        )
        self.global_avg_min = self._download_csv_from_gcs(
            f"{self.output_path}/ml_results_min/avg_encoding_min/global_avg"
        )
        
        # Get global averages
        self.global_max = float(self.global_avg_max['avg_price'].iloc[0])
        self.global_min = float(self.global_avg_min['avg_price'].iloc[0])
        
        logger.info(f"Global averages - min: ${self.global_min:.2f}, max: ${self.global_max:.2f}")
    
    def _load_artist_data_from_gcs(self):
        """Load artist-Spotify mapping from GCS"""
        try:
            df = self._download_csv_from_gcs(f"{self.data_path}/master_df.csv")
            
            # Clean artist names
            df['artists'] = df['artists'].astype(str).str.replace(r"[\[\]']", "", regex=True)
            df['spotify_popularity'] = pd.to_numeric(df['spotify_popularity'], errors='coerce')
            
            # Create artist -> popularity mapping
            self.artist_popularity = (
                df.groupby('artists')['spotify_popularity']
                .max()
                .to_dict()
            )
            
            # Create artist -> genre/subgenre mapping
            self.artist_genre = (
                df.groupby('artists')
                .agg({'genre': 'first', 'subGenre': 'first'})
                .to_dict('index')
            )
            
            # Default popularity
            valid_pops = [v for v in self.artist_popularity.values() if pd.notna(v)]
            self.default_popularity = np.median(valid_pops) if valid_pops else 50.0
            
            logger.info(f"Loaded {len(self.artist_popularity)} artists from GCS")
            
        except Exception as e:
            logger.warning(f"Failed to load artist data: {e}")
            self.artist_popularity = {}
            self.artist_genre = {}
            self.default_popularity = 50.0
    
    def _get_avg_price(self, df: pd.DataFrame, key: str, value: str, default: float) -> float:
        """Get average price from mapping"""
        try:
            match = df[df[key] == value]
            if not match.empty:
                return float(match['avg_price'].iloc[0])
        except:
            pass
        return default
    
    def _extract_temporal_features(self, date_str: Optional[str]) -> Tuple[int, int, int, str]:
        """Extract year, month, weekday from date"""
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.year, dt.month, dt.weekday(), dt.strftime("%a")
        except:
            now = datetime.now()
            return now.year, now.month, now.weekday(), now.strftime("%a")
    
    def _prepare_input_data(self, artist: str, city: str, state: str, 
                           date: Optional[str], genre: Optional[str]) -> Tuple[pd.DataFrame, Dict]:
        """Prepare input data with all required features"""
        # Get artist info
        artist_key = artist.strip()
        spotify_pop = self.artist_popularity.get(artist_key, self.default_popularity)
        
        # Infer genre if not provided
        if not genre and artist_key in self.artist_genre:
            artist_info = self.artist_genre[artist_key]
            genre = artist_info.get('genre', '')
            subgenre = artist_info.get('subGenre', genre)
        else:
            subgenre = genre or ''
        
        # Get temporal features
        year, month, weekday, weekday_name = self._extract_temporal_features(date)
        
        # Get average prices
        state_avg_max = self._get_avg_price(self.state_avg_max, 'state', state, self.global_max)
        city_avg_max = self._get_avg_price(self.city_avg_max, 'city', city, state_avg_max)
        genre_avg_max = self._get_avg_price(self.genre_avg_max, 'genre', genre, self.global_max) if genre else self.global_max
        subgenre_avg_max = self._get_avg_price(self.subgenre_avg_max, 'subgenre', subgenre, genre_avg_max) if subgenre else genre_avg_max
        
        state_avg_min = self._get_avg_price(self.state_avg_min, 'state', state, self.global_min)
        city_avg_min = self._get_avg_price(self.city_avg_min, 'city', city, state_avg_min)
        genre_avg_min = self._get_avg_price(self.genre_avg_min, 'genre', genre, self.global_min) if genre else self.global_min
        subgenre_avg_min = self._get_avg_price(self.subgenre_avg_min, 'subgenre', subgenre, genre_avg_min) if subgenre else genre_avg_min
        
        # Create DataFrame
        data = {
            'artist': [artist],
            'city': [city],
            'state': [state],
            'genre': [genre or 'Unknown'],
            'subgenre': [subgenre or 'Unknown'],
            'year': [year],
            'month': [month],
            'weekday': [weekday],
            'spotify_popularity': [spotify_pop],
            'state_avg_price': [state_avg_max],
            'city_avg_price': [city_avg_max],
            'genre_avg_price': [genre_avg_max],
            'subgenre_avg_price': [subgenre_avg_max],
            'weekday_name': [weekday_name]
        }
        
        return pd.DataFrame(data), {
            'state_avg_max': state_avg_max, 'city_avg_max': city_avg_max,
            'genre_avg_max': genre_avg_max, 'subgenre_avg_max': subgenre_avg_max,
            'state_avg_min': state_avg_min, 'city_avg_min': city_avg_min,
            'genre_avg_min': genre_avg_min, 'subgenre_avg_min': subgenre_avg_min
        }
    
    def _compute_probability(self, spotify_pop: float, city_avg: float, 
                            month: int, weekday: int) -> float:
        """Compute event occurrence probability"""
        pop_score = spotify_pop / 100.0
        city_market_score = min(1.0, city_avg / self.global_max) if self.global_max > 0 else 0.5
        
        is_weekend = weekday in [4, 5, 6]
        is_peak_season = month in [6, 7, 8, 12]
        
        prob = 0.40 + (pop_score * 0.30 + city_market_score * 0.15 + 
                       (0.10 if is_weekend else 0) + (0.05 if is_peak_season else 0))
        
        return min(0.95, max(0.10, prob))
    
    def predict(self, artist: str, city: str, state: str, 
                date: Optional[str] = None, genre: Optional[str] = None) -> Dict:
        """Predict ticket prices using Spark ML models from GCS"""
        if not artist or not city or not state:
            raise ValueError("Artist, city, and state are required")
        
        try:
            # Prepare input data
            input_df, avg_prices = self._prepare_input_data(artist, city, state, date, genre)
            
            # Predict MAX price
            input_df_max = input_df.copy()
            input_df_max['state_avg_price'] = avg_prices['state_avg_max']
            input_df_max['city_avg_price'] = avg_prices['city_avg_max']
            input_df_max['genre_avg_price'] = avg_prices['genre_avg_max']
            input_df_max['subgenre_avg_price'] = avg_prices['subgenre_avg_max']
            
            spark_df_max = self.spark.createDataFrame(input_df_max)
            pred_max = float(self.max_model.transform(spark_df_max).select("prediction").first()[0])
            
            # Predict MIN price
            input_df_min = input_df.copy()
            input_df_min['state_avg_price'] = avg_prices['state_avg_min']
            input_df_min['city_avg_price'] = avg_prices['city_avg_min']
            input_df_min['genre_avg_price'] = avg_prices['genre_avg_min']
            input_df_min['subgenre_avg_price'] = avg_prices['subgenre_avg_min']
            
            spark_df_min = self.spark.createDataFrame(input_df_min)
            pred_min = float(self.min_model.transform(spark_df_min).select("prediction").first()[0])
            
            # Ensure max >= min
            if pred_max < pred_min:
                pred_max = pred_min * 1.5
            
            # Compute probability
            probability = self._compute_probability(
                input_df['spotify_popularity'].iloc[0],
                avg_prices['city_avg_max'],
                input_df['month'].iloc[0],
                input_df['weekday'].iloc[0]
            )
            
            # Build response
            top_features = [
                {"name": f"Spotify Popularity: {input_df['spotify_popularity'].iloc[0]:.0f}", "score": 0.343},
                {"name": f"State ({state}): ${avg_prices['state_avg_max']:.0f}", "score": 0.171},
                {"name": f"City ({city}): ${avg_prices['city_avg_max']:.0f}", "score": 0.170},
                {"name": f"Month ({input_df['month'].iloc[0]})", "score": 0.104},
                {"name": f"Subgenre: ${avg_prices['subgenre_avg_max']:.0f}", "score": 0.089},
                {"name": f"Weekday ({input_df['weekday_name'].iloc[0]})", "score": 0.063},
            ]
            
            return {
                "probability": round(probability, 3),
                "pred_min": round(pred_min, 2),
                "pred_max": round(pred_max, 2),
                "top_features": top_features,
                "note": f"Day: {input_df['weekday_name'].iloc[0]}, Genre: {input_df['genre'].iloc[0]}",
                "model_info": {
                    "model_type": "RandomForest",
                    "framework": "Spark ML",
                    "source": "Google Cloud Storage"
                }
            }
            
        except Exception as e:
            logger.error(f"Prediction error: {e}", exc_info=True)
            raise
    
    def get_model_info(self) -> Dict:
        """Get model information"""
        return {
            "model_type": "RandomForest Regressor",
            "framework": "Apache Spark ML",
            "source": "Google Cloud Storage",
            "bucket": self.bucket_name,
            "total_artists": len(self.artist_popularity),
            "total_cities": len(self.city_avg_max),
            "global_avg_min": round(self.global_min, 2),
            "global_avg_max": round(self.global_max, 2),
            "default_popularity": round(self.default_popularity, 2)
        }
    
    def cleanup(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
        
        # Clean up temp directory
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        logger.info("Resources cleaned up")
