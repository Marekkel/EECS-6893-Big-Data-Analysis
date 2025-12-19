#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test Script for Spark ML Backend

Tests the predictor and API endpoints
"""

import sys
import requests
import json
from pathlib import Path

def test_predictor():
    """Test the Spark ML predictor directly"""
    print("=" * 60)
    print("Testing Spark ML Predictor Module")
    print("=" * 60)
    
    try:
        from predictor import SparkMLPredictor
        
        print("\n1. Loading Spark ML models...")
        print("   (This will take 30-60 seconds...)")
        
        predictor = SparkMLPredictor(
            max_model_path="../output/ml_multi_models_max/models/random_forest",
            min_model_path="../output/ml_multi_models_min/models/random_forest",
            max_avg_encoding_path="../output/ml_results_max/avg_encoding_max",
            min_avg_encoding_path="../output/ml_results_min/avg_encoding_min",
            master_data_path="../data/master_df.csv"
        )
        print("   ✓ Spark ML predictor loaded successfully")
        
        print("\n2. Getting model information...")
        info = predictor.get_model_info()
        print(f"   Model: {info['model_type']}")
        print(f"   Framework: {info['framework']}")
        print(f"   Artists: {info['total_artists']}")
        print(f"   Cities: {info['total_cities_max']}")
        print(f"   Features: {info['feature_count']}")
        
        print("\n3. Testing predictions...")
        test_cases = [
            {
                "artist": "Taylor Swift",
                "city": "New York",
                "state": "NY",
                "date": "2024-06-15",
                "genre": "Pop"
            },
            {
                "artist": "Bad Bunny",
                "city": "Miami",
                "state": "FL",
                "date": "2024-07-20",
                "genre": "Hip-Hop / Rap"
            },
            {
                "artist": "Mannheim Steamroller Christmas",
                "city": "Huntsville",
                "state": "AL",
                "genre": "New Age"
            }
        ]
        
        for i, case in enumerate(test_cases, 1):
            print(f"\n   Test {i}: {case['artist']} in {case['city']}, {case['state']}")
            result = predictor.predict(**case)
            print(f"   Probability: {result['probability']:.1%}")
            print(f"   Price Range: ${result['pred_min']:.2f} - ${result['pred_max']:.2f}")
            print(f"   Model: {result['model_info']['model_type']}")
        
        print("\n4. Cleaning up...")
        predictor.cleanup()
        
        print("\n✓ All predictor tests passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Predictor test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_api():
    """Test the API endpoints"""
    print("\n" + "=" * 60)
    print("Testing API Endpoints")
    print("=" * 60)
    
    base_url = "http://localhost:8000"
    
    try:
        print("\n1. Testing root endpoint...")
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print("   ✓ Root endpoint OK")
            print(f"   Model: {data.get('model', 'N/A')}")
        else:
            print(f"   ✗ Failed: {response.status_code}")
            return False
        
        print("\n2. Testing health check...")
        response = requests.get(f"{base_url}/api/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print("   ✓ Health check OK")
            print(f"   Status: {data['status']}")
            print(f"   Spark active: {data['spark_session_active']}")
        else:
            print(f"   ✗ Failed: {response.status_code}")
            return False
        
        print("\n3. Testing model info endpoint...")
        response = requests.get(f"{base_url}/api/model-info", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print("   ✓ Model info OK")
            print(f"   Model type: {data['model_type']}")
            print(f"   Framework: {data['framework']}")
            print(f"   Total artists: {data['total_artists']}")
        else:
            print(f"   ✗ Failed: {response.status_code}")
            return False
        
        print("\n4. Testing prediction endpoint...")
        test_data = {
            "artist": "Taylor Swift",
            "city": "New York",
            "state": "NY",
            "date": "2024-06-15",
            "genre": "Pop"
        }
        
        response = requests.post(
            f"{base_url}/api/predict",
            json=test_data,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print("   ✓ Prediction endpoint OK")
            print(f"   Probability: {data['probability']:.1%}")
            print(f"   Min Price: ${data['pred_min']:.2f}")
            print(f"   Max Price: ${data['pred_max']:.2f}")
            print(f"   Model: {data['model_info']['model_type']}")
            print(f"   Framework: {data['model_info']['framework']}")
        else:
            print(f"   ✗ Failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
        
        print("\n✓ All API tests passed!")
        return True
        
    except requests.exceptions.ConnectionError:
        print("\n✗ Cannot connect to API server")
        print("   Make sure server is running: python app.py")
        return False
    except requests.exceptions.Timeout:
        print("\n✗ Request timeout")
        print("   Server might be starting up (first startup takes 30-60s)")
        return False
    except Exception as e:
        print(f"\n✗ API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("Spark ML Backend Test Suite")
    print("=" * 60)
    
    print("\n⚠️  Note: Tests require:")
    print("   - Java 8 or 11 installed")
    print("   - JAVA_HOME set correctly")
    print("   - Model files in ../output/")
    print("   - PySpark installed")
    
    # Test predictor
    print("\n" + "=" * 60)
    choice = input("\nTest Spark ML predictor directly? (y/n): ").lower().strip()
    
    predictor_ok = True
    if choice == 'y':
        print("\n⏳ Loading Spark and models (30-60 seconds)...")
        predictor_ok = test_predictor()
    
    # Test API
    print("\n" + "=" * 60)
    print("Note: API tests require server to be running")
    print("Start with: python app.py")
    print("=" * 60)
    
    choice = input("\nTest API endpoints? (y/n): ").lower().strip()
    
    api_ok = True
    if choice == 'y':
        api_ok = test_api()
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    if choice == 'y' or predictor_ok:
        print(f"Predictor: {'✓ PASS' if predictor_ok else '✗ FAIL'}")
    if choice == 'y':
        print(f"API:       {'✓ PASS' if api_ok else '✗ FAIL'}")
    print("=" * 60)
    
    if predictor_ok and api_ok:
        print("\n✓ All tests passed! Backend is ready.")
        print("\nNext steps:")
        print("  1. Start backend: python app.py")
        print("  2. Open website/index.html")
        print("  3. Test predictions!")
        return 0
    else:
        print("\n✗ Some tests failed. Check errors above.")
        return 1


if __name__ == "__main__":
    exit(main())
