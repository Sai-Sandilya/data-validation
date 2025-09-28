#!/usr/bin/env python3

import requests
import json

def test_api_endpoints():
    base_url = "http://localhost:8000"
    
    print("=== Testing API Endpoints ===")
    
    # Test main endpoint
    try:
        response = requests.get(f"{base_url}/")
        print(f"[OK] Main endpoint: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"[ERROR] Main endpoint failed: {e}")
    
    # Test database connection
    test_config = {
        "host": "localhost",
        "port": 3306,
        "database": "source_db",
        "user": "root",
        "password": "Sandy@123"
    }
    
    try:
        response = requests.post(
            f"{base_url}/db/test-connection",
            json=test_config,
            headers={"Content-Type": "application/json"}
        )
        print(f"[OK] Database connection: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
    
    # Test get tables endpoint
    try:
        response = requests.post(
            f"{base_url}/db/get-tables",
            json=test_config,
            headers={"Content-Type": "application/json"}
        )
        print(f"[OK] Get tables: {response.status_code} - {response.json()}")
    except Exception as e:
        print(f"[ERROR] Get tables failed: {e}")

if __name__ == "__main__":
    test_api_endpoints()
