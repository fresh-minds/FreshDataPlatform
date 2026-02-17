import requests
import sys
import time
from typing import Dict, Tuple

import os

# Service Configuration
SERVICES = {
    "Airflow Webserver": {"url": "http://localhost:8080/health", "method": "GET"},
    "DataHub GMS": {"url": f"http://{os.getenv('DATAHUB_GMS_HOST', 'localhost')}:8081/health", "method": "GET"},
    "MinIO": {"url": f"{os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')}/minio/health/live", "method": "GET"},
    # Postgres/Warehouse check would need psycopg2 connection, skipping http check for them
}

def check_service(name: str, config: Dict) -> Tuple[bool, str]:
    """Check if a service is healthy."""
    url = config["url"]
    auth = config.get("auth")
    
    try:
        response = requests.get(url, auth=auth, timeout=5)
        if response.status_code == 200:
            return True, f"OK ({response.status_code})"
        else:
            return False, f"Failed ({response.status_code})"
    except requests.exceptions.ConnectionError:
        return False, "Connection Refused (Is it running?)"
    except Exception as e:
        return False, f"Error: {str(e)}"

def verify_platform():
    print(f"{'='*50}")
    print("Open Data Platform - Health Verification")
    print(f"{'='*50}\n")
    
    all_healthy = True
    
    for name, config in SERVICES.items():
        print(f"Checking {name}...", end=" ", flush=True)
        is_healthy, message = check_service(name, config)
        
        if is_healthy:
            print(f"✅ {message}")
        else:
            print(f"❌ {message}")
            all_healthy = False
            
    print(f"\n{'='*50}")
    if all_healthy:
        print("🎉 Platform is HEALTHY. All services are responsive.")
        sys.exit(0)
    else:
        print("⚠️  Platform issues detected. Check docker-compose logs.")
        sys.exit(1)

if __name__ == "__main__":
    verify_platform()
