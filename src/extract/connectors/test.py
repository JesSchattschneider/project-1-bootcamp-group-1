from src.extract.connectors.findwork_api import FindWorkApiClient
import sys
import os

print(sys.path)
# Add the root directory of your project to the PYTHONPATH

# Example usage
api_key = "your_api_key"
client = FindWorkApiClient(api_key)
jobs = client.get_jobs("python developer")
print(jobs)
