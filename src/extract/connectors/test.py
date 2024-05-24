from src.extract.connectors.findwork_api import FindWorkApiClient
import sys
import os

print(sys.path)
# Add the root directory of your project to the PYTHONPATH

# Example usage
api_key = "73a5e5f20098a3d50ba601ebb4791719f4c26f7e"
client = FindWorkApiClient(api_key)
jobs = client.get_jobs("python developer")
print(jobs)
