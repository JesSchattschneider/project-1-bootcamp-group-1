from src.extract.connectors.findwork_api import FindWorkApiClient
import sys
import os
import pandas as pd

print(sys.path)
# Add the root directory of your project to the PYTHONPATH

# Example usage
api_key = "73a5e5f20098a3d50ba601ebb4791719f4c26f7e"
client = FindWorkApiClient(api_key)
jobs = client.get_jobs("data engineer")

# get jobs data by page
# jobs = client.get_jobs("python developer", page=2)

# get jobs data by location
# jobs = client.get_jobs("python developer", location="New York")

# Print the keys of the 'jobs' dictionary
print("Keys in the 'jobs' dictionary:", jobs.keys())

# print the keys in the results dictionary
print("Keys in the 'results' dictionary:", jobs['results'][0].keys())

# place the results in a dataframe
df = pd.json_normalize(jobs['results'])
print(df)

# Print the number of job listings in the 'results' list
print("Number of job listings in the 'results' list:", len(jobs['results']))


# Print the first job listing in the 'results' list
print("First job listing in the 'results' list:", jobs['results'][0])


# get all jobs data without search query
# jobs = client.get_jobs("")

# get all jobs data with search query that has 'data engineer' in the role key
jobs = client.get_jobs("data engineer")

#


# Normalize the 'results' column to flatten the nested structure
df_results = pd.json_normalize(jobs, 'results')

# Print the columns of the normalized DataFrame to verify its structure
print("Columns in the normalized DataFrame:", df_results.columns)

# Print the first few rows of the normalized DataFrame to ensure correct normalization
print("First few rows of the normalized DataFrame:")
print(df_results.head())

# Extract the 'company_name' column and convert it to a list
company_names = df_results['company_name'].tolist()

# Print the list of company names
print("List of company names:", company_names)
