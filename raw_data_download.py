import zipfile
import os
import json
from google.cloud import secretmanager, storage

# Set GCP bucket name
GCP_BUCKET = 'medallion-files' 

# Get Kaggle API key from Google Secrets Manager
client = secretmanager.SecretManagerServiceClient()
name = "projects/538116411733/secrets/kaggle-api-key/versions/1"
response = client.access_secret_version(request={"name": name})
kaggle_credentials = json.loads(response.payload.data.decode("UTF-8"))

# Set Kaggle API environment variables
os.environ['KAGGLE_USERNAME'] = kaggle_credentials['username']
os.environ['KAGGLE_KEY'] = kaggle_credentials['key']

# Download dataset from Kaggle
import kaggle
kaggle.api.authenticate()
kaggle.api.competition_download_files('predict-energy-behavior-of-prosumers')

# Unzip dataset
os.makedirs('/home/raw_data', exist_ok=True)
with zipfile.ZipFile('predict-energy-behavior-of-prosumers.zip', 'r') as zip_ref:
    zip_ref.extractall('/home/raw_data')

# Move dataset to GCP bucket
client = storage.Client()
bucket = client.get_bucket(GCP_BUCKET)

for root, _, files in os.walk('/home/raw_data'):
    for file_name in files:
        local_path = os.path.join(root, file_name)
        relative_path = os.path.relpath(local_path, '/home/raw_data')
        blob_path = os.path.join('raw_data', relative_path)
        
        # Upload the file to GCS
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        print(f"Uploaded {local_path} to {blob_path}")
