#!/bin/bash

GCP_PROJECT="dataproc-testing-410711"
GCP_REGION="europe-west1"
FILES_BUCKET="medallion-files"
TABLES_BUCKET="medallion-tables"


gcloud auth activate-service-account --key-file="./secrets/storage-service-account.json"
gcloud auth list

if gsutil ls -b -p $GCP_PROJECT "gs://$FILES_BUCKET/" &> /dev/null; then
  echo "Bucket $FILES_BUCKET already exists."
else
  # Create the bucket if it doesn't exist
  gsutil mb -p $GCP_PROJECT -l $GCP_REGION gs://$FILES_BUCKET/
  echo "Bucket $FILES_BUCKET created."
fi


if gsutil ls -b -p $GCP_PROJECT "gs://$TABLES_BUCKET/" &> /dev/null; then
  echo "Bucket $TABLES_BUCKET already exists."
else
  # Create the bucket if it doesn't exist
  gsutil mb -p $GCP_PROJECT -l $GCP_REGION gs://$TABLES_BUCKET/
  echo "Bucket $TABLES_BUCKET created."
fi


FILES=("medallion_init.sh" "raw_data_download.py" "bronze_table_creation.py" "silver_table_creation.py" "gold_table_creation.py")

for FILE_PATH in "${FILES[@]}"; do
    DESTINATION_PATH="gs://$FILES_BUCKET/$FILE_PATH"
    gsutil cp "$FILE_PATH" "$DESTINATION_PATH"
    echo "File $FILE_PATH uploaded to $DESTINATION_PATH."
done