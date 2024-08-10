# Project Overview

As part of my Data Science Master's Degree at Harvard University's Extension School, I took a Data Engineering course that was centered on Databricks. One of the topics was building data pipelines using Medallion Architecture in Databricks using Delta Lake. In the course we were told that Delta Lake was available as open-source software, and this project shows how to create a data pipeline using Medallion Architecture, open-source Delta Lake, and Dataproc on Google Cloud Platform (GCP). Dataproc allows you to run Spark jobs on GCP compute clusters. The data pipelines are run using Apache Airflow. The project runs Airflow using Docker but should work with Airflow run in other ways. 

# Project Setup

- Create a GCP project with the Dataproc API enabled
- Create a GCP service account with the Storage Admin role and export a key for the service account. Name the key storage-service-account.json and add it to a secrets folder in folder where you run the project. 
- Modify the variables at the top of bucket_setup.sh to match your GCP project name and region along with custom names for your buckets. Also modify the variables for the bucket names at the top of bronze_table_creation.py, silver_table_creation.py, and gold_table_creation.py.
- Run bucket_setup.sh to create a GCP bucket to store files needed to run the pipeline, upload the files, and create a bucket to store Delta tables by running the following command:
```bash
./bucket_setup.sh
```
- Create a GCP service account with Dataproc Editor, Dataproc Worker and Service Account User roles and export a key for the service account. Store the key in the secrets folder in your project folder. 
- Modify dags/medallion_architecture.py to match your GCP project, region, Dataproc service account email address, and change the URIs so they match your chosen bucket name. Depending on you compute quotas, you may need/want to modify the VMs being used for the cluster.
- The raw data for pipeline is from Kaggle. In order to download the raw data as part of the pipeline, you need to save a valid Kaggle API key with the name kaggle-api-key in the GCP Secret Manager. 

# Running the Pipeline Using Airflow

If you already have Airflow set up, you can run the DAG contained in medallion_architecture.py. 

If you do not have Airflow set up, one option is to run it by using the docker-compose file that sets up Airflow running in Docker containers. The docker-compose file is essentially the same as the file you get by running:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml'
```

It has been modified to mount the secrets folder.

The process is verified to work when using a Mac as the local machine, and https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html can be used to help to troubleshoot if you face issues when using a different local OS. The website also points you to references for installing Docker, if you have not already done so. At least 4GB of memory for Docker is recommended to run Airflow through Docker. 

To launch Airflow using the docker-compose file, run this code from the terminal while in the project directory: 
```bash
docker compose up
```

The first time this is run will download the needed Docker images and set up a username and password to use the Airflow web UI. The default username and password are "airflow".

To launch the web ui, you go to http://localhost:8080 and log in.

You need to set up a GCP connection to be able to run your pipeline on GCP using Airflow. Once logged in to the Airflow UI, select Admin from the menu, and then select connections. Add a new connection with the Connection Id set to dataproc_connection, Connection Type set to Google Cloud, Project Id set to your GCP project name, and Keyfile Path set to /opt/airflow/secrets/dataproc-service-account.json. Save the connection.

At this point you can run the DAG by selecting DAGs from the menu. From there, search for the DAG "medallion-architecture" in the DAGS section of the UI, make the DAG active by selecting it, and run it. 

To shut down Airflow when running it using Docker, run this code from the terminal while in the project directory: 
```bash
docker compose down
```

# DAG Specifics

The first task run by the DAG is to create a Datproc cluster with the initialization actions provided for in medallion_init.sh.

The second task is to create bronze Delta tables from the raw data. The bronze layer in Medallion architecture is the first landing for raw data and does not transform the data in any way.

The third task is to transform the bronze layer data to then be saved in the silver layer. There is significant data cleanup, processing, and aggregation done in this layer. 

The fourth task is to transform the silver layer data into a highly-curated data in a wide table in the gold layer.

The final task is to delete the cluster so that compute costs are limited to those needed to run the pipeline.