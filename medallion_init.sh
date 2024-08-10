#!/bin/bash

# pip install on dataproc cluster
pip install delta-spark==2.3.0 geopy google-cloud-secret-manager kaggle

# Install Delta Lake JARs on dataproc cluster
# Delta Lake 2.30 is the version that is compatible with Spark 3.3.2
# Spark 3.3.2 and Scala 2.12 are the versions installed on Dataproc 2.1.x clusters used in this project
JARS_DIR="/usr/lib/spark/jars"
DELTA_VERSION="2.3.0"
SCALA_VERSION="2.12"

# Download Delta Lake JAR
curl -o ${JARS_DIR}/delta-core_${SCALA_VERSION}-${DELTA_VERSION}.jar \
  https://repo1.maven.org/maven2/io/delta/delta-core_${SCALA_VERSION}/${DELTA_VERSION}/delta-core_${SCALA_VERSION}-${DELTA_VERSION}.jar

# Download Delta Storage JAR
curl -o /usr/lib/spark/jars/delta-storage-${DELTA_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar
